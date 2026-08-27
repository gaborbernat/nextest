// Copyright (c) The nextest Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use crate::{
    errors::{ChildFdError, ErrorList},
    test_command::spawn_process,
    test_output::{CaptureStrategy, ChildExecutionOutput, ChildOutput, ChildSplitOutput},
};
use bytes::BytesMut;
use std::{
    io::{self, PipeReader},
    process::Stdio,
    sync::Arc,
};
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::{Child as TokioChild, ChildStderr, ChildStdout},
};

cfg_if::cfg_if! {
    if #[cfg(unix)] {
        #[path = "unix.rs"]
        mod unix;
        use unix as os;
    } else if #[cfg(windows)] {
        #[path = "windows.rs"]
        mod windows;
        use windows as os;
    } else {
        compile_error!("unsupported target platform");
    }
}

/// A spawned child process along with its file descriptors.
pub(crate) struct Child {
    pub child: TokioChild,
    pub child_fds: ChildFds,
}

pub(super) fn spawn(
    mut cmd: std::process::Command,
    strategy: CaptureStrategy,
    stdin_passthrough: bool,
) -> std::io::Result<Child> {
    if stdin_passthrough {
        cmd.stdin(Stdio::inherit());
    } else {
        cmd.stdin(Stdio::null());
    }

    let (mut child, combined_rx) = spawn_process(|| {
        let combined_rx: Option<PipeReader> = match strategy {
            CaptureStrategy::None => None,
            CaptureStrategy::Split => {
                cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
                None
            }
            CaptureStrategy::Combined => {
                // std::io::pipe() tracks platform-specific O_CLOEXEC support more
                // accurately than mio-pipe 0.1.1 and also works on Windows.
                let (rx, tx) = std::io::pipe()?;
                cmd.stdout(tx.try_clone()?).stderr(tx);
                Some(rx)
            }
        };

        let mut cmd: tokio::process::Command = cmd.into();
        Ok((cmd.spawn()?, combined_rx))
    })?;

    let output = match strategy {
        CaptureStrategy::None => ChildFds::new_split(None, None),
        CaptureStrategy::Split => {
            let stdout = child.stdout.take().expect("stdout was set");
            let stderr = child.stderr.take().expect("stderr was set");

            ChildFds::new_split(Some(stdout), Some(stderr))
        }
        CaptureStrategy::Combined => ChildFds::new_combined(
            os::pipe_reader_to_file(combined_rx.expect("combined_fx was set")).into(),
        ),
    };

    Ok(Child {
        child,
        child_fds: output,
    })
}

/// The size of each buffered reader's buffer, and the size at which we grow the combined buffer.
///
/// This size is not totally arbitrary, but rather the (normal) page size on most systems.
const CHUNK_SIZE: usize = 4 * 1024;

/// A `BufReader` over an `AsyncRead` that tracks the state of the reader and
/// whether it is done.
pub(crate) struct FusedBufReader<R> {
    reader: BufReader<R>,
    done: bool,
}

impl<R: AsyncRead + Unpin> FusedBufReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(CHUNK_SIZE, reader),
            done: false,
        }
    }

    pub(crate) async fn fill_buf(&mut self, acc: &mut BytesMut) -> Result<(), io::Error> {
        if self.done {
            return Ok(());
        }

        let res = self.reader.fill_buf().await;
        match res {
            Ok(buf) => {
                acc.extend_from_slice(buf);
                if buf.is_empty() {
                    self.done = true;
                }
                let len = buf.len();
                self.reader.consume(len);
                Ok(())
            }
            Err(error) => {
                self.done = true;
                Err(error)
            }
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done
    }
}

/// A version of [`FusedBufReader::fill_buf`] that works with an `Option<FusedBufReader>`.
async fn fill_buf_opt<R: AsyncRead + Unpin>(
    reader: Option<&mut FusedBufReader<R>>,
    acc: Option<&mut BytesMut>,
) -> Result<(), io::Error> {
    if let Some(reader) = reader {
        let acc = acc.expect("reader and acc must match");
        reader.fill_buf(acc).await
    } else {
        Ok(())
    }
}

/// A version of [`FusedBufReader::is_done`] that works with an `Option<FusedBufReader>`.
fn is_done_opt<R: AsyncRead + Unpin>(reader: Option<&FusedBufReader<R>>) -> bool {
    reader.is_none_or(|r| r.is_done())
}

/// Output and result accumulator for a child process.
pub(crate) struct ChildAccumulator {
    // TODO: it would be nice to also store the tokio::process::Child here, and
    // for `fill_buf` to select over it.
    pub(crate) fds: ChildFds,
    pub(crate) output: ChildOutputMut,
    pub(crate) errors: Vec<ChildFdError>,
}

impl ChildAccumulator {
    pub(crate) fn new(fds: ChildFds) -> Self {
        let output = fds.make_acc();
        Self {
            fds,
            output,
            errors: Vec::new(),
        }
    }

    pub(crate) async fn fill_buf(&mut self) {
        let res = self.fds.fill_buf(&mut self.output).await;
        if let Err(error) = res {
            self.errors.push(error);
        }
    }

    pub(crate) fn snapshot_in_progress(
        &self,
        error_description: &'static str,
    ) -> ChildExecutionOutput {
        ChildExecutionOutput::Output {
            result: None,
            output: self.output.snapshot(),
            errors: ErrorList::new(error_description, self.errors.clone()),
        }
    }
}

/// File descriptors (or Windows handles) for the child process.
pub(crate) enum ChildFds {
    /// Separate stdout and stderr, or they're not captured.
    Split {
        stdout: Option<FusedBufReader<ChildStdout>>,
        stderr: Option<FusedBufReader<ChildStderr>>,
    },

    /// Combined stdout and stderr.
    Combined { combined: FusedBufReader<File> },
}

impl ChildFds {
    pub(crate) fn new_split(stdout: Option<ChildStdout>, stderr: Option<ChildStderr>) -> Self {
        Self::Split {
            stdout: stdout.map(FusedBufReader::new),
            stderr: stderr.map(FusedBufReader::new),
        }
    }

    pub(crate) fn new_combined(rx: File) -> Self {
        Self::Combined {
            combined: FusedBufReader::new(rx),
        }
    }

    pub(crate) fn is_done(&self) -> bool {
        match self {
            Self::Split { stdout, stderr } => {
                is_done_opt(stdout.as_ref()) && is_done_opt(stderr.as_ref())
            }
            Self::Combined { combined } => combined.is_done(),
        }
    }
}

impl ChildFds {
    /// Makes an empty `ChildOutput` with the appropriate buffers for this `ChildFds`.
    pub(crate) fn make_acc(&self) -> ChildOutputMut {
        match self {
            Self::Split { stdout, stderr } => ChildOutputMut::Split {
                stdout: stdout.as_ref().map(|_| BytesMut::with_capacity(CHUNK_SIZE)),
                stderr: stderr.as_ref().map(|_| BytesMut::with_capacity(CHUNK_SIZE)),
            },
            Self::Combined { .. } => ChildOutputMut::Combined(BytesMut::with_capacity(CHUNK_SIZE)),
        }
    }

    /// Fills one of the buffers in `acc` with available data from the child process.
    ///
    /// This is a single step in the process of collecting the output of a child process. This
    /// operation is cancel-safe, since the underlying [`AsyncBufReadExt::fill_buf`] operation is
    /// cancel-safe.
    ///
    /// We follow this "externalized progress" pattern rather than having the collect output futures
    /// own the data they're collecting, to enable future improvements where we can dump
    /// currently-captured output to the terminal.
    pub(crate) async fn fill_buf(&mut self, acc: &mut ChildOutputMut) -> Result<(), ChildFdError> {
        match self {
            Self::Split { stdout, stderr } => {
                let (stdout_acc, stderr_acc) = acc.as_split_mut();
                // Wait until either of these make progress.
                tokio::select! {
                    res = fill_buf_opt(stdout.as_mut(), stdout_acc), if !is_done_opt(stdout.as_ref()) => {
                        res.map_err(|error| ChildFdError::ReadStdout(Arc::new(error)))
                    }
                    res = fill_buf_opt(stderr.as_mut(), stderr_acc), if !is_done_opt(stderr.as_ref()) => {
                        res.map_err(|error| ChildFdError::ReadStderr(Arc::new(error)))
                    }
                    // If both are done, do nothing.
                    else => {
                        Ok(())
                    }
                }
            }
            Self::Combined { combined } => {
                if !combined.is_done() {
                    combined
                        .fill_buf(acc.as_combined_mut())
                        .await
                        .map_err(|error| ChildFdError::ReadCombined(Arc::new(error)))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// The output of a child process that's currently being collected.
pub(crate) enum ChildOutputMut {
    /// Separate stdout and stderr (`None` if not captured).
    Split {
        stdout: Option<BytesMut>,
        stderr: Option<BytesMut>,
    },
    /// Combined stdout and stderr.
    Combined(BytesMut),
}

impl ChildOutputMut {
    fn as_split_mut(&mut self) -> (Option<&mut BytesMut>, Option<&mut BytesMut>) {
        match self {
            Self::Split { stdout, stderr } => (stdout.as_mut(), stderr.as_mut()),
            _ => panic!("ChildOutput is not split"),
        }
    }

    fn as_combined_mut(&mut self) -> &mut BytesMut {
        match self {
            Self::Combined(combined) => combined,
            _ => panic!("ChildOutput is not combined"),
        }
    }

    /// Makes a snapshot of the current output, returning a [`TestOutput`].
    ///
    /// This requires cloning the output so it's more expensive than [`Self::freeze`].
    pub(crate) fn snapshot(&self) -> ChildOutput {
        match self {
            Self::Split { stdout, stderr } => ChildOutput::Split(ChildSplitOutput {
                stdout: stdout.as_ref().map(|x| x.clone().freeze().into()),
                stderr: stderr.as_ref().map(|x| x.clone().freeze().into()),
            }),
            Self::Combined(combined) => ChildOutput::Combined {
                output: combined.clone().freeze().into(),
            },
        }
    }

    /// Marks the collection as done, returning a `TestOutput`.
    pub(crate) fn freeze(self) -> ChildOutput {
        match self {
            Self::Split { stdout, stderr } => ChildOutput::Split(ChildSplitOutput {
                stdout: stdout.map(|x| x.freeze().into()),
                stderr: stderr.map(|x| x.freeze().into()),
            }),
            Self::Combined(combined) => ChildOutput::Combined {
                output: combined.freeze().into(),
            },
        }
    }

    /// Returns the lengths of stdout and stderr in bytes.
    ///
    /// Returns `None` for each stream that wasn't captured.
    pub(crate) fn stdout_stderr_len(&self) -> (Option<u64>, Option<u64>) {
        match self {
            Self::Split { stdout, stderr } => (
                stdout.as_ref().map(|b| b.len() as u64),
                stderr.as_ref().map(|b| b.len() as u64),
            ),
            Self::Combined(combined) => (Some(combined.len() as u64), None),
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use std::{
        collections::BTreeSet,
        ffi::c_void,
        io,
        mem::{self, MaybeUninit},
        os::fd::{AsRawFd, RawFd},
        path::Path,
        process::Command,
        sync::Barrier,
        thread,
    };

    const PIPE_IDENTITIES_PREFIX: &str = "NEXTEST_PIPE_IDENTITIES=";
    const SPAWN_CONCURRENCY: usize = 32;
    const SPAWN_ROUNDS: usize = 32;
    const PROC_PIDFDPIPEINFO: libc::c_int = 6;

    #[test]
    fn concurrent_spawns_do_not_inherit_capture_pipes() {
        let executable = std::env::current_exe().expect("current test executable is available");
        let runtime = tokio::runtime::Runtime::new().expect("Tokio runtime starts");
        let mut inheritances = Vec::new();
        let mut omissions = Vec::new();

        for _ in 0..SPAWN_ROUNDS {
            let barrier = Barrier::new(SPAWN_CONCURRENCY);
            let children = thread::scope(|scope| {
                (0..SPAWN_CONCURRENCY)
                    .map(|_| {
                        let runtime_handle = runtime.handle().clone();
                        let executable = &executable;
                        let barrier = &barrier;
                        scope.spawn(move || {
                            barrier.wait();
                            let _guard = runtime_handle.enter();
                            spawn_pipe_reporter(executable)
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|handle| handle.join().expect("spawn thread does not panic"))
                    .collect::<io::Result<Vec<_>>>()
                    .expect("capture children start")
            });
            let reports = children
                .into_iter()
                .map(|mut child| {
                    let capture_identity = capture_identity(&child);
                    let child_identities = runtime.block_on(read_pipe_identities(&mut child));
                    wait_until_stopped(&child);
                    (capture_identity, child_identities, child)
                })
                .collect::<Vec<_>>();
            let capture_identities = reports
                .iter()
                .map(|(identity, _, _)| *identity)
                .collect::<BTreeSet<_>>();

            omissions.extend(
                reports
                    .iter()
                    .filter(|(capture, child, _)| !child.contains(capture))
                    .map(|(capture, child, _)| (*capture, child.clone())),
            );
            inheritances.extend(reports.iter().flat_map(|(capture, child, _)| {
                let capture = *capture;
                child
                    .intersection(&capture_identities)
                    .copied()
                    .filter(move |identity| *identity != capture)
                    .map(move |inherited| (capture, inherited))
            }));
            for (_, _, child) in &reports {
                resume(child);
            }
            for (_, _, child) in reports {
                runtime.block_on(collect_output(child));
            }
            assert_eq!(capture_identities.len(), SPAWN_CONCURRENCY);
        }

        assert_eq!(
            (omissions.len(), inheritances.len()),
            (0, 0),
            "capture reports omitted own pipes or inherited sibling pipes: {:#?}, {:#?}",
            omissions.first(),
            inheritances.first()
        );
    }

    fn spawn_pipe_reporter(executable: &Path) -> io::Result<Child> {
        let mut command = Command::new(executable);
        command.args([
            "--exact",
            "test_command::imp::tests::child_reports_pipe_identities",
            "--ignored",
            "--nocapture",
        ]);
        spawn(command, CaptureStrategy::Combined, false)
    }

    fn capture_identity(child: &Child) -> PipeHandle {
        match &child.child_fds {
            ChildFds::Combined { combined } => {
                PipeHandle(pipe_info(combined.reader.get_ref().as_raw_fd()).pipe_peerhandle)
            }
            ChildFds::Split { .. } => unreachable!("reporter output is combined"),
        }
    }

    async fn read_pipe_identities(child: &mut Child) -> BTreeSet<PipeHandle> {
        let ChildFds::Combined { combined } = &mut child.child_fds else {
            unreachable!("reporter output is combined");
        };
        loop {
            let mut line = String::new();
            assert_ne!(
                combined
                    .reader
                    .read_line(&mut line)
                    .await
                    .expect("capture output is readable"),
                0,
                "child exited before reporting pipe identities"
            );
            if let Some(identities) = line.strip_prefix(PIPE_IDENTITIES_PREFIX) {
                return parse_pipe_identities(identities.trim_end());
            }
        }
    }

    fn wait_until_stopped(child: &Child) {
        let pid = child.child.id().expect("capture child has a process ID") as libc::pid_t;
        let mut status = 0;
        // SAFETY: waitpid writes one status value and leaves the stopped child alive.
        assert_eq!(
            unsafe { libc::waitpid(pid, &mut status, libc::WUNTRACED) },
            pid
        );
        assert!(libc::WIFSTOPPED(status), "capture child stopped");
    }

    fn resume(child: &Child) {
        let pid = child.child.id().expect("capture child has a process ID") as libc::pid_t;
        // SAFETY: kill sends SIGCONT without accessing process memory.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    }

    async fn collect_output(mut child: Child) {
        let status = child.child.wait().await.expect("capture child exits");
        let mut accumulator = ChildAccumulator::new(child.child_fds);
        while !accumulator.fds.is_done() {
            accumulator.fill_buf().await;
        }
        assert!(status.success(), "capture child succeeds");
        assert!(
            accumulator.errors.is_empty(),
            "capture reads succeed: {:?}",
            accumulator.errors
        );
    }

    fn parse_pipe_identities(identities: &str) -> BTreeSet<PipeHandle> {
        identities
            .split(',')
            .map(|handle| PipeHandle(handle.parse().expect("pipe handle is numeric")))
            .collect()
    }

    #[test]
    #[ignore]
    fn child_reports_pipe_identities() {
        let identities = open_pipe_identities()
            .into_iter()
            .map(|handle| handle.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        println!("{PIPE_IDENTITIES_PREFIX}{identities}");
        // SAFETY: SIGSTOP suspends this child until the parent records every pipe identity.
        assert_eq!(unsafe { libc::raise(libc::SIGSTOP) }, 0);
    }

    fn open_pipe_identities() -> BTreeSet<PipeHandle> {
        let pid = std::process::id() as libc::c_int;
        // SAFETY: A null buffer asks proc_pidinfo for the required byte count.
        let required =
            unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
        assert!(required >= 0, "proc_pidinfo sizes the descriptor list");

        let entry_size = mem::size_of::<libc::proc_fdinfo>();
        let mut entries = Vec::<libc::proc_fdinfo>::with_capacity(
            required as usize / entry_size + SPAWN_CONCURRENCY,
        );
        // SAFETY: proc_pidinfo initializes at most the vector's allocated capacity.
        let filled = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                entries.as_mut_ptr().cast::<c_void>(),
                (entries.capacity() * entry_size) as libc::c_int,
            )
        };
        assert!(filled >= 0, "proc_pidinfo lists descriptors");
        // SAFETY: proc_pidinfo returned the number of initialized bytes.
        unsafe { entries.set_len(filled as usize / entry_size) };

        entries
            .into_iter()
            .filter(|entry| entry.proc_fdtype == libc::PROX_FDTYPE_PIPE as u32)
            .map(|entry| PipeHandle(pipe_info(entry.proc_fd).pipe_handle))
            .collect()
    }

    fn pipe_info(fd: RawFd) -> PipeInfo {
        let mut info = MaybeUninit::<PipeFdInfo>::uninit();
        let info_size = mem::size_of::<PipeFdInfo>() as libc::c_int;
        // SAFETY: proc_pidfdinfo writes at most info_size bytes to info.
        let filled = unsafe {
            libc::proc_pidfdinfo(
                std::process::id() as libc::c_int,
                fd,
                PROC_PIDFDPIPEINFO,
                info.as_mut_ptr().cast::<c_void>(),
                info_size,
            )
        };
        assert_eq!(filled, info_size, "proc_pidfdinfo reads pipe metadata");
        // SAFETY: proc_pidfdinfo initialized the complete PipeFdInfo value.
        unsafe { info.assume_init() }.pipe_info
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct PipeHandle(u64);

    #[repr(C)]
    struct ProcFileInfo {
        open_flags: u32,
        status: u32,
        offset: libc::off_t,
        file_type: i32,
        guard_flags: u32,
    }

    #[repr(C)]
    struct PipeInfo {
        stat: libc::vinfo_stat,
        pipe_handle: u64,
        pipe_peerhandle: u64,
        status: i32,
        reserved: i32,
    }

    #[repr(C)]
    struct PipeFdInfo {
        file_info: ProcFileInfo,
        pipe_info: PipeInfo,
    }
}
