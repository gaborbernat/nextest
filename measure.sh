#!/usr/bin/env bash
set -u
echo "cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
echo "uname: $(uname -s)"
echo "timeout available: $(command -v timeout || echo NO)"
cargo nextest run --no-run -p nextest-runner 2>&1 | tail -1

kinds="absolute"
if [ "$(uname -s)" = "Darwin" ]; then kinds="absolute relative"; fi

for kind in $kinds; do
  for i in $(seq 10); do
    out=$(cargo nextest run -p nextest-runner --no-fail-fast \
          -E "test(concurrent_spawns) & test($kind)" 2>&1)
    status=$?
    n=$(printf '%s\n' "$out" | grep -cE '(PASS|FAIL) \[' || true)
    if printf '%s\n' "$out" | grep -q 'FAIL'; then
      r=$(printf '%s\n' "$out" | grep -oE 'in round [0-9]+' | head -1)
      echo "$kind iter $i: HIT ${r:-stall} (cases=$n)"
    elif [ "$n" -gt 0 ]; then
      echo "$kind iter $i: MISS (cases=$n)"
    else
      echo "$kind iter $i: HARNESS-ERROR exit=$status"
      printf '%s\n' "$out" | tail -5 | sed 's/^/    | /'
    fi
  done
done
