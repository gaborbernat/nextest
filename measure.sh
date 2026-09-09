#!/usr/bin/env bash
set -u
echo "cores: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
cargo nextest run --no-run -p nextest-runner 2>&1 | tail -1
echo "--- available cases:"
cargo nextest list -p nextest-runner -E 'test(concurrent_spawns)' 2>&1 | tail -8
for kind in absolute relative; do
  for i in $(seq 10); do
    out=$(timeout 900 cargo nextest run -p nextest-runner --no-fail-fast -E "test(concurrent_spawns) & test($kind)" 2>&1)
    n=$(printf '%s' "$out" | grep -cE '^ +(PASS|FAIL|TRY)' || true)
    if printf '%s' "$out" | grep -q 'FAIL'; then
      r=$(printf '%s' "$out" | grep -oE 'in round [0-9]+' | head -1)
      echo "$kind iter $i: HIT ${r:-stall} (cases=$n)"
    else
      echo "$kind iter $i: MISS (cases=$n)"
    fi
  done
done
