#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-${TMPDIR:-/tmp}}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GUARD_SCRIPT="$SCRIPT_DIR/run_unnest_ab_startup_sensitivity_guard.sh"
if [ ! -x "$GUARD_SCRIPT" ]; then
  echo "startup sensitivity guard script not executable: $GUARD_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_startup_sensitivity_guard_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_OK="$WORKDIR/fake_startup_ok.sh"
cat >"$FAKE_OK" <<'EOF_OK'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_warm_over_cold=1.200000|join_unnest_warm_over_cold=1.300000|any_array_warm_over_cold=1.100000"
EOF_OK
chmod +x "$FAKE_OK"

OUT_OK="$(
  UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_OK" \
  "$GUARD_SCRIPT" "$TMP_ROOT"
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_startup_sensitivity_guard status=ok"; then
  echo "expected ok guard status for fake startup sensitivity output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

FAKE_HIGH="$WORKDIR/fake_startup_high.sh"
cat >"$FAKE_HIGH" <<'EOF_HIGH'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_warm_over_cold=2.500000|join_unnest_warm_over_cold=2.800000|any_array_warm_over_cold=2.300000"
EOF_HIGH
chmod +x "$FAKE_HIGH"

set +e
OUT_HIGH="$(
  UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_HIGH" \
  "$GUARD_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_HIGH=$?
set -e
if [ "$STATUS_HIGH" -eq 0 ]; then
  echo "expected non-zero exit for above-threshold startup sensitivity ratios" >&2
  printf '%s\n' "$OUT_HIGH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HIGH" | grep -Fq "unnest_ab_startup_sensitivity_guard status=regression"; then
  echo "expected regression status output for above-threshold startup sensitivity ratios" >&2
  printf '%s\n' "$OUT_HIGH" >&2
  exit 1
fi

FAKE_MISSING="$WORKDIR/fake_startup_missing.sh"
cat >"$FAKE_MISSING" <<'EOF_MISSING'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_warm_over_cold=1.100000|any_array_warm_over_cold=1.200000"
EOF_MISSING
chmod +x "$FAKE_MISSING"

set +e
OUT_MISSING="$(
  UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_MISSING" \
  "$GUARD_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_MISSING=$?
set -e
if [ "$STATUS_MISSING" -eq 0 ]; then
  echo "expected non-zero exit for missing warm-over-cold field" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING" | grep -Fq "startup sensitivity value is not numeric for join_unnest_warm_over_cold"; then
  echo "expected missing join_unnest warm-over-cold diagnostic" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi

set +e
OUT_BAD_THRESH="$(
  UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_OK" \
  UNNEST_STARTUP_GUARD_MAX_JOIN_UNNEST_WARM_OVER_COLD=bad \
  "$GUARD_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_BAD_THRESH=$?
set -e
if [ "$STATUS_BAD_THRESH" -eq 0 ]; then
  echo "expected non-zero exit for invalid startup sensitivity threshold value" >&2
  printf '%s\n' "$OUT_BAD_THRESH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_THRESH" | grep -Fq "startup sensitivity max thresholds must be positive decimals"; then
  echo "expected invalid startup sensitivity threshold validation message" >&2
  printf '%s\n' "$OUT_BAD_THRESH" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_startup_sensitivity_guard status=ok"
