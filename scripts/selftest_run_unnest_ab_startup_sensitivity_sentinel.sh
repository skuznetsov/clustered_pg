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
SENTINEL_SCRIPT="$SCRIPT_DIR/run_unnest_ab_startup_sensitivity_sentinel.sh"
if [ ! -x "$SENTINEL_SCRIPT" ]; then
  echo "startup sensitivity sentinel script not executable: $SENTINEL_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_startup_sensitivity_sentinel_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_OK="$WORKDIR/fake_startup_ok.sh"
cat >"$FAKE_OK" <<'EOF_OK'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_cold=1.000000|insert_warm=1.200000|insert_warm_over_cold=1.200000|join_unnest_cold=1.000000|join_unnest_warm=1.300000|join_unnest_warm_over_cold=1.300000|any_array_cold=1.000000|any_array_warm=1.100000|any_array_warm_over_cold=1.100000|cold_warmup_selects=0|warm_warmup_selects=1"
EOF_OK
chmod +x "$FAKE_OK"

OUT_OK="$(
  UNNEST_STARTUP_SENTINEL_PROBE_SCRIPT="$FAKE_OK" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT"
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_startup_sensitivity_sentinel status=observe"; then
  echo "expected observe status from startup sensitivity sentinel" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "gating=off"; then
  echo "expected gating=off in startup sensitivity sentinel output" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

FAKE_BAD_WARMUP="$WORKDIR/fake_startup_bad_warmup.sh"
cat >"$FAKE_BAD_WARMUP" <<'EOF_BAD_WARMUP'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_cold=1.000000|insert_warm=1.200000|insert_warm_over_cold=1.200000|join_unnest_cold=1.000000|join_unnest_warm=1.300000|join_unnest_warm_over_cold=1.300000|any_array_cold=1.000000|any_array_warm=1.100000|any_array_warm_over_cold=1.100000|cold_warmup_selects=1|warm_warmup_selects=0"
EOF_BAD_WARMUP
chmod +x "$FAKE_BAD_WARMUP"

set +e
OUT_BAD_WARMUP="$(
  UNNEST_STARTUP_SENTINEL_PROBE_SCRIPT="$FAKE_BAD_WARMUP" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_BAD_WARMUP=$?
set -e
if [ "$STATUS_BAD_WARMUP" -eq 0 ]; then
  echo "expected non-zero exit for unexpected startup warmup selectors" >&2
  printf '%s\n' "$OUT_BAD_WARMUP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_WARMUP" | grep -Fq "startup sensitivity sentinel expected cold_warmup_selects=0 and warm_warmup_selects=1"; then
  echo "expected startup warmup selector diagnostic" >&2
  printf '%s\n' "$OUT_BAD_WARMUP" >&2
  exit 1
fi

FAKE_MISSING="$WORKDIR/fake_startup_missing_field.sh"
cat >"$FAKE_MISSING" <<'EOF_MISSING'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_cold=1.000000|insert_warm=1.200000|insert_warm_over_cold=1.200000|join_unnest_cold=1.000000|join_unnest_warm=1.300000|any_array_cold=1.000000|any_array_warm=1.100000|any_array_warm_over_cold=1.100000|cold_warmup_selects=0|warm_warmup_selects=1"
EOF_MISSING
chmod +x "$FAKE_MISSING"

set +e
OUT_MISSING="$(
  UNNEST_STARTUP_SENTINEL_PROBE_SCRIPT="$FAKE_MISSING" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_MISSING=$?
set -e
if [ "$STATUS_MISSING" -eq 0 ]; then
  echo "expected non-zero exit for missing startup ratio field" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING" | grep -Fq "startup sensitivity sentinel value is not numeric for join_unnest_warm_over_cold"; then
  echo "expected missing startup ratio field diagnostic" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_startup_sensitivity_sentinel status=ok"
