#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [tmp_root_abs_dir]" >&2
  exit 2
fi

TMP_ROOT="${1:-/private/tmp}"
if [[ "$TMP_ROOT" != /* ]]; then
  echo "tmp_root_abs_dir must be absolute: $TMP_ROOT" >&2
  exit 2
fi
if [ ! -d "$TMP_ROOT" ]; then
  echo "tmp_root_abs_dir not found: $TMP_ROOT" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SENTINEL_SCRIPT="$SCRIPT_DIR/run_unnest_ab_perf_sentinel.sh"
if [ ! -x "$SENTINEL_SCRIPT" ]; then
  echo "sentinel script not executable: $SENTINEL_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_unnest_ab_perf_sentinel_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_OK="$WORKDIR/fake_probe_ok.sh"
cat >"$FAKE_OK" <<'EOF_OK'
#!/usr/bin/env bash
set -euo pipefail
echo " ratio_kv|insert=1.020000|join_unnest=1.500000|any_array=1.040000"
echo "unnest_ab_probe: status=ok"
EOF_OK
chmod +x "$FAKE_OK"

OUT_OK="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_OK" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT"
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_perf_sentinel status=ok"; then
  echo "expected ok sentinel status for fake passing probe" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

FAKE_LOW="$WORKDIR/fake_probe_low.sh"
cat >"$FAKE_LOW" <<'EOF_LOW'
#!/usr/bin/env bash
set -euo pipefail
echo "ratio_kv|insert=0.700000|join_unnest=0.900000|any_array=0.800000"
echo "unnest_ab_probe: status=ok"
EOF_LOW
chmod +x "$FAKE_LOW"

set +e
OUT_LOW="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_LOW" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_LOW=$?
set -e
if [ "$STATUS_LOW" -eq 0 ]; then
  echo "expected regression exit for below-threshold ratios" >&2
  printf '%s\n' "$OUT_LOW" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_LOW" | grep -Fq "unnest_ab_perf_sentinel status=regression"; then
  echo "expected regression status output for below-threshold ratios" >&2
  printf '%s\n' "$OUT_LOW" >&2
  exit 1
fi

OUT_OBSERVE="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_LOW" \
  UNNEST_SENTINEL_ENFORCE_THRESHOLDS=off \
  "$SENTINEL_SCRIPT" "$TMP_ROOT"
)"
if ! printf '%s\n' "$OUT_OBSERVE" | grep -Fq "unnest_ab_perf_sentinel status=observe"; then
  echo "expected observe status output when thresholds are disabled" >&2
  printf '%s\n' "$OUT_OBSERVE" >&2
  exit 1
fi

FAKE_MISSING="$WORKDIR/fake_probe_missing.sh"
cat >"$FAKE_MISSING" <<'EOF_MISSING'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_probe: status=ok"
EOF_MISSING
chmod +x "$FAKE_MISSING"

set +e
OUT_MISSING="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_MISSING" \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_MISSING=$?
set -e
if [ "$STATUS_MISSING" -eq 0 ]; then
  echo "expected non-zero exit for missing ratio_kv line" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MISSING" | grep -Fq "missing ratio_kv line in probe output"; then
  echo "expected missing ratio error output" >&2
  printf '%s\n' "$OUT_MISSING" >&2
  exit 1
fi

set +e
OUT_BAD_THRESH="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_OK" \
  UNNEST_SENTINEL_MIN_JOIN_UNNEST_RATIO=bad \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_BAD_THRESH=$?
set -e
if [ "$STATUS_BAD_THRESH" -eq 0 ]; then
  echo "expected non-zero exit for invalid threshold value" >&2
  printf '%s\n' "$OUT_BAD_THRESH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_THRESH" | grep -Fq "sentinel min ratios must be positive decimals"; then
  echo "expected invalid threshold validation message" >&2
  printf '%s\n' "$OUT_BAD_THRESH" >&2
  exit 1
fi

set +e
OUT_BAD_ENFORCE="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_OK" \
  UNNEST_SENTINEL_ENFORCE_THRESHOLDS=maybe \
  "$SENTINEL_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_BAD_ENFORCE=$?
set -e
if [ "$STATUS_BAD_ENFORCE" -eq 0 ]; then
  echo "expected non-zero exit for invalid enforce_thresholds value" >&2
  printf '%s\n' "$OUT_BAD_ENFORCE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD_ENFORCE" | grep -Fq "sentinel enforce_thresholds must be boolean"; then
  echo "expected invalid enforce_thresholds validation message" >&2
  printf '%s\n' "$OUT_BAD_ENFORCE" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_perf_sentinel status=ok"
