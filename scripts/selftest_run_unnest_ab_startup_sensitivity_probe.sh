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
SENSITIVITY_SCRIPT="$SCRIPT_DIR/run_unnest_ab_startup_sensitivity_probe.sh"
if [ ! -x "$SENSITIVITY_SCRIPT" ]; then
  echo "startup sensitivity script not executable: $SENSITIVITY_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_startup_sensitivity_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_PROBE="$WORKDIR/fake_probe.sh"
cat >"$FAKE_PROBE" <<'EOF_FAKE'
#!/usr/bin/env bash
set -euo pipefail
warmup="${UNNEST_AB_WARMUP_SELECTS:-}"
case "$warmup" in
  0)
    echo "ratio_kv|insert=1.000000|join_unnest=1.000000|any_array=1.000000"
    ;;
  1)
    echo "ratio_kv|insert=2.000000|join_unnest=3.000000|any_array=4.000000"
    ;;
  *)
    echo "unexpected warmup value: $warmup" >&2
    exit 2
    ;;
esac
EOF_FAKE
chmod +x "$FAKE_PROBE"

OUT_OK="$(
  UNNEST_STARTUP_PROBE_SCRIPT="$FAKE_PROBE" \
  "$SENSITIVITY_SCRIPT" "$TMP_ROOT"
)"
if ! printf '%s\n' "$OUT_OK" | grep -Fq "unnest_ab_startup_sensitivity status=ok"; then
  echo "expected status=ok output from startup sensitivity script" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "insert_warm_over_cold=2.000000"; then
  echo "expected insert_warm_over_cold=2.000000" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "join_unnest_warm_over_cold=3.000000"; then
  echo "expected join_unnest_warm_over_cold=3.000000" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_OK" | grep -Fq "any_array_warm_over_cold=4.000000"; then
  echo "expected any_array_warm_over_cold=4.000000" >&2
  printf '%s\n' "$OUT_OK" >&2
  exit 1
fi

FAKE_BAD="$WORKDIR/fake_probe_bad.sh"
cat >"$FAKE_BAD" <<'EOF_BAD'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_probe: status=ok"
EOF_BAD
chmod +x "$FAKE_BAD"

set +e
OUT_BAD="$(
  UNNEST_STARTUP_PROBE_SCRIPT="$FAKE_BAD" \
  "$SENSITIVITY_SCRIPT" "$TMP_ROOT" 2>&1
)"
STATUS_BAD=$?
set -e
if [ "$STATUS_BAD" -eq 0 ]; then
  echo "expected non-zero exit when ratio_kv line is missing" >&2
  printf '%s\n' "$OUT_BAD" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_BAD" | grep -Fq "missing ratio_kv line in probe output"; then
  echo "expected missing ratio_kv diagnostic" >&2
  printf '%s\n' "$OUT_BAD" >&2
  exit 1
fi

echo "selftest_run_unnest_ab_startup_sensitivity_probe status=ok"
