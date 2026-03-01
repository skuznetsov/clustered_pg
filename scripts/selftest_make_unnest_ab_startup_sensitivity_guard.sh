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
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_make_startup_sensitivity_guard_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_PROBE_OK="$WORKDIR/fake_startup_probe_ok.sh"
cat >"$FAKE_PROBE_OK" <<'EOF_PROBE_OK'
#!/usr/bin/env bash
set -euo pipefail
if [ "${UNNEST_AB_WARMUP_SELECTS:-}" = "0" ]; then
  echo "ratio_kv|insert=1.000000|join_unnest=1.000000|any_array=1.000000"
else
  echo "ratio_kv|insert=1.200000|join_unnest=1.300000|any_array=1.100000"
fi
echo "unnest_ab_probe: status=ok"
EOF_PROBE_OK
chmod +x "$FAKE_PROBE_OK"

OUT_PROBE="$(
  make -s -C "$REPO_ROOT" --no-print-directory \
    unnest-ab-startup-sensitivity \
    UNNEST_STARTUP_PROBE_SCRIPT="$FAKE_PROBE_OK"
)"
if ! printf '%s\n' "$OUT_PROBE" | grep -Fq "unnest_ab_startup_sensitivity status=ok"; then
  echo "expected ok startup sensitivity status for fake make probe override" >&2
  printf '%s\n' "$OUT_PROBE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_PROBE" | grep -Fq "insert_warm_over_cold=1.200000"; then
  echo "expected fake startup probe output from make override" >&2
  printf '%s\n' "$OUT_PROBE" >&2
  exit 1
fi

FAKE_GUARD_OK="$WORKDIR/fake_startup_guard_ok.sh"
cat >"$FAKE_GUARD_OK" <<'EOF_GUARD_OK'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_warm_over_cold=1.200000|join_unnest_warm_over_cold=1.300000|any_array_warm_over_cold=1.100000"
EOF_GUARD_OK
chmod +x "$FAKE_GUARD_OK"

OUT_GUARD_OK="$(
  make -s -C "$REPO_ROOT" --no-print-directory \
    unnest-ab-startup-sensitivity-guard \
    UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_GUARD_OK" \
    UNNEST_STARTUP_GUARD_MAX_INSERT_WARM_OVER_COLD=1.30 \
    UNNEST_STARTUP_GUARD_MAX_JOIN_UNNEST_WARM_OVER_COLD=1.35 \
    UNNEST_STARTUP_GUARD_MAX_ANY_ARRAY_WARM_OVER_COLD=1.20
)"
if ! printf '%s\n' "$OUT_GUARD_OK" | grep -Fq "unnest_ab_startup_sensitivity_guard status=ok"; then
  echo "expected guard status=ok for fake make guard override" >&2
  printf '%s\n' "$OUT_GUARD_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_GUARD_OK" | grep -Fq "max_insert_warm_over_cold=1.30"; then
  echo "expected max_insert_warm_over_cold passthrough from make guard override" >&2
  printf '%s\n' "$OUT_GUARD_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_GUARD_OK" | grep -Fq "max_join_unnest_warm_over_cold=1.35"; then
  echo "expected max_join_unnest_warm_over_cold passthrough from make guard override" >&2
  printf '%s\n' "$OUT_GUARD_OK" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_GUARD_OK" | grep -Fq "max_any_array_warm_over_cold=1.20"; then
  echo "expected max_any_array_warm_over_cold passthrough from make guard override" >&2
  printf '%s\n' "$OUT_GUARD_OK" >&2
  exit 1
fi

FAKE_GUARD_HIGH="$WORKDIR/fake_startup_guard_high.sh"
cat >"$FAKE_GUARD_HIGH" <<'EOF_GUARD_HIGH'
#!/usr/bin/env bash
set -euo pipefail
echo "unnest_ab_startup_sensitivity status=ok|insert_warm_over_cold=1.700000|join_unnest_warm_over_cold=1.900000|any_array_warm_over_cold=1.600000"
EOF_GUARD_HIGH
chmod +x "$FAKE_GUARD_HIGH"

set +e
OUT_GUARD_HIGH="$(
  make -s -C "$REPO_ROOT" --no-print-directory \
    unnest-ab-startup-sensitivity-guard \
    UNNEST_STARTUP_GUARD_PROBE_SCRIPT="$FAKE_GUARD_HIGH" \
    UNNEST_STARTUP_GUARD_MAX_INSERT_WARM_OVER_COLD=1.40 \
    UNNEST_STARTUP_GUARD_MAX_JOIN_UNNEST_WARM_OVER_COLD=1.50 \
    UNNEST_STARTUP_GUARD_MAX_ANY_ARRAY_WARM_OVER_COLD=1.30 2>&1
)"
STATUS_GUARD_HIGH=$?
set -e
if [ "$STATUS_GUARD_HIGH" -eq 0 ]; then
  echo "expected non-zero exit for make guard override above thresholds" >&2
  printf '%s\n' "$OUT_GUARD_HIGH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_GUARD_HIGH" | grep -Fq "unnest_ab_startup_sensitivity_guard status=regression"; then
  echo "expected guard regression status for make override above thresholds" >&2
  printf '%s\n' "$OUT_GUARD_HIGH" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_GUARD_HIGH" | grep -Fq "max_join_unnest_warm_over_cold=1.50"; then
  echo "expected max_join_unnest_warm_over_cold passthrough in regression output" >&2
  printf '%s\n' "$OUT_GUARD_HIGH" >&2
  exit 1
fi

echo "selftest_make_unnest_ab_startup_sensitivity_guard status=ok"
