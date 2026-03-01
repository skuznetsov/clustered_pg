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
CHECK_SCRIPT="$SCRIPT_DIR/check_unnest_ab_kv_output_fields.sh"

if [ ! -x "$CHECK_SCRIPT" ]; then
  echo "kv-output check script not executable: $CHECK_SCRIPT" >&2
  exit 2
fi

set +e
OUT_HELP="$(
  bash "$CHECK_SCRIPT" --help 2>&1
)"
STATUS_HELP=$?
set -e
if [ "$STATUS_HELP" -ne 0 ]; then
  echo "expected --help to exit with status 0" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq "usage: "; then
  echo "expected usage line in --help output" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP" | grep -Fq -- "--schema-policy exact|min"; then
  echo "expected schema policy options in --help output" >&2
  printf '%s\n' "$OUT_HELP" >&2
  exit 1
fi

set +e
OUT_HELP_SHORT="$(
  bash "$CHECK_SCRIPT" -h 2>&1
)"
STATUS_HELP_SHORT=$?
set -e
if [ "$STATUS_HELP_SHORT" -ne 0 ]; then
  echo "expected -h to exit with status 0" >&2
  printf '%s\n' "$OUT_HELP_SHORT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_HELP_SHORT" | grep -Fq "usage: "; then
  echo "expected usage line in -h output" >&2
  printf '%s\n' "$OUT_HELP_SHORT" >&2
  exit 1
fi

LINE_V1="unnest_ab_boundary_history_date_map_mode_bench|status=ok|mode=map-only|schema_version=1|speedup_x=100.00"

bash "$CHECK_SCRIPT" "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status mode schema_version speedup_x >/dev/null
bash "$CHECK_SCRIPT" --require-schema-version 1 --schema-policy exact "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status mode schema_version speedup_x >/dev/null
bash "$CHECK_SCRIPT" --require-schema-version 1 --schema-policy min "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status mode schema_version speedup_x >/dev/null

set +e
OUT_UNKNOWN="$(
  bash "$CHECK_SCRIPT" --unknown "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_UNKNOWN=$?
set -e
if [ "$STATUS_UNKNOWN" -eq 0 ]; then
  echo "expected non-zero exit for unknown option" >&2
  printf '%s\n' "$OUT_UNKNOWN" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_UNKNOWN" | grep -Fq "unknown option: --unknown"; then
  echo "expected explicit unknown option error" >&2
  printf '%s\n' "$OUT_UNKNOWN" >&2
  exit 1
fi

set +e
OUT_UNKNOWN_JSON="$(
  bash "$CHECK_SCRIPT" --error-format json --unknown "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_UNKNOWN_JSON=$?
set -e
if [ "$STATUS_UNKNOWN_JSON" -eq 0 ]; then
  echo "expected non-zero exit for unknown option in json error mode" >&2
  printf '%s\n' "$OUT_UNKNOWN_JSON" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_UNKNOWN_JSON" | grep -Fq '"status":"error"'; then
  echo "expected json error payload marker for unknown option" >&2
  printf '%s\n' "$OUT_UNKNOWN_JSON" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_UNKNOWN_JSON" | grep -Fq '"code":"unknown_option"'; then
  echo "expected unknown_option code in json error payload" >&2
  printf '%s\n' "$OUT_UNKNOWN_JSON" >&2
  exit 1
fi

set +e
OUT_DUP_REQUIRE="$(
  bash "$CHECK_SCRIPT" --require-schema-version 1 --require-schema-version 1 "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_DUP_REQUIRE=$?
set -e
if [ "$STATUS_DUP_REQUIRE" -eq 0 ]; then
  echo "expected non-zero exit for duplicate --require-schema-version option" >&2
  printf '%s\n' "$OUT_DUP_REQUIRE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_DUP_REQUIRE" | grep -Fq "duplicate --require-schema-version option"; then
  echo "expected duplicate --require-schema-version diagnostic" >&2
  printf '%s\n' "$OUT_DUP_REQUIRE" >&2
  exit 1
fi

set +e
OUT_DUP_POLICY="$(
  bash "$CHECK_SCRIPT" --schema-policy exact --schema-policy min "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_DUP_POLICY=$?
set -e
if [ "$STATUS_DUP_POLICY" -eq 0 ]; then
  echo "expected non-zero exit for duplicate --schema-policy option" >&2
  printf '%s\n' "$OUT_DUP_POLICY" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_DUP_POLICY" | grep -Fq "duplicate --schema-policy option"; then
  echo "expected duplicate --schema-policy diagnostic" >&2
  printf '%s\n' "$OUT_DUP_POLICY" >&2
  exit 1
fi

set +e
OUT_MIN_WITHOUT_REQUIRE="$(
  bash "$CHECK_SCRIPT" --schema-policy min "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_MIN_WITHOUT_REQUIRE=$?
set -e
if [ "$STATUS_MIN_WITHOUT_REQUIRE" -eq 0 ]; then
  echo "expected non-zero exit for --schema-policy min without --require-schema-version" >&2
  printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE" | grep -Fq -- "--schema-policy requires --require-schema-version"; then
  echo "expected explicit policy/require dependency diagnostic" >&2
  printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE" >&2
  exit 1
fi

set +e
OUT_MIN_WITHOUT_REQUIRE_JSON="$(
  bash "$CHECK_SCRIPT" --error-format json --schema-policy min "$LINE_V1" "unnest_ab_boundary_history_date_map_mode_bench" status 2>&1
)"
STATUS_MIN_WITHOUT_REQUIRE_JSON=$?
set -e
if [ "$STATUS_MIN_WITHOUT_REQUIRE_JSON" -eq 0 ]; then
  echo "expected non-zero exit for min-policy without required schema in json mode" >&2
  printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE_JSON" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE_JSON" | grep -Fq '"status":"error"'; then
  echo "expected json error payload marker for missing required option" >&2
  printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE_JSON" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE_JSON" | grep -Fq '"code":"missing_required_option"'; then
  echo "expected missing_required_option code in json error payload" >&2
  printf '%s\n' "$OUT_MIN_WITHOUT_REQUIRE_JSON" >&2
  exit 1
fi

echo "selftest_check_unnest_ab_kv_output_fields status=ok"
