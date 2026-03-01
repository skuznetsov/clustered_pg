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
LINT_SCRIPT="$SCRIPT_DIR/lint_comparator_policy.sh"
CONTRACT_FILE="$SCRIPT_DIR/comparator_policy_contract.json"

if [ ! -x "$LINT_SCRIPT" ]; then
  echo "lint script not executable: $LINT_SCRIPT" >&2
  exit 2
fi
if [ ! -f "$CONTRACT_FILE" ]; then
  echo "contract file not found: $CONTRACT_FILE" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_policy_lint_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

cp "$LINT_SCRIPT" "$WORKDIR/lint_comparator_policy.sh"
cp "$CONTRACT_FILE" "$WORKDIR/comparator_policy_contract.json"
chmod +x "$WORKDIR/lint_comparator_policy.sh"

while IFS= read -r rel_file; do
  [ -n "$rel_file" ] || continue
  src="$SCRIPT_DIR/$rel_file"
  dst="$WORKDIR/$rel_file"
  if [ ! -f "$src" ]; then
    echo "contract-referenced file not found: $src" >&2
    exit 2
  fi
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
done < <(sed -n 's/.*"file"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$CONTRACT_FILE")

BASE_CONTRACT="$WORKDIR/contract.base.json"
cp "$WORKDIR/comparator_policy_contract.json" "$BASE_CONTRACT"

expect_ok_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_ok.out"
  "$@" >"$out" 2>&1
  if ! grep -Fq "$expected" "$out"; then
    echo "expected success output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

expect_fail_contains() {
  local expected="$1"
  shift
  local out="$WORKDIR/expect_fail.out"
  if "$@" >"$out" 2>&1; then
    echo "expected failure but command succeeded: $*" >&2
    cat "$out" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$out"; then
    echo "expected failure output to contain: $expected" >&2
    cat "$out" >&2
    exit 1
  fi
}

run_lint() {
  (cd "$WORKDIR" && env -u POLICY_LINT_WARNINGS_MAX ./lint_comparator_policy.sh)
}

run_lint_mode() {
  local mode="$1"
  (cd "$WORKDIR" && env -u POLICY_LINT_WARNINGS_MAX POLICY_LINT_DUPLICATE_TARGET_MODE="$mode" ./lint_comparator_policy.sh)
}

run_lint_warnings_max() {
  local max="$1"
  (cd "$WORKDIR" && env -u POLICY_LINT_DUPLICATE_TARGET_MODE POLICY_LINT_WARNINGS_MAX="$max" ./lint_comparator_policy.sh)
}

run_lint_mode_with_warnings_max() {
  local mode="$1"
  local max="$2"
  (cd "$WORKDIR" && POLICY_LINT_DUPLICATE_TARGET_MODE="$mode" POLICY_LINT_WARNINGS_MAX="$max" ./lint_comparator_policy.sh)
}

restore_contract() {
  cp "$BASE_CONTRACT" "$WORKDIR/comparator_policy_contract.json"
}

expect_ok_contains "policy_lint_status=ok" run_lint
expect_ok_contains "policy_lint_warnings=0" run_lint
expect_ok_contains "policy_lint_warnings=0" run_lint_warnings_max 0

restore_contract
awk '
  /"schema_version"/ && !done {
    sub(/[0-9]+/, "2");
    done = 1;
  }
  { print }
' "$WORKDIR/comparator_policy_contract.json" >"$WORKDIR/contract.tmp"
mv "$WORKDIR/contract.tmp" "$WORKDIR/comparator_policy_contract.json"
if ! grep -Fq '"schema_version": 2' "$WORKDIR/comparator_policy_contract.json"; then
  echo "failed to set schema_version=2 in test contract" >&2
  exit 1
fi
expect_fail_contains "unsupported_schema_version=2|expected=1" run_lint

restore_contract
grep -v '"schema_version"' "$WORKDIR/comparator_policy_contract.json" >"$WORKDIR/contract.tmp"
mv "$WORKDIR/contract.tmp" "$WORKDIR/comparator_policy_contract.json"
expect_fail_contains "missing_schema_version=" run_lint

restore_contract
awk '
  /"schema_version"/ && !done {
    sub(/:[[:space:]]*[0-9]+/, ": \"1\"");
    done = 1;
  }
  { print }
' "$WORKDIR/comparator_policy_contract.json" >"$WORKDIR/contract.tmp"
mv "$WORKDIR/contract.tmp" "$WORKDIR/comparator_policy_contract.json"
if ! grep -Fq '"schema_version": "1"' "$WORKDIR/comparator_policy_contract.json"; then
  echo "failed to set schema_version string type in test contract" >&2
  exit 1
fi
expect_fail_contains "invalid_schema_version_type=str" run_lint

restore_contract
python3 - "$WORKDIR/comparator_policy_contract.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
checks = data.get("checks", [])
if len(checks) < 2:
    raise SystemExit("expected at least 2 checks to craft duplicate-name case")
checks[1]["name"] = checks[0]["name"]
path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
PY
expect_fail_contains "duplicate_check_name=unnest_polarity_higher" run_lint

restore_contract
python3 - "$WORKDIR/comparator_policy_contract.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
checks = data.get("checks", [])
if len(checks) < 1:
    raise SystemExit("expected at least 1 check to craft duplicate-target case")
checks.append(
    {
        "name": "duplicate_target_case",
        "file": checks[0]["file"],
        "needle": checks[0]["needle"],
    }
)
path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
PY
expect_fail_contains "duplicate_check_target_file=compare_unnest_ab_logsets.sh" run_lint

restore_contract
python3 - "$WORKDIR/comparator_policy_contract.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
checks = data.get("checks", [])
if len(checks) < 1:
    raise SystemExit("expected at least 1 check to craft duplicate-target warn case")
checks.append(
    {
        "name": "duplicate_target_warn_case",
        "file": checks[0]["file"],
        "needle": checks[0]["needle"],
    }
)
path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
PY
expect_ok_contains "policy_lint_warning|duplicate_check_target_file=compare_unnest_ab_logsets.sh" run_lint_mode warn
expect_ok_contains "policy_lint_warnings=1" run_lint_mode warn
expect_ok_contains "policy_lint_warnings=1" run_lint_mode_with_warnings_max warn 1
expect_fail_contains "warnings_threshold_exceeded=1|max=0" run_lint_mode_with_warnings_max warn 0

restore_contract
expect_fail_contains "invalid_duplicate_target_mode=foo|expected=error_or_warn" run_lint_mode foo
expect_fail_contains "invalid_warnings_max=foo|expected_non_negative_int_or_empty" run_lint_warnings_max foo
expect_fail_contains "invalid_warnings_max=-1|expected_non_negative_int_or_empty" run_lint_warnings_max -1

restore_contract
python3 - "$WORKDIR/comparator_policy_contract.json" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))
checks = data.get("checks", [])
if len(checks) < 1:
    raise SystemExit("expected at least 1 check to craft invalid-path case")
checks[0]["file"] = "../Makefile"
path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
PY
expect_fail_contains "invalid_check_file_path=../Makefile|reason=parent_or_dot_segment|index=0" run_lint

restore_contract
printf '{\n  "schema_version": 1,\n  "checks": [\n' >"$WORKDIR/comparator_policy_contract.json"
expect_fail_contains "invalid_json=" run_lint

echo "selftest_lint_comparator_policy status=ok"
