#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTRACT_FILE="$SCRIPT_DIR/comparator_policy_contract.json"
EXPECTED_SCHEMA_VERSION=1
DUPLICATE_TARGET_MODE="${POLICY_LINT_DUPLICATE_TARGET_MODE:-error}"
WARNINGS_MAX_RAW="${POLICY_LINT_WARNINGS_MAX:-}"

case "$DUPLICATE_TARGET_MODE" in
  error|warn) ;;
  *)
    echo "policy_lint_error|invalid_duplicate_target_mode=$DUPLICATE_TARGET_MODE|expected=error_or_warn" >&2
    exit 1
    ;;
esac

if [ -n "$WARNINGS_MAX_RAW" ] && [[ ! "$WARNINGS_MAX_RAW" =~ ^[0-9]+$ ]]; then
  echo "policy_lint_error|invalid_warnings_max=$WARNINGS_MAX_RAW|expected_non_negative_int_or_empty" >&2
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "policy_lint_error|missing_python3=1" >&2
  exit 1
fi

python3 - "$SCRIPT_DIR" "$CONTRACT_FILE" "$EXPECTED_SCHEMA_VERSION" "$DUPLICATE_TARGET_MODE" "$WARNINGS_MAX_RAW" <<'PY'
import json
import pathlib
import sys


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


script_dir = pathlib.Path(sys.argv[1])
contract_path = pathlib.Path(sys.argv[2])
expected_schema_version = int(sys.argv[3])
duplicate_target_mode = sys.argv[4]
warnings_max_raw = sys.argv[5]
warnings_max: int | None = None
if warnings_max_raw != "":
    try:
        warnings_max = int(warnings_max_raw)
    except ValueError:
        fail(
            "policy_lint_error|invalid_warnings_max="
            f"{warnings_max_raw}|expected_non_negative_int_or_empty"
        )
    if warnings_max < 0:
        fail(
            "policy_lint_error|invalid_warnings_max="
            f"{warnings_max_raw}|expected_non_negative_int_or_empty"
        )

if not contract_path.is_file():
    fail(f"policy_lint_error|missing_file={contract_path}")

try:
    data = json.loads(contract_path.read_text(encoding="utf-8"))
except Exception as exc:  # noqa: BLE001
    fail(
        f"policy_lint_error|invalid_json={contract_path}|error_type={type(exc).__name__}"
    )

if not isinstance(data, dict):
    fail(f"policy_lint_error|invalid_contract_root={contract_path}")

if "schema_version" not in data:
    fail(f"policy_lint_error|missing_schema_version={contract_path}")

schema_version = data["schema_version"]
if not isinstance(schema_version, int):
    fail(
        "policy_lint_error|invalid_schema_version_type="
        f"{type(schema_version).__name__}|contract={contract_path}"
    )

if schema_version != expected_schema_version:
    fail(
        "policy_lint_error|unsupported_schema_version="
        f"{schema_version}|expected={expected_schema_version}|contract={contract_path}"
    )

checks = data.get("checks")
if not isinstance(checks, list) or len(checks) == 0:
    fail(f"policy_lint_error|no_checks_found_in_contract={contract_path}")

seen_names: dict[str, int] = {}
seen_targets: dict[tuple[str, str], int] = {}
warnings_count = 0
for idx, entry in enumerate(checks):
    if not isinstance(entry, dict):
        fail(f"policy_lint_error|invalid_contract_entry_index={idx}|reason=not_object")

    for field in ("name", "file", "needle"):
        value = entry.get(field)
        if not isinstance(value, str) or value == "":
            fail(
                "policy_lint_error|invalid_contract_entry_index="
                f"{idx}|field={field}|reason=missing_or_non_string"
            )

    name = entry["name"]
    rel_file = entry["file"]
    needle = entry["needle"]

    if "\\" in rel_file:
        fail(
            "policy_lint_error|invalid_check_file_path="
            f"{rel_file}|reason=backslash_not_allowed|index={idx}"
        )

    rel_path = pathlib.PurePosixPath(rel_file)
    if rel_path.is_absolute():
        fail(
            "policy_lint_error|invalid_check_file_path="
            f"{rel_file}|reason=absolute_path|index={idx}"
        )
    if any(part in ("", ".", "..") for part in rel_path.parts):
        fail(
            "policy_lint_error|invalid_check_file_path="
            f"{rel_file}|reason=parent_or_dot_segment|index={idx}"
        )

    if name in seen_names:
        first_idx = seen_names[name]
        fail(
            "policy_lint_error|duplicate_check_name="
            f"{name}|first_index={first_idx}|duplicate_index={idx}"
        )
    seen_names[name] = idx

    target_key = (rel_file, needle)
    if target_key in seen_targets:
        first_idx = seen_targets[target_key]
        if duplicate_target_mode == "warn":
            print(
                "policy_lint_warning|duplicate_check_target_file="
                f"{rel_file}|first_index={first_idx}|duplicate_index={idx}"
            )
            warnings_count += 1
        else:
            fail(
                "policy_lint_error|duplicate_check_target_file="
                f"{rel_file}|first_index={first_idx}|duplicate_index={idx}"
            )
    seen_targets[target_key] = idx

    target = (script_dir / rel_path).resolve()
    script_root = script_dir.resolve()
    if script_root not in (target, *target.parents):
        fail(
            "policy_lint_error|invalid_check_file_path="
            f"{rel_file}|reason=outside_scripts_dir|index={idx}"
        )

    if not target.is_file():
        fail(f"policy_lint_error|missing_file={target}")

    content = target.read_text(encoding="utf-8", errors="replace")
    if needle in content:
        print(f"policy_lint_check|name={name}|status=ok")
    else:
        fail(f"policy_lint_check|name={name}|status=fail|needle={needle}|file={target}")

print(f"policy_lint_warnings={warnings_count}")
if warnings_max is not None and warnings_count > warnings_max:
    fail(
        "policy_lint_error|warnings_threshold_exceeded="
        f"{warnings_count}|max={warnings_max}"
    )
print("policy_lint_status=ok")
PY
