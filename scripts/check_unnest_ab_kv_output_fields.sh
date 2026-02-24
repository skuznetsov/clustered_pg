#!/usr/bin/env bash
set -euo pipefail

print_usage() {
  echo "usage: $0 [--error-format text|json] [--require-schema-version <n>] [--schema-policy exact|min] <kv_line> <prefix> <required_key_1> [required_key_2 ...]"
}

json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  value="${value//$'\r'/\\r}"
  value="${value//$'\t'/\\t}"
  printf '%s' "$value"
}

emit_error() {
  local code="$1"
  local message="$2"
  if [ "$ERROR_FORMAT" = "json" ]; then
    printf '{"status":"error","code":"%s","message":"%s"}\n' "$code" "$(json_escape "$message")" >&2
  else
    printf '%s\n' "$message" >&2
  fi
}

die() {
  local exit_code="$1"
  local code="$2"
  local message="$3"
  emit_error "$code" "$message"
  exit "$exit_code"
}

ERROR_FORMAT="text"
REQUIRED_SCHEMA_VERSION=""
SCHEMA_POLICY="exact"
SEEN_ERROR_FORMAT=0
SEEN_REQUIRE_SCHEMA_VERSION=0
SEEN_SCHEMA_POLICY=0
while [ "$#" -gt 0 ]; do
  case "${1:-}" in
    -h|--help)
      print_usage
      exit 0
      ;;
    --error-format)
      if [ "$#" -lt 2 ]; then
        die 2 "missing_option_value" "missing value for --error-format"
      fi
      if [ "$SEEN_ERROR_FORMAT" -eq 1 ]; then
        die 2 "duplicate_option" "duplicate --error-format option"
      fi
      case "${2:-}" in
        text|json)
          ERROR_FORMAT="${2:-}"
          ;;
        *)
          die 2 "invalid_option_value" "error format must be text or json: ${2:-}"
          ;;
      esac
      SEEN_ERROR_FORMAT=1
      shift 2
      ;;
    --require-schema-version)
      if [ "$#" -lt 2 ]; then
        die 2 "missing_option_value" "missing value for --require-schema-version"
      fi
      if [ "$SEEN_REQUIRE_SCHEMA_VERSION" -eq 1 ]; then
        die 2 "duplicate_option" "duplicate --require-schema-version option"
      fi
      REQUIRED_SCHEMA_VERSION="${2:-}"
      SEEN_REQUIRE_SCHEMA_VERSION=1
      shift 2
      ;;
    --schema-policy)
      if [ "$#" -lt 2 ]; then
        die 2 "missing_option_value" "missing value for --schema-policy"
      fi
      if [ "$SEEN_SCHEMA_POLICY" -eq 1 ]; then
        die 2 "duplicate_option" "duplicate --schema-policy option"
      fi
      SCHEMA_POLICY="${2:-}"
      SEEN_SCHEMA_POLICY=1
      shift 2
      ;;
    --)
      shift
      break
      ;;
    --*)
      die 2 "unknown_option" "unknown option: ${1:-}"
      ;;
    *)
      break
      ;;
  esac
done

if [ "$#" -lt 3 ]; then
  die 2 "usage_error" "$(print_usage)"
fi

LINE="$1"
PREFIX="$2"
shift 2

if [ -z "$LINE" ]; then
  die 2 "invalid_input" "kv_line must be non-empty"
fi
if [ -z "$PREFIX" ]; then
  die 2 "invalid_input" "prefix must be non-empty"
fi
if [ "$SCHEMA_POLICY" != "exact" ] && [ "$SCHEMA_POLICY" != "min" ]; then
  die 2 "invalid_option_value" "schema policy must be exact or min: $SCHEMA_POLICY"
fi
if [ -n "$REQUIRED_SCHEMA_VERSION" ] && ! [[ "$REQUIRED_SCHEMA_VERSION" =~ ^[0-9]+$ ]]; then
  die 2 "invalid_option_value" "required schema version must be a non-negative integer: $REQUIRED_SCHEMA_VERSION"
fi
if [ -z "$REQUIRED_SCHEMA_VERSION" ] && [ "$SCHEMA_POLICY" != "exact" ]; then
  die 2 "missing_required_option" "--schema-policy requires --require-schema-version"
fi

if [[ "$LINE" != "$PREFIX|"* ]]; then
  die 1 "prefix_mismatch" "line does not start with expected prefix '$PREFIX|': $LINE"
fi

field_value() {
  local wanted_key="$1"
  local field
  local -a fields=()
  IFS='|' read -r -a fields <<< "$LINE"
  for field in "${fields[@]}"; do
    if [[ "$field" == "$wanted_key="* ]]; then
      printf '%s\n' "${field#"$wanted_key="}"
      return 0
    fi
  done
  return 1
}

if [ -n "$REQUIRED_SCHEMA_VERSION" ]; then
  schema_value="$(field_value "schema_version" || true)"
  if [ -z "$schema_value" ]; then
    die 1 "missing_required_key" "missing required key 'schema_version' in line: $LINE"
  fi
  if ! [[ "$schema_value" =~ ^[0-9]+$ ]]; then
    die 1 "schema_not_numeric" "schema_version must be numeric (got: $schema_value) in line: $LINE"
  fi
  if [ "$SCHEMA_POLICY" = "exact" ]; then
    if [ "$schema_value" -ne "$REQUIRED_SCHEMA_VERSION" ]; then
      die 1 "schema_version_mismatch" "schema_version mismatch: policy=exact expected=$REQUIRED_SCHEMA_VERSION actual=$schema_value line=$LINE"
    fi
  else
    if [ "$schema_value" -lt "$REQUIRED_SCHEMA_VERSION" ]; then
      die 1 "schema_version_below_minimum" "schema_version below minimum: policy=min expected_min=$REQUIRED_SCHEMA_VERSION actual=$schema_value line=$LINE"
    fi
  fi
fi

for key in "$@"; do
  if [ -z "$key" ]; then
    die 2 "invalid_input" "required key must be non-empty"
  fi
  if ! field_value "$key" >/dev/null; then
    die 1 "missing_required_key" "missing required key '$key' in line: $LINE"
  fi
done

echo "check_unnest_ab_kv_output_fields status=ok prefix=$PREFIX keys=$# schema_required=${REQUIRED_SCHEMA_VERSION:-none} schema_policy=$SCHEMA_POLICY"
