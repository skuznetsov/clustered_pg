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
RUNNER_SCRIPT="$SCRIPT_DIR/run_lightweight_selftests.sh"

if [ ! -x "$RUNNER_SCRIPT" ]; then
  echo "runner script not executable: $RUNNER_SCRIPT" >&2
  exit 2
fi

WORKDIR="$(mktemp -d "$TMP_ROOT/pg_sorted_heap_lightweight_run_label_prop_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

cp "$RUNNER_SCRIPT" "$WORKDIR/run_lightweight_selftests.sh"
chmod +x "$WORKDIR/run_lightweight_selftests.sh"

cat >"$WORKDIR/selftest_injected_success.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 0
EOF
chmod +x "$WORKDIR/selftest_injected_success.sh"

awk '
  /^run_one "/ && !done {
    print "run_one \"selftest_injected_success.sh\""
    done = 1
    next
  }
  /^run_one "/ {
    next
  }
  { print }
' "$WORKDIR/run_lightweight_selftests.sh" >"$WORKDIR/run_lightweight_selftests.tmp"
mv "$WORKDIR/run_lightweight_selftests.tmp" "$WORKDIR/run_lightweight_selftests.sh"
chmod +x "$WORKDIR/run_lightweight_selftests.sh"

EXPECTED_LABEL="ci-label_01:abc.def"
OUT="$WORKDIR/runner.out"
LIGHTWEIGHT_SELFTEST_RUN_LABEL="$EXPECTED_LABEL" \
LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="off" \
  "$WORKDIR/run_lightweight_selftests.sh" "$TMP_ROOT" jsonl >"$OUT" 2>&1

python3 - "$OUT" "$EXPECTED_LABEL" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
expected_label = sys.argv[2]
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if len(lines) != 3:
    raise SystemExit(f"expected exactly 3 jsonl events, got {len(lines)}")

events = [json.loads(line) for line in lines]
for i, event in enumerate(events, start=1):
    if event.get("event_seq") != i:
        raise SystemExit(
            f"expected event_seq={i} at position {i}, got {event.get('event_seq')}"
        )
    if event.get("run_label") != expected_label:
        raise SystemExit(
            f"expected run_label={expected_label!r}, got {event.get('run_label')!r}"
        )

if events[0].get("event") != "begin" or events[1].get("event") != "ok" or events[2].get("event") != "final":
    raise SystemExit("expected begin->ok->final event sequence")

if events[2].get("status") != "ok":
    raise SystemExit(f"expected final status=ok, got {events[2].get('status')!r}")
if events[2].get("tests") != 1:
    raise SystemExit(f"expected final tests=1, got {events[2].get('tests')!r}")
if events[0].get("script") != "selftest_injected_success.sh":
    raise SystemExit("unexpected begin script marker")
if events[1].get("script") != "selftest_injected_success.sh":
    raise SystemExit("unexpected ok script marker")
PY

echo "selftest_run_lightweight_selftests_run_label_propagation status=ok"
