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

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_lightweight_success_stream_selftest.XXXXXX")"
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

OUT="$WORKDIR/runner.out"
(cd "$WORKDIR" && LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="off" ./run_lightweight_selftests.sh "$TMP_ROOT" jsonl >"$OUT" 2>&1)

python3 - "$OUT" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if len(lines) != 3:
    raise SystemExit(f"expected exactly 3 jsonl events, got {len(lines)}")

events = [json.loads(line) for line in lines]
for i, event in enumerate(events, start=1):
    if event.get("event_seq") != i:
        raise SystemExit(
            f"expected event_seq={i} at position {i}, got {event.get('event_seq')}"
        )

run_ids = {event.get("run_id") for event in events}
if len(run_ids) != 1:
    raise SystemExit("expected single stable run_id across success stream")

run_labels = {event.get("run_label") for event in events}
if len(run_labels) != 1:
    raise SystemExit("expected single stable run_label across success stream")

schema_versions = {event.get("schema_version") for event in events}
if schema_versions != {1}:
    raise SystemExit(f"expected schema_version=1 for all events, got {schema_versions}")

if events[0].get("event") != "begin":
    raise SystemExit("expected first event to be begin")
if events[1].get("event") != "ok":
    raise SystemExit("expected second event to be ok")
if events[2].get("event") != "final":
    raise SystemExit("expected third event to be final")

if events[0].get("script") != "selftest_injected_success.sh":
    raise SystemExit("unexpected begin script marker")
if events[1].get("script") != "selftest_injected_success.sh":
    raise SystemExit("unexpected ok script marker")

ok_elapsed = events[1].get("elapsed_s")
if not isinstance(ok_elapsed, int) or ok_elapsed < 0:
    raise SystemExit(f"expected non-negative integer ok elapsed_s, got {ok_elapsed!r}")

if events[2].get("status") != "ok":
    raise SystemExit(f"expected final status=ok, got {events[2].get('status')!r}")
if events[2].get("tests") != 1:
    raise SystemExit(f"expected final tests=1, got {events[2].get('tests')!r}")
total_elapsed = events[2].get("total_elapsed_s")
if not isinstance(total_elapsed, int) or total_elapsed < 0:
    raise SystemExit(
        f"expected non-negative integer final total_elapsed_s, got {total_elapsed!r}"
    )
if any(event.get("event") == "fail" for event in events):
    raise SystemExit("did not expect fail event in success stream")
PY

echo "selftest_run_lightweight_selftests_success_stream status=ok"
