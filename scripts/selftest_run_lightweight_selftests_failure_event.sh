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

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_lightweight_fail_event_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

cp "$RUNNER_SCRIPT" "$WORKDIR/run_lightweight_selftests.sh"
chmod +x "$WORKDIR/run_lightweight_selftests.sh"

cat >"$WORKDIR/selftest_injected_failure.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exit 42
EOF
chmod +x "$WORKDIR/selftest_injected_failure.sh"

awk '
  /^run_one "/ && !done {
    print "run_one \"selftest_injected_failure.sh\""
    done = 1
    next
  }
  { print }
' "$WORKDIR/run_lightweight_selftests.sh" >"$WORKDIR/run_lightweight_selftests.tmp"
mv "$WORKDIR/run_lightweight_selftests.tmp" "$WORKDIR/run_lightweight_selftests.sh"
chmod +x "$WORKDIR/run_lightweight_selftests.sh"

OUT="$WORKDIR/runner.out"
if (cd "$WORKDIR" && LIGHTWEIGHT_SELFTEST_AUTO_TMP_CLEAN="off" ./run_lightweight_selftests.sh "$TMP_ROOT" jsonl >"$OUT" 2>&1); then
  echo "expected runner to fail on injected failure script" >&2
  cat "$OUT" >&2
  exit 1
fi

if ! grep -Fq '"event":"begin"' "$OUT"; then
  echo "expected begin event in runner output" >&2
  cat "$OUT" >&2
  exit 1
fi
if ! grep -Fq '"script":"selftest_injected_failure.sh"' "$OUT"; then
  echo "expected injected script marker in runner output" >&2
  cat "$OUT" >&2
  exit 1
fi
if ! grep -Fq '"event":"fail"' "$OUT"; then
  echo "expected fail event in runner output" >&2
  cat "$OUT" >&2
  exit 1
fi
if ! grep -Fq '"exit_code":42' "$OUT"; then
  echo "expected exit_code=42 in fail event" >&2
  cat "$OUT" >&2
  exit 1
fi
if grep -Fq '"event":"final","event_ts"' "$OUT"; then
  echo "unexpected final success event in failing run" >&2
  cat "$OUT" >&2
  exit 1
fi

python3 - "$OUT" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
if len(lines) != 2:
    raise SystemExit(f"expected exactly 2 jsonl events, got {len(lines)}")

events = [json.loads(line) for line in lines]
for i, event in enumerate(events, start=1):
    if event.get("event_seq") != i:
        raise SystemExit(
            f"expected event_seq={i} at position {i}, got {event.get('event_seq')}"
        )

run_ids = {event.get("run_id") for event in events}
if len(run_ids) != 1:
    raise SystemExit("expected single stable run_id across failure stream")

run_labels = {event.get("run_label") for event in events}
if len(run_labels) != 1:
    raise SystemExit("expected single stable run_label across failure stream")

if events[0].get("event") != "begin" or events[1].get("event") != "fail":
    raise SystemExit("expected begin->fail event sequence")

if events[0].get("script") != "selftest_injected_failure.sh":
    raise SystemExit("unexpected begin script marker")
if events[1].get("script") != "selftest_injected_failure.sh":
    raise SystemExit("unexpected fail script marker")
if events[1].get("exit_code") != 42:
    raise SystemExit(f"expected fail exit_code=42, got {events[1].get('exit_code')}")
PY

echo "selftest_run_lightweight_selftests_failure_event status=ok"
