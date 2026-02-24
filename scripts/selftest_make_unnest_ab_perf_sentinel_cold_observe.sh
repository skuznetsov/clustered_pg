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

WORKDIR="$(mktemp -d "$TMP_ROOT/clustered_pg_make_unnest_ab_perf_sentinel_cold_observe_selftest.XXXXXX")"
cleanup() {
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

FAKE_LOW="$WORKDIR/fake_probe_low.sh"
cat >"$FAKE_LOW" <<'EOF_LOW'
#!/usr/bin/env bash
set -euo pipefail
echo "ratio_kv|insert=0.700000|join_unnest=0.900000|any_array=0.800000"
echo "unnest_ab_probe: status=ok"
EOF_LOW
chmod +x "$FAKE_LOW"

OUT="$(
  UNNEST_SENTINEL_PROBE_SCRIPT="$FAKE_LOW" \
  make -s -C "$REPO_ROOT" --no-print-directory unnest-ab-perf-sentinel-cold-observe
)"
if ! printf '%s\n' "$OUT" | grep -Fq "unnest_ab_perf_sentinel status=observe"; then
  echo "expected observe status from unnest-ab-perf-sentinel-cold-observe target" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | grep -Fq "warmup_selects=0"; then
  echo "expected warmup_selects=0 from unnest-ab-perf-sentinel-cold-observe target" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi
if ! printf '%s\n' "$OUT" | grep -Fq "enforce_thresholds=0"; then
  echo "expected enforce_thresholds=0 from unnest-ab-perf-sentinel-cold-observe target" >&2
  printf '%s\n' "$OUT" >&2
  exit 1
fi

echo "selftest_make_unnest_ab_perf_sentinel_cold_observe status=ok"
