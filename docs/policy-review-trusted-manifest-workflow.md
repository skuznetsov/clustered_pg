# Policy review trusted-manifest workflow

This workflow is for **maximum review-window throughput** when manifest freshness is externally controlled.

## Recommended execution chain

1. Build manifest from review input directory.
2. Check manifest freshness against SLA (`max_age_seconds`).
3. Run policy review window in trusted mode (`manifest_trusted=on`).

You can run all three steps with one target:

```bash
make unnest-ab-profile-boundary-history-policy-review-window-trusted \
  UNNEST_AB_NIGHTLY_POLICY_REVIEW_INPUT=/abs/path/to/history_summaries \
  UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_OUT=/abs/path/to/review.manifest.tsv \
  UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=86400 \
  UNNEST_AB_NIGHTLY_POLICY_REVIEW_STRICT_MIN_OBS=48 \
  UNNEST_AB_NIGHTLY_POLICY_REVIEW_WINDOW_FILES=14
```

For deterministic testing only, you can override the freshness check clock:

```bash
UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_NOW_EPOCH=<unix_epoch_override>
```

## Safety model

- `trusted` mode skips per-file freshness checks for speed.
- Freshness must be enforced before execution via:
  - `scripts/check_unnest_ab_boundary_history_policy_review_manifest_freshness.sh`
- Direct script runs can enforce freshness in-process via:
  - `UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=<non_negative_seconds>`
- `UNNEST_AB_POLICY_REVIEW_MANIFEST_FRESHNESS_PRECHECKED=on` is an internal compose fast-path and must be used only with:
  - `UNNEST_AB_POLICY_REVIEW_MANIFEST_TRUSTED=on`
  - `UNNEST_AB_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS=<non_negative_seconds>`
- Default (non-trusted) mode remains available for robust fallback behavior on stale/missing manifest entries.

## Operational guidance

- Keep `UNNEST_AB_NIGHTLY_POLICY_REVIEW_MANIFEST_MAX_AGE_SECONDS` aligned with artifact update cadence.
- Use `trusted` mode only when:
  - input directory is controlled by your pipeline,
  - manifest is rebuilt as part of the same run or within SLA.
- If freshness cannot be guaranteed, use non-trusted mode.
