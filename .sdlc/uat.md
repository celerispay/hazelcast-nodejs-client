# UAT Results — hazelcast-client (near-cache TTL rework / audit follow-up)
**Date:** 2026-06-12  **Sign-off:** mixed (auto + deferred-cluster)
**Code under test:** branch 3.12.x @ eac1b80e (T6 commits b8a0b381 + c1d4ab1a, merged)

> Supersedes the 2026-06-11 UAT (which described the original per-NearCacheImpl
> setInterval design — that timer has since been removed by the L1 leak rework).

## Summary
Scenarios: 10 | Auto: 8 | Cluster/Human (deferred): 2 | Passed: 8 | Failed: 0 | Deferred: 2

## Results
| Scenario | Type | Result | Note |
|----------|------|--------|------|
| S1 — no per-NearCacheImpl timer (audit L1 closed) | auto | ✅ PASS | grep NearCache.ts: 0 setInterval/clearInterval/startExpirationTask/unref |
| S2 — single manager timer, unref'd + cleared | auto | ✅ PASS | NearCacheManager.ts: 1 setInterval, 1 unref, 1 clearInterval (in destroyAllNearCaches) |
| S3 — ttl=0 caches skipped (audit L5) | auto | ✅ PASS | manager tick gated on getTimeToLiveSeconds() > 0 |
| S4 — reclaims never-read TTL orphan (evictionPolicy NONE) | auto* | ✅ PASS | green-gate mocha (FIFO describe) — cluster-free 5/5 passing |
| S5 — overwrite w/ refreshed window not prematurely expired | auto* | ✅ PASS | same green-gate run (stale-generation drop) |
| S6 — TTL absolute, does not slide on read (T1) | auto | ✅ PASS | DataRecord.setCreationTime guard `this.ttl > 0 && this.expirationTime > 0` present |
| S7 — max-idle reset-on-read preserved | auto* | ✅ PASS | sweep is TTL-only (isExpired(0)); max-idle test passing in green-gate run |
| S8 — no forbidden changes (deps/codec/config surface) | auto | ✅ PASS | NO_DEP_CHANGES, CODEC_UNTOUCHED, NEARCACHECONFIG_UNTOUCHED |
| S9 — cluster-free mocha block, bare-repo standalone | cluster | ⏭️ DEFERRED | needs hazelcast-remote-controller resolvable; verified in build worktree, not re-run in bare main repo (partial --ignore-scripts install) |
| S10 — full near-cache suite vs live member (H14/H15) | cluster | ⏭️ DEFERRED | needs live Hazelcast member + remote-controller — the long-standing HANDOFF gap |

\* S4/S5/S7 behavioral assertions were run this session at the review green gate against the
identical committed source (build worktree, full deps installed):
`mocha --grep "FIFO expiration queue|max-idle reset-on-read" test/nearcache/NearCacheTest.js`
→ 5 passing. The merge into 3.12.x was byte-identical (same diffstat), so the merged code
carries the same verification.

## Deferred cluster sign-off (for a human with infrastructure)
Run after `hazelcast-remote-controller` + a live member are available (`pretest` provisions
them). Confirms end-to-end behavior through NearCachedMapProxy read-through:
- H14: a frequently-refreshed TTL key still expires on schedule from the ORIGINAL put (not last refresh).
- H15: after `client.shutdown()` the Node process exits cleanly — the single manager timer is cleared, no hung handle.
Command: `npm test` (auto-starts a member) or `npx mocha test/nearcache/NearCacheTest.js`.

## Doc nit (non-blocking, to fix at ship)
HANDOFF.md still describes the OLD per-NearCacheImpl setInterval design — prose lags the merged
single-manager-timer + FIFO-queue code. Refresh before release.
