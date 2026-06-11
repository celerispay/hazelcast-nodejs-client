## Review: all | Status: PASS_WITH_WARNINGS

### Findings
| Severity | File | Line | Issue | Fix |
|----------|------|------|-------|-----|
| HIGH | src/nearcache/NearCache.ts | 178, 132, 255-262 | `doExpiration()` does `Array.from(this.internalStore.values())` + full O(n) scan on EVERY `put()` and every `tryReserveForUpdate()` miss. On a hot write/read-through path with a large near cache this allocates a snapshot array and scans all records per write — a real throughput/GC concern at scale. | Throttle the sweep: gate it behind a time check (e.g. only sweep if `Date.now() - lastExpirationRun > expirationIntervalMs`) or a write counter, so it runs periodically rather than on every single write. Store `lastExpirationRun` as a field. Preserves correctness (read path still lazily expires) while removing per-write O(n) cost. |
| MEDIUM | src/nearcache/DataRecord.ts | 121 | The guard `this.ttl > 0 && this.expirationTime > 0` makes `setCreationTime()` a silent no-op for ttl>0 records. Correct for the TTL-sliding fix, but it also silently drops the `creationTime` argument callers may pass — behavior now depends on hidden record state. Currently the only caller (`tryPublishReserved`) passes no arg, so safe today, but the method's contract is now surprising. | Acceptable as-is for the fix; consider a brief WHY note that the early-return intentionally ignores the passed `creationTime` for absolute-TTL records, and/or rename intent. No code change required. |
| LOW | src/nearcache/NearCache.ts | 255 | `doExpiration()` is `protected` while `put`/`tryReserveForUpdate` are the only callers and the method is internal-only; fine, but it is not part of the `NearCache` interface and is reachable only via the concrete impl. Matches existing `doEvictionIfRequired`/`expireRecord` convention. | No action — convention-consistent. |
| LOW | test/nearcache/NearCacheTest.js | 269-289 | The new `max-idle reset-on-read` test sits inside `describe('NearCacheImpl')`, which has a `before` hook that starts a real cluster/client, yet this test only uses an in-process `NearCacheImpl` and never touches the cluster — it pays cluster-startup cost for no reason. Timing (4 chained 250ms reads vs 1s idle) is sound but moderately tight. | Optional: move the pure in-process test outside the cluster-backed `describe`, or accept the existing pattern (other CacheRecord tests do the same). |

### Notes (verified correct — no finding)
- `setCreationTime` guard: ttl=0 records (`expirationTime===undefined`) skip the guard and still stamp correctly; first stamping for a ttl>0 reserved record happens in the constructor at reservation time, so TTL is absolute from near-cache insertion. Read-through publish no longer slides it. Correct.
- `isExpired(0)` in `doExpiration()`: max-idle branch (`maxIdleSeconds > 0`) is disabled, only the absolute-TTL branch evaluates — sweep targets TTL-only, leaving max-idle lazy on the read path. Correct.
- Mutation-during-iteration: `Array.from(...values())` snapshots before the loop, so `expireRecord()` deleting from the live store during iteration is safe.
- `expiredCount` double-count: `expireRecord()` increments only when `internalStore.delete(key)` returns true, so the new sweep and the lazy `get`-path expiry cannot double-count the same key. Correct.
- Max-idle reset-on-read: `setAccessTime()` and the `get()` path are unchanged. Preserved per requirement.
- `0 = unlimited`: ttl=0 → `expirationTime===undefined` → `isExpired(0)` false; never swept. Preserved.
- Constraints: no new deps, no async/await (bluebird only), no `NearCacheConfig`/public-API change, no codec change. All honored.
- No secrets, no swallowed errors (test `catch` re-reports via `done(e)`). Conventions match the file.

### Summary
CRITICAL: 0 | HIGH: 1 | MEDIUM: 1 | LOW: 2
Action: Fix recommended before merge — address the HIGH per-write O(n) sweep (throttle it). The fix is functionally correct; the concern is hot-path performance at scale.

## Decisions
- HIGH (doExpiration per-write O(n) sweep): fixed af14e1df — throttled to EXPIRATION_TASK_INTERVAL_MS=1000, lastExpirationTime=0 init [2026-06-11]
- MEDIUM (setCreationTime ignores arg for ttl>0): informational, no action
- LOW x2 (max-idle test cost/timing): informational, no action
