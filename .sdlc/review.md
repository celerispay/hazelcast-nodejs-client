## Review: all (security + quality + correctness) | Status: PASS

Scope: cumulative diff `3a5571b8..HEAD` (commits b8a0b381, c1d4ab1a) over
`src/nearcache/DataRecord.ts`, `src/nearcache/NearCache.ts`,
`src/nearcache/NearCacheManager.ts`, `test/nearcache/NearCacheTest.js`.
Branch `celeris/build-20260612115654`. Final pre-merge gate for 3.12.x.

### Audit L1 re-verification (the gating finding)

CONFIRMED CLOSED. Verified against actual code, not just the diff intent:

- Base `3a5571b8:src/nearcache/NearCache.ts` had `private expirationTaskHandle`,
  `this.startExpirationTask()` in the `NearCacheImpl` constructor (line 124), a
  per-instance `setInterval` (line 279), and a `clearInterval` only inside
  `destroy()` (lines 248-250) — exactly the L1 leak: a missed `postDestroy()`
  (rejected `map.destroy()` round-trip) skips `destroy()`, so the per-instance
  1s timer fires forever.
- Current `NearCache.ts`: `grep` for `setInterval|clearInterval|startExpirationTask|
  expirationTaskHandle|unref` returns NONE. The per-instance field, constructor
  call, and `destroy()` clearInterval are all removed.
- `NearCacheImpl.destroy()` (NearCache.ts:286-289) now only `internalStore.clear()`
  + `resetExpirationQueue()` — no timer to clear. A missed proxy-destroy degrades
  to benign dormant retention (store + queue stay referenced until client
  shutdown), NOT a live perpetually-firing timer. Same failure mode as pre-fix.
- Manager timer ownership verified end-to-end: `HazelcastClient.shutdown()`
  (HazelcastClient.ts:375-380) calls `nearCacheManager.destroyAllNearCaches()`,
  which (NearCacheManager.ts:63-67) clears the single shared interval exactly once
  and nulls the handle.

### Manager timer correctness (audit item 2)

CONFIRMED CORRECT:
- Single instance: one `expirationTaskHandle` on the manager.
- Lazily started: `getOrCreateNearCache` starts it only when
  `expirationTaskHandle === undefined` (NearCacheManager.ts:47-49).
- `.unref()`'d: guarded `typeof handle.unref === 'function'` then `unref()`.
- Cleared exactly once on shutdown via `destroyAllNearCaches` (guarded `!= null`,
  then set `undefined` — idempotent).
- Tick skips ttl=0: manager `doExpiration()` (NearCacheManager.ts:101-104) calls a
  cache's `doExpiration()` only when `getTimeToLiveSeconds() > 0` (closes L5/L6).

### FIFO expiration queue correctness

CONFIRMED CORRECT:
- Put-order == expiration-order invariant HOLDS. `DataRecord.expirationTime` is
  computed once in the constructor for ttl>0 (DataRecord.ts:46-50) and
  `setCreationTime()` early-returns for ttl>0 (DataRecord.ts:136-138), so it never
  slides. No diff path mutates `expirationTime` after enqueue. The
  `tryReserveForUpdate` → `tryPublishReserved` path stays consistent: expirationTime
  is fixed at reservation (constructor); publish's `setCreationTime()` does not
  change it, so the enqueued value remains valid.
- Drain-from-front (NearCache.ts:326-346): stops at first non-expired head
  (`head.expirationTime > now` → break); stale heads dropped lazily — key gone
  (`cur === undefined`) or superseded generation
  (`cur.getExpirationTime() !== head.expirationTime`) → cursor advanced, skipped,
  store untouched. Reclamation routes through `expireRecord()` so `expiredCount`
  accounting is intact. TTL-only: `isExpired(0)` disables max-idle; max-idle stays
  lazy on read.
- Enqueue coverage COMPLETE: the only two ttl>0 record-creation paths are `put()`
  (NearCache.ts:234) and `tryReserveForUpdate()` (NearCache.ts:183) — both call
  `enqueueExpiration()`. `tryPublishReserved` reuses the already-enqueued
  reservation record (creates no new record) → no missing-enqueue regression. ttl=0
  records are correctly never enqueued.
- Compaction (NearCache.ts:374-402): two triggers — `overStale` rebuild keeps
  exactly one element per live key (matching expirationTime + `seenKeys` dedup), and
  a `slice()` prefix reclaim once the cursor passes the midpoint. Both reset
  `expirationQueueHead = 0`, so the cursor stays valid after slice/rebuild. The
  rebuild cannot drop a live-and-unexpired record: every finite-ttl live record has
  exactly one queue element whose `expirationTime` matches, and it is retained.
  Growth bounded by `EXPIRATION_QUEUE_STALE_FACTOR * store.size`. No off-by-one in
  the cursor logic.
- `clear()` (NearCache.ts:277) and `destroy()` (NearCache.ts:288) both call
  `resetExpirationQueue()` → empties array AND resets cursor. No stale cursor past a
  shrunk array.
- ES5 safety: no `for...of` over Maps/iterators in the diff. `doExpiration` uses an
  indexed while-loop; the compaction rebuild uses an indexed `for`; the only
  `for...of` additions iterate `Array.from(this.caches.values())` (a real array),
  matching the project's existing safe pattern. The prior dead-code bug is not
  reintroduced.

### Standard passes
- Regression: get/put/invalidate/tryReserve/tryPublish bodies are identical to base
  except added `enqueueExpiration()` calls and queue resets. No observable
  read/write behavior change. The reclaim test was correctly rewritten to drive the
  sweep via `doExpiration()` directly; new FIFO edge-case tests (overwrite, delete,
  insertion-order) added.
- Security: absolute, non-sliding TTL preserved — nothing here re-stamps
  expirationTime or extends entry life. No regression of the audit's S4 net
  improvement. No secrets, no injection surface, no input-trust change.
- Resource/leak: per-instance timer eliminated; single unref()'d manager timer
  cleared on shutdown; queue growth bounded. This is the fix's whole purpose and it
  holds.
- Conventions: bluebird `Promise` retained, no async/await, `0`=unlimited honored via
  `> 0` guards, no public API surface change (DataRecord/NearCacheImpl and the new
  `getExpirationTime`/`doExpiration`/`getTimeToLiveSeconds` accessors are internal,
  not re-exported through `index`).
- Compilation: `tsc --noEmit` reports 0 errors in `src/`. The only tsc output is
  pre-existing `@types/bluebird` declaration-syntax noise from a node_modules
  toolchain version mismatch — unrelated to this diff.

### Findings
| Severity | File | Line | Issue | Fix |
|----------|------|------|-------|-----|
| LOW | src/nearcache/NearCache.ts | 354-359 | `enqueueExpiration` guards on `expirationTime > 0`; for ttl=0, `getExpirationTime()` returns `undefined` and `undefined > 0` is falsy — correct, but relies on an implicit undefined/number coercion that is slightly opaque. | Optional readability: `if (expirationTime !== undefined && expirationTime > 0)`. Behavior already correct. |
| LOW | src/nearcache/NearCacheManager.ts | 30 | `private expirationTaskHandle: any;` uses `any` for the timer handle (mirrors the removed per-instance style). | Optional: type as `ReturnType<typeof setInterval>`. Non-blocking; consistent with surrounding code. |

### Summary
CRITICAL: 0 | HIGH: 0 | MEDIUM: 0 | LOW: 2
Action: Merge approved.

STATUS: PASS
Audit finding L1 (HIGH): CONFIRMED CLOSED — no per-NearCacheImpl timer remains; the
sole sweep timer is the single unref()'d NearCacheManager interval, lazily started
and cleared exactly once on HazelcastClient.shutdown → destroyAllNearCaches; a missed
postDestroy() now degrades to benign dormant retention with no live timer.
