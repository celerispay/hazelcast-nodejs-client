# hazelcast-client — Plan (Bugfix, S)

Near-cache TTL not expiring. Root cause: read-through publish re-stamps
`expirationTime` (sliding TTL from last refresh). Fix TTL to be absolute from the
original put, add evictionPolicy-independent proactive reclamation, preserve max-idle
reset-on-read. Conventions: bluebird Promise, `Date.now()` ms, `*1000` sec→ms, no
async/await, no public API change, `0`=unlimited.

## Tasks

- [ ] T1: Stop TTL re-stamp on read-through publish — TTL absolute from original put (est: 1.5h)
  files: src/nearcache/NearCache.ts (modify), src/nearcache/DataRecord.ts (modify)
  adds:  DataRecord.setCreationTime() guard (no-op / skip when expirationTime already set for reserved record); NearCache.tryPublishReserved() must not reset creationTime/expirationTime on an already-reserved record
  ~LOC:  20
  forbidden: do NOT touch setAccessTime() on hit (max-idle reset-on-read stays); no NearCacheConfig surface change; no async/await; no new fields exposed publicly; keep DataRecord constructor behavior for ttl>0 (verify it sets expirationTime = creationTime + ttl*1000)

- [ ] T2: Add evictionPolicy-independent proactive reclamation of TTL-expired records (est: 1.5h)
  files: src/nearcache/NearCache.ts (modify)
  adds:  NearCache TTL-sweep that frees TTL-expired records regardless of evictionPolicy (default NONE), routed through existing expireRecord() so expiredCount accounting holds
  ~LOC:  30
  forbidden: no background setInterval/timer unless unavoidable — prefer sweep hook on existing write/access path consistent with lazy design; do NOT alter doEvictionIfRequired eviction semantics; preserve `0`=unlimited (`> 0` guards); do NOT reclaim on max-idle (TTL only); no public API change

- [ ] T3: Regression tests for all three behaviors (est: 1h)
  files: test/nearcache/NearCacheTest.js (modify)
  adds:  test "TTL expires with intervening read inside window" (put, get mid-window, promiseAfter(ttl, get) => undefined); test "proactive reclaim removes expired-but-never-read entry" (assert record gone / expiredCount without a get touching it, evictionPolicy NONE); regression test "max-idle still resets on read" (read keeps entry alive past maxIdle window)
  ~LOC:  45
  forbidden: do not weaken the existing "ttl expire" assertion pattern; use existing promiseAfter helper + chai expect; no new test deps

## Parallel Groups
- Group A (independent): T1, T2
- Group B (needs A): T3
