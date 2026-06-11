# Exploration: hazelcast-client
**Stack:** TypeScript 2.8.4 (ES5/CommonJS), bluebird Promises, `long`
**Tests:** mocha 3 + chai (`expect`) + sinon; `test/nearcache/NearCacheTest.js`
**Authority:** map (`.sdlc/celeris-codebase-map.md`, fresh) + 4 near-cache files read
**Nature:** backend (data-grid client library; no UI/AI deps — `bluebird`/`long`/`safe-buffer` only)

## Conventions
- Time units: config is **seconds**; internally converted to **ms** (`creationTime + ttl * 1000`, `lastAccessTime + maxIdleSeconds * 1000`). `Date.now()` everywhere.
- Records stored in `DataKeyedHashMap<DataRecord>` keyed by `Data`. `get()` returns a bluebird `Promise` gated on a `ready` deferred.
- Expiry is **lazy / read-triggered** (`get`) plus opportunistic sweep during eviction (`filterExpiredRecord`). No background timer.
- `0` (ttl or maxIdle) means **unlimited** — guarded by `> 0` checks.
- Removal counters: `expireRecord` -> `expiredCount`, `evictRecord` -> `evictedCount`.

## Golden Examples
- Expiry check — `src/nearcache/DataRecord.ts:69`
  ```ts
  isExpired(maxIdleSeconds: number): boolean {
      const now = Date.now();
      if ((this.expirationTime > 0 && this.expirationTime < now) ||
          (maxIdleSeconds > 0 && this.lastAccessTime + maxIdleSeconds * 1000 < now)) {
          return true;
      } else { return false; }
  }
  ```
- Read path that must enforce expiry — `src/nearcache/NearCache.ts` `get()`
  ```ts
  if (dr.isExpired(this.maxIdleSeconds)) { this.expireRecord(key); this.missCount++; return undefined; }
  dr.setAccessTime(); dr.hitRecord(); this.hitCount++;
  ```
- Test assertion pattern — `test/nearcache/NearCacheTest.js` ("ttl expire")
  ```js
  nearCache.put(ds('key'), 'val');
  return expect(promiseAfter(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key'))))
      .to.eventually.be.undefined;   // promiseAfter = ttlSec * 1500ms
  ```

## Bug Hypothesis
Primary suspect: **`expirationTime` is recomputed and `lastAccessTime`/`creationTime` are reset on the read-through publish path, sliding expiry forward forever.** `NearCache.ts` `tryPublishReserved()` calls `internalRecord.setCreationTime()` on every successful reserved publish (`DataRecord.ts:115` `setCreationTime` resets `creationTime = Date.now()` and `expirationTime = creationTime + ttl*1000`). For a `NearCachedMapProxy` whose reads go through reserve→get→publish, each cluster fetch re-stamps the record, so TTL is measured from the *last refresh*, not the original put — entries effectively never reach `expirationTime < now`.

Secondary/contributing suspects to verify:
1. **maxIdle slides on every read:** `get()` calls `dr.setAccessTime()` on every hit (`NearCache.ts` get), resetting `lastAccessTime = Date.now()`. So `maxIdleSeconds` is only honored if the entry is *never* read in the window — any read keeps it alive indefinitely. Matches "client keeps returning entries past expiry."
2. **Constructor only sets `expirationTime` when `ttl` truthy** (`DataRecord.ts:48`): records created via `tryReserveForUpdate` use `this.timeToLiveSeconds`; if a config layer passes ttl as a string or 0-default leaks, `expirationTime=undefined` and `undefined > 0` is `false` → never TTL-expires.
3. **Lazy-only expiry:** with `evictionPolicy=NONE` (the default, `NearCacheConfig.ts`) `doEvictionIfRequired()` returns early, so the sweep in `filterExpiredRecord` never runs. Expired entries persist until individually `get`-touched.

Strongest evidence to fix first: the `setCreationTime()`/`setAccessTime()` re-stamping in the read path (items 1 + primary). Reproduce by extending `NearCacheTest.js` ttl/maxIdle cases with an intervening `get()` before `promiseAfter`.

## Size Estimate
Files: ~2 (`src/nearcache/NearCache.ts`, `src/nearcache/DataRecord.ts`); config `NearCacheConfig.ts` read-only.
Layers: 1 (nearcache). Depth: surface (guard/ordering of expiry checks).
SIZE:S:localized expiry-check/re-stamp bug in 1-2 nearcache files, lazy read path

## Conflicts
NONE — single expiry implementation; no competing patterns in the layer.

## Rules for engineers
1. Modifying `NearCache.ts`/`DataRecord.ts` → match surrounding code: bluebird `Promise`, `Date.now()` ms, `* 1000` for sec→ms, no async/await.
2. Keep `0` = unlimited semantics; preserve `expiredCount`/`evictedCount` accounting.
3. Verify via `test/nearcache/NearCacheTest.js` style: `nearCache.put` then `promiseAfter(ttlSec, get)` asserts `undefined`; add a case with a read inside the window to lock maxIdle/ttl behavior.
4. Do not add a background timer unless the planner asks — expiry is lazy by design.
