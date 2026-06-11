# Exploration: hazelcast-client
**Stack:** TypeScript 2.8.4 (ES5/CommonJS), bluebird Promises  **Tests:** Mocha 3.2.0 + Sinon + chai
**Authority:** map (.sdlc/celeris-codebase-map.md, fresh) + 4 near-cache source files read
**Nature:** backend (TS client library, no react/ai deps in package.json)

## Size Estimate
Files affected: ~2 (`src/nearcache/NearCache.ts`, `src/nearcache/DataRecord.ts`)
Layers touched: 1 (nearcache subsystem) — possibly +1 test file
Change depth: structural (eviction/expiry logic, single module)
→ Recommended: S — bugfix scoped to 1 module, 2 src files + 1 test, no public API change

**Change scope:** `src/nearcache/NearCache.ts`, `src/nearcache/DataRecord.ts`, `test/nearcache/NearCacheTest.js`

### Bug evidence (unbounded growth)
- `isEvictionRequired()` = `evictionPolicy !== NONE && evictionMaxSize <= size`. Default `evictionPolicy = NONE` (NearCacheConfig.ts) → size-based eviction is **disabled by default**, so `evictionMaxSize` is never enforced unless a policy is set.
- TTL / maxIdle removal is **passive only**: expired records are dropped lazily inside `get()` (`isExpired(maxIdleSeconds)`) and during `recomputeEvictionPool()` sampling. An entry that is written and never read again is never proactively expired → memory grows. No background reaper exists.
- `DataRecord.isExpired` reads `this.expirationTime > 0`, but `expirationTime` is a `Date.now()+ttl*1000` epoch ms value (always > 0 when ttl set) — confirm the guard intent; `ttl===0`/`undefined` → `expirationTime = undefined` → never TTL-expires (by design).

## Golden Examples

### Near-cache store/expiry [authoritative — only impl]
`src/nearcache/NearCache.ts`
```ts
protected isEvictionRequired(): boolean {
    return this.evictionPolicy !== EvictionPolicy.NONE && this.evictionMaxSize <= this.internalStore.size;
}
protected doEvictionIfRequired(): void {
    if (!this.isEvictionRequired()) { return; }
    const internalSize = this.internalStore.size;
    if (this.recomputeEvictionPool() > 0) { return; }
    else { this.evictRecord(this.evictionCandidatePool[0].key); this.evictionCandidatePool = this.evictionCandidatePool.slice(1); }
}
```

### Record expiry check [authoritative]
`src/nearcache/DataRecord.ts`
```ts
isExpired(maxIdleSeconds: number): boolean {
    const now = Date.now();
    if ((this.expirationTime > 0 && this.expirationTime < now) ||
        (maxIdleSeconds > 0 && this.lastAccessTime + maxIdleSeconds * 1000 < now)) {
        return true;
    }
    return false;
}
```

### Lifecycle (create/destroy) [authoritative]
`src/nearcache/NearCacheManager.ts`
```ts
public getOrCreateNearCache(name: string): NearCache {
    let nearCache = this.caches.get(name);
    if (nearCache == null) {
        nearCache = new NearCacheImpl(this.client.getConfig().getNearCacheConfig(name),
            this.client.getSerializationService());
        this.caches.set(name, nearCache);
    }
    return nearCache;
}
public destroyNearCache(name: string): void {
    const nc = this.caches.get(name);
    if (nc != null) { this.caches.delete(name); nc.clear(); }
}
```

### Tests [Mocha + chai-as-promised, parameterized configs]
`test/nearcache/NearCacheTest.js`
```js
var NearCacheImpl = require('../../lib/nearcache/NearCache').NearCacheImpl;
var EvictionPolicy = Config.EvictionPolicy;
evictionPolicy.forEach(function (evictionPolicy) {
  ttls.forEach(function (ttl) {
    var ncc = new Config.NearCacheConfig();
    ncc.timeToLiveSeconds = ttl; ncc.evictionMaxSize = 100; ncc.evictionPolicy = evictionPolicy;
    testConfigs.push(ncc);
  });
});
```

## Conflicts
NONE — single implementation per layer; no competing patterns.

## Rules for engineers
1. **Modifying existing file** → match surrounding code (bluebird Promises, `this.`-prefixed private fields, `protected` eviction helpers). Golden examples are the file you are editing.
2. Preserve the Apache 2.0 header on every file. Public API of `NearCache` interface must not change (out of scope per task).
3. 64-bit values use `long`; do not use JS numbers for reservation ids/sequences.
4. No native async/await — return `Promise<T>` (bluebird) to match `NearCache.get`.
5. Test style: Mocha `describe`/`it` in `test/nearcache/*Test.js`, run against compiled `lib/`; require impl from `../../lib/nearcache/...`; use chai `expect` + `chai-as-promised`; parameterize over eviction policies/ttls as the existing suite does. Time-based tests use `promiseLater` from `../Util`.
