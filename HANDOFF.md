# Handoff — Near-Cache TTL Expiration Fix

**Package:** `@celerispay/hazelcast-client`
**Type:** Bugfix (existing published library)
**Branch:** `celeris`
**Date:** 2026-06-11

> ⚠ **Action required before release:** 3 UAT scenarios that need a live JVM +
> `hazelcast-remote-controller` + Hazelcast cluster have **not** been run in this
> environment. The integration green-gate was **not** executed here. See
> [Deferred verification](#-deferred-verification-action-required-before-release).

---

## Summary

Near-cache entries configured with a TTL (`timeToLiveSeconds`) were not expiring: the
client kept returning (and retaining) entries past their configured expiry. This fix makes
TTL **absolute from the original put**, and adds **proactive background reclamation** of
TTL-expired records so expired memory is freed even when the entry is never read again.
Max-idle (`maxIdleSeconds`) reset-on-read behavior is intentionally preserved.

## Root Cause

The read-through publish path slid the TTL window forward on every cluster refresh:

```
NearCachedMapProxy read-through
  → NearCache.tryPublishReserved(key, value, reservationId)
    → DataRecord.setCreationTime()      // re-stamped creationTime + expirationTime
```

`setCreationTime()` recomputed `expirationTime = creationTime + ttl*1000` against the
*latest* refresh time. So TTL was effectively measured from the **last refresh**, not the
**original put** — a frequently-refreshed key never aged out.

## What Changed (per file)

### `src/nearcache/DataRecord.ts`
- `setCreationTime()` now **early-returns** for records where `ttl > 0 && expirationTime > 0`
  (i.e. an absolute-TTL record whose `expirationTime` was already computed in the
  constructor). TTL is therefore anchored to the original put and the read-through publish
  path can no longer re-stamp it.
- The early-return **intentionally drops** the optional `creationTime` argument for
  absolute-TTL records. This is deliberate (re-stamping would extend the entry past its
  original expiration) and is documented inline as the WHY. The only caller today
  (`tryPublishReserved`) passes no argument, so the contract is safe.
- The constructor behavior for `ttl > 0` is unchanged and verified: it sets
  `expirationTime = creationTime + ttl * 1000`. `ttl = 0` → `expirationTime = undefined`
  (unlimited).

### `src/nearcache/NearCache.ts`
- New background TTL-sweep task, fully **decoupled from put/get**:
  - `EXPIRATION_TASK_INTERVAL_MS = 1000` (sweep cadence in ms).
  - `startExpirationTask()` — `setInterval(this.doExpiration, …)`, stores the handle on the
    instance, and calls `.unref()` on it so a live timer never keeps the Node process alive
    on its own.
  - `doExpiration()` — snapshots the store (`Array.from(internalStore.values())`) and removes
    only **TTL-expired** records via `isExpired(0)` → `expireRecord()`. Passing `0` disables
    the max-idle branch of `isExpired`, so the sweep is TTL-only; max-idle stays lazy on the
    read path. Removal routes through `expireRecord()` so `expiredCount` accounting stays
    consistent and cannot double-count keys already expired on the read path.
  - The task is started in the constructor and stopped in the new `destroy()`.
  - `destroy()` clears the interval (guarded by a null check), nulls the handle, and clears
    the store.
- **Reclamation is fully decoupled from put/get.** An earlier write-path-coupled approach
  (sweep gated on `put()` / `tryReserveForUpdate()`) was implemented, then **rejected in
  review** for making every put pay a time check and forcing the put that crosses the
  throttle window to eat the full O(n) scan — degrading puts exactly at peak load. The
  `doExpiration()` calls were removed from the write path.
- The lazy `get()`-path expiry (`isExpired(maxIdleSeconds)` → `expireRecord`) is **retained
  as the read-time correctness guarantee** — `get()` never returns a TTL-expired entry. The
  background task handles memory reclamation; the read path handles correctness.

### `src/nearcache/NearCacheManager.ts`
- `destroyNearCache(name)` now calls `nearCache.destroy()` before/after removing the cache
  from its map, so the background timer is torn down on client shutdown
  (`destroyAllNearCaches()` fans out to it). This prevents a timer leak when caches go away.

### `test/nearcache/NearCacheTest.js`
Three regression tests added:
1. **`ttl expires even with an intervening read inside the window`** — put, read mid-window
   (asserts the value is still present), then read after the window (asserts `undefined`).
   Guards against the re-stamp regression: a read inside the window must not extend TTL.
2. **`proactively reclaims a ttl-expired entry that is never read again`** — under
   `EvictionPolicy.NONE`, put an orphan, never read it, never issue a second put; after the
   TTL + a sweep tick, assert `expiredCount > 0` and `entryCount === 0`. Calls
   `nearCache.destroy()` in teardown (both success and error paths) so no `setInterval`
   leaks.
3. **`max-idle still resets on read`** — in a top-level sibling `describe` with **no**
   cluster `before` hook (pure in-process `NearCacheImpl`): repeated reads inside the idle
   window keep the entry alive past the original max-idle window, confirming reset-on-read is
   preserved. Also tears down via `destroy()`.

## Behavior Guarantees

- **Max-idle reset-on-read preserved** (intentional per requirements): `setAccessTime()` and
  the `get()` path are unchanged. Max-idle = time since last access.
- **`0` = unlimited preserved**: `ttl = 0` → `expirationTime = undefined` → `isExpired(0)` is
  false → never swept.
- **`expiredCount` accounting preserved**: all removals route through `expireRecord()`, which
  increments only when `internalStore.delete(key)` returns true (no double-count between the
  sweep and lazy read-path expiry).
- **No public API change**: no change to the `NearCacheConfig` surface or any client-facing
  interface.
- **No codec / wire-protocol change**: `src/codec` untouched.
- **No new dependencies**; stays bluebird-Promise-consistent (no async/await).

## Design Note

The background expiration task mirrors the existing `RepairingTask` near-cache
background-maintenance pattern (periodic timer, started with the cache, cleared on teardown),
which is why a `setInterval`-style task is acceptable here despite the otherwise lazy design.

This proactive sweep is an **addition relative to upstream**: the vanilla Hazelcast Node.js
client expires near-cache entries lazily (read-path only). `.unref()` on the handle plus
`destroy()` teardown (wired through `NearCacheManager.destroyNearCache`) prevent timer leaks
and ensure the process can exit cleanly.

## ⚠ Deferred verification (action required before release)

The 3 runtime UAT scenarios below require a live JVM + `hazelcast-remote-controller` + a
Hazelcast cluster, which were **not available in this environment**. The full near-cache
integration suite green-gate was **not executed here**. Whoever has a cluster must run these
before release:

| ID  | Scenario | Why deferred |
|-----|----------|--------------|
| S13 | Full near-cache mocha suite green vs. live cluster | needs JVM + remote-controller |
| H14 | Read-through TTL repro — TTL measured from original put, not last refresh | needs live cluster |
| H15 | No timer leak — process exits cleanly after client shutdown | needs live client/cluster |

Runnable commands (from `.sdlc/uat.md` / below):

```bash
# S13 — full near-cache suite against a live cluster (auto-downloads remote-controller + IMDG via Maven)
npm test

# or the targeted near-cache spec (requires a running cluster / remote-controller):
npx mocha test/nearcache/NearCacheTest.js --reporter spec
```

- **H14** is exercised by the read-through path through `NearCachedMapProxy` against a live
  cluster; confirm a frequently-refreshed TTL key still expires on schedule from its original
  put. The in-process regression test `ttl expires even with an intervening read inside the
  window` covers the same invariant without a cluster, but the read-through repro itself needs
  the cluster.
- **H15** confirms the `.unref()` + `destroy()` teardown leaves no lingering `setInterval`
  handle — start a client with a near-cached map, shut it down, and confirm the Node process
  exits cleanly (no hang).

## How to verify locally

```bash
npm install
npm run compile
# then, with a cluster / remote-controller available:
npx mocha test/nearcache/NearCacheTest.js --reporter spec
```

The pure in-process tests (e.g. `max-idle still resets on read`, and `CacheRecord`
expiry tests) run without a cluster; the cluster-backed `describe` blocks need a live member.

## Key Commits

| Commit     | Task | Description |
|------------|------|-------------|
| `083d1eea` | T1   | TTL absolute from original put; guard `setCreationTime` re-stamp |
| `f13cde01` | T2   | Proactively reclaim TTL-expired records regardless of eviction policy |
| `48502cc7` | T3   | Regression tests (TTL expiry, proactive reclaim, max-idle reset-on-read) |
| `4bfb07f7` | T4   | **Rework:** reclaim via background task; decouple from put/get |
| `29c92c50` | T5   | Polish: clarify `setCreationTime` guard; move pure max-idle test off cluster fixture |

(The intermediate `af14e1df` write-path throttle was superseded by the T4 background-task
rework after the user rejected coupling reclamation to the write path.)
