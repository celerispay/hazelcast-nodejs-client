# Handoff — Near-Cache TTL Expiration Fix

**Package:** `@celerispay/hazelcast-client`
**Type:** Bugfix (existing published library)
**Branch:** `3.12.x` @ `eac1b80e`
**Date:** 2026-06-12 (audit-follow-up rework merged)

> ⚠ **Action required before release:** 2 UAT scenarios that need a live JVM +
> `hazelcast-remote-controller` + Hazelcast cluster (**H14** and **H15**) have **not**
> been run in this environment. Everything else is verified. See
> [Deferred verification](#-deferred-verification-action-required-before-release).

---

## Summary

Near-cache entries configured with a TTL (`timeToLiveSeconds`) were not expiring: the
client kept returning (and retaining) entries past their configured expiry. This fix makes
TTL **absolute from the original put**, and adds **proactive background reclamation** of
TTL-expired records so expired memory is freed even when the entry is never read again.
Max-idle (`maxIdleSeconds`) reset-on-read behavior is intentionally preserved.

Reclamation is driven by a **single shared timer owned by `NearCacheManager`** (not a timer
per near cache) and each cache sweeps via an insertion-ordered **FIFO expiration queue**
drained from the front. This design is the audit-follow-up rework: the original fix used a
per-`NearCacheImpl` `setInterval` plus a full O(n) store scan each tick, which a pre-release
audit flagged as a HIGH timer/memory leak (finding **L1**) — both are now removed. See
[Audit L1 — CLOSED](#audit-l1-high--closed).

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
- **No per-instance timer.** `NearCacheImpl` owns no `setInterval`/`clearInterval` and no
  timer handle. The TTL sweep is owned entirely by `NearCacheManager`'s single shared timer
  (below); this file only exposes the per-cache sweep method the manager calls.
- New **FIFO expiration queue** for TTL reclamation, fully **decoupled from put/get**:
  - `expirationQueue: ExpirationQueueEntry[]` + an `expirationQueueHead` cursor. Records with
    a finite `expirationTime` (`ttl > 0`) are appended in **put order** by `enqueueExpiration()`
    from the two record-creation paths (`put()` and `tryReserveForUpdate()`). Because TTL is
    constant per cache and `expirationTime` is **absolute** (post-T1 it never slides on read),
    **put-order == expiration-order**: the head is always the soonest to expire.
  - `doExpiration()` — **drains expired entries from the FRONT** of the queue and stops at the
    first non-expired head (`head.expirationTime > now` → break). Cost is
    **O(entries actually expiring this tick)**, not O(store size) — and there is no per-tick
    snapshot array. Removal of a record uses `isExpired(0)` (the `0` disables the max-idle
    branch, so the sweep is TTL-only; max-idle stays lazy on the read path) routed through
    `expireRecord()`, so `expiredCount` accounting stays consistent and never double-counts a
    key already expired on the read path.
  - **Stale/superseded entries are dropped lazily on drain**: a head whose key is gone
    (`cur === undefined`) or whose live record no longer carries the queued `expirationTime`
    (a newer generation re-put) is popped and skipped without touching the store.
  - `compactExpirationQueueIfNeeded()` bounds queue growth: a `slice()` reclaims the consumed
    prefix once the cursor passes the array midpoint, and an `overStale` rebuild (triggered
    when live entries exceed `EXPIRATION_QUEUE_STALE_FACTOR * store.size`) keeps **≤ 1 element
    per live key**, collapsing duplicate same-key generations from overwrite churn.
  - `clear()` and `destroy()` both call `resetExpirationQueue()` (empties the array, resets the
    cursor) so the queue never survives teardown and the cursor is never left past a shrunk
    array.
- **Reclamation is fully decoupled from put/get.** An earlier write-path-coupled approach
  (sweep gated on `put()` / `tryReserveForUpdate()`) was implemented, then **rejected in
  review** for making every put pay a time check and forcing the put that crosses the
  throttle window to eat the full O(n) scan. The FIFO queue replaces both that and the later
  per-tick full-store scan.
- The lazy `get()`-path expiry (`isExpired(maxIdleSeconds)` → `expireRecord`) is **retained
  as the read-time correctness guarantee** — `get()` never returns a TTL-expired entry. The
  background sweep handles memory reclamation; the read path handles correctness.

### `src/nearcache/NearCacheManager.ts`
- **Owns the single shared TTL-sweep timer** (mirrors the existing `RepairingTask` pattern):
  - `EXPIRATION_TASK_INTERVAL_MS = 1000` (sweep cadence in ms).
  - `startExpirationTask()` — one `setInterval(this.doExpiration, …)` stored on the manager,
    **lazily started** on first near cache (`getOrCreateNearCache` starts it only when
    `expirationTaskHandle === undefined`) and `.unref()`'d (guarded `typeof … === 'function'`)
    so the timer never keeps the Node process alive on its own.
  - `doExpiration()` — each tick iterates `Array.from(this.caches.values())` and calls a
    cache's `doExpiration()` **only when `getTimeToLiveSeconds() > 0`**, skipping the default
    no-TTL config entirely (audit **L5/L6**: no wasted scan, one timer instead of N).
  - The timer is cleared exactly once on `HazelcastClient.shutdown()` →
    `destroyAllNearCaches()` (guarded `!= null`, then handle set `undefined` — idempotent).
- `destroyNearCache(name)` removes the cache from the map and calls `nearCache.destroy()`
  (which now only clears the store + expiration queue — there is **no per-cache timer to
  clear**).

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

The background expiration sweep mirrors the existing `RepairingTask` near-cache
background-maintenance pattern: **one** periodic timer owned by the manager, lazily started
on first near cache and cleared once on client shutdown — which is why a `setInterval`-style
task is acceptable here despite the otherwise lazy design.

This proactive sweep is an **addition relative to upstream**: the vanilla Hazelcast Node.js
client expires near-cache entries lazily (read-path only). The single `.unref()`'d
manager-owned handle, cleared in `destroyAllNearCaches()` on `HazelcastClient.shutdown()`,
ensures the process can exit cleanly with no leaked timer.

**Why the FIFO queue (resource efficiency).** The original fix swept every TTL-enabled cache
with a full `Array.from(internalStore.values())` scan each tick — **O(store size) per tick**,
forever, even when nothing was expiring. The per-cache FIFO expiration queue replaces that
with a front-drain that is **O(entries actually expiring this tick)** and allocates no
per-tick snapshot array, so an idle-but-large near cache costs ~nothing per tick. Combined
with the ttl=0 skip, the steady-state sweep cost is proportional to actual expiry traffic,
not to cache size or cache count.

## ⚠ Deferred verification (action required before release)

This is **the only thing still gated before release.** The 2 runtime UAT scenarios below
require a live JVM + `hazelcast-remote-controller` + a Hazelcast cluster, which were **not
available in this environment**. Everything else (no-per-cache-timer, single manager timer,
ttl=0 skip, FIFO drain/overwrite/delete behavior, TTL-absolute, max-idle reset-on-read) was
verified automatically this session (see `.sdlc/uat.md`). Whoever has a cluster must run
these before release:

| ID  | Scenario | Why deferred |
|-----|----------|--------------|
| H14 | Read-through TTL repro — TTL expires from the ORIGINAL put, not the last refresh | needs live cluster |
| H15 | Clean process exit after `client.shutdown()` — no hung timer | needs live client/cluster |

Runnable command (from `.sdlc/uat.md`):

```bash
# Full near-cache suite against a live cluster (npm test auto-provisions
# hazelcast-remote-controller + Hazelcast IMDG via Maven and starts a member):
npm test

# or the targeted near-cache spec (requires a running cluster / remote-controller):
npx mocha test/nearcache/NearCacheTest.js --reporter spec
```

- **H14** is exercised by the read-through path through `NearCachedMapProxy` against a live
  cluster; confirm a frequently-refreshed TTL key still expires on schedule from its original
  put. The in-process regression test `ttl expires even with an intervening read inside the
  window` covers the same invariant without a cluster, but the read-through repro itself needs
  the cluster.
- **H15** confirms the single `.unref()`'d **manager** timer is cleared on
  `HazelcastClient.shutdown()` → `destroyAllNearCaches()`, leaving no lingering `setInterval`
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
| `4bfb07f7` | T4   | Reclaim via background task; decouple from put/get |
| `29c92c50` | T5   | Polish: clarify `setCreationTime` guard; move pure max-idle test off cluster fixture |
| `b8a0b381` | T6   | **Audit rework:** single `NearCacheManager`-level timer (ttl=0 skipped); remove per-`NearCacheImpl` timer |
| `c1d4ab1a` | T6   | **Audit rework:** per-cache FIFO expiration queue (front-drain) replaces the O(n) full-store scan |

Merged at `eac1b80e`. (The intermediate `af14e1df` write-path throttle was superseded by the
T4 background-task rework after the user rejected coupling reclamation to the write path; the
T4 per-`NearCacheImpl` timer was in turn replaced by the T6 manager-owned timer + FIFO queue
after the pre-release audit found finding L1.)

## Audit L1 (HIGH) — CLOSED
A pre-release audit (regression / security / memory — `.sdlc/pre-release-audit.md`) found the
**original** per-`NearCacheImpl` `setInterval` design carried a **HIGH timer/memory leak
(L1)**: `map.destroy()` skips near-cache teardown (`postDestroy` → `destroyNearCache` →
`destroy()` → `clearInterval`) when the server destroy round-trip **rejects** (cluster
instability), leaving a per-cache 1s timer firing — and the bound callback pinning the whole
`NearCacheImpl` + store — until client shutdown. Regression and security were CLEAN (security
a net improvement); L2/L5/L6 were efficiency findings.

**How it is closed (T6 rework):**
- There is **no per-`NearCacheImpl` timer anymore.** `NearCacheImpl.destroy()` now only clears
  the store + the expiration queue — there is no timer to clear. A missed proxy-destroy
  therefore degrades to **benign dormant retention** (store + queue stay referenced until
  client shutdown), exactly the **same failure mode as before the original fix** — no live
  timer can leak. → **L1 CLOSED.**
- The sole sweep timer is the single `.unref()`'d `NearCacheManager` interval, lazily started
  on first near cache and cleared exactly once on `HazelcastClient.shutdown()` →
  `destroyAllNearCaches()`.
- The manager tick skips caches with `timeToLiveSeconds === 0` (**L5**), uses one timer for
  all caches instead of N (**L6**), and the per-cache FIFO front-drain replaces the O(n)
  per-tick full-store scan (**L2/L5/L6** efficiency).

**Re-verified by review base-vs-HEAD** (`.sdlc/review.md`, cumulative diff `3a5571b8..HEAD`):
a `grep` of the current `NearCache.ts` for `setInterval|clearInterval|startExpirationTask|
expirationTaskHandle|unref` returns NONE; manager timer ownership and clear-on-shutdown were
verified end-to-end. Verdict: **CONFIRMED CLOSED** — CRITICAL 0 / HIGH 0 / MEDIUM 0 / LOW 2
(both LOW are optional readability nits). Merge approved.
