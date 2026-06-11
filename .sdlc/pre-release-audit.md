# Pre-Release Audit — near-cache TTL fix
**Date:** 2026-06-11  **Branch:** 3.12.x (fix already merged)  **Verdict:** HOLD release until L1 fixed

Triggered by user request: "double check if it can cause regression or memory leaks or
any security vulnerabilities." Three independent adversarial audits (regression, memory/
timer leak, security) + orchestrator verification of the gating control-flow.

## Summary
| Axis | Verdict |
|------|---------|
| Regression | ✅ SAFE — no regression (6 hypotheses cleared) |
| Security | ✅ SAFE — net improvement (TTL no longer slides → revoked-token-style data expires on schedule) |
| Memory / timers | ⚠️ LEAK FOUND — L1 HIGH (gating), plus L2/L5/L6 |

## Findings (memory/timer)

### L1 — HIGH — timer+store leak when map.destroy() server round-trip fails  ← GATING
- `BaseProxy.destroy()` (src/proxy/BaseProxy.ts:64-67) = `destroyProxy(name).then(() => postDestroy())`.
- `ProxyManager.destroyProxy` (src/proxy/ProxyManager.ts:128-133) does `invokeOnRandomTarget(...).return()` — a network round-trip.
- On REJECT (cluster hiccup / timeout / target disconnected), `.then(postDestroy)` is skipped.
- `NearCachedMapProxy.postDestroy` (src/proxy/NearCachedMapProxy.ts:89-91) → `destroyNearCache` → `nearCache.destroy()` → `clearInterval` therefore never runs.
- Result: the NearCacheImpl stays in `NearCacheManager.caches` with its 1s `setInterval` firing forever (cleared only at client shutdown via destroyAllNearCaches). The bound `doExpiration` callback pins the whole NearCacheImpl + internalStore (`.unref()` does NOT remove GC reachability).
- Pre-fix this path left only dormant retained memory (no timer). The fix newly converts it into a live perpetually-firing timer → genuinely new leak on a routine op under realistic cluster instability. Accumulates one leaked timer+store per distinct near-cache name destroyed-during-error.

### L2 — MEDIUM — orphaned timer on proxy-create failure
- NearCacheImpl timer starts in its constructor (src/nearcache/NearCache.ts:124), invoked from `getOrCreateNearCache` inside the NearCachedMapProxy constructor (src/proxy/ProxyManager.ts:101) BEFORE `createProxy` resolves.
- If createProxy rejects (no `.catch` on the deferred chain), the near cache is already registered in `caches` with a live timer; reclaimed only at client shutdown.

### L5 — LOW–MEDIUM — wasted work for default (no-TTL) config
- NearCacheConfig defaults: `timeToLiveSeconds = 0`, `maxIdleSeconds = 0` (src/config/NearCacheConfig.ts:30,35).
- For ttl=0, DataRecord.expirationTime is undefined → `isExpired(0)` always false. `doExpiration` (NearCache.ts:293-300) still does a full `Array.from(internalStore.values())` + scan every 1s forever and can never expire anything. Pure waste + GC churn for the common no-TTL/invalidation-only config.

### L6 — MEDIUM — N timers for N near-cached maps
- One independent 1s timer per NearCacheImpl, each doing a full O(n) snapshot. The codebase's RepairingTask uses a single shared scheduler; this fix does not.

## Recommended fix (resolves L1 + L2 + L5 + L6 together)
Move the sweep to a SINGLE `NearCacheManager`-level scheduled task:
1. One `setInterval` owned by NearCacheManager (not per NearCacheImpl), `.unref()`'d.
2. Each tick: iterate `listAllNearCaches()`, and for each cache with `timeToLiveSeconds > 0`, run its TTL-only sweep (expose a `doExpiration()`/`reclaimExpired()` method on the NearCache interface; the manager calls it).
3. Skip caches with `timeToLiveSeconds === 0` entirely (L5).
4. Start the manager timer lazily (first near cache created, or in manager ctor) and clear it in client shutdown (HazelcastClient.shutdown already calls destroyAllNearCaches — add the timer-clear there or in a NearCacheManager.shutdown()).
5. Remove the per-NearCacheImpl `startExpirationTask`/`setInterval`. Keep `destroy()` (clears store) and `clear()` as-is. With no per-cache timer, a missed proxy-destroy (L1/L2) degrades to the SAME benign dormant-retention as before the fix — no live timer leaks.

Regression/security audits remain valid after this rework (sweep logic is unchanged, only its owner/cadence moves). Re-verify the reclaim regression test still passes (it constructs NearCacheImpl directly — the test must now drive the sweep via the manager OR call a public reclaim method directly).

## NOT blocking
- Regression: none. Security: none (net improvement). L5/L6 are efficiency; fixed for free by the rework above.

## Audit provenance
- Regression audit: SAFE (H1-H6 all confirmed-safe).
- Security audit: NO SECURITY ISSUE (S1-S6); S4 = net improvement.
- Memory/leak audit: LEAK FOUND (L1 HIGH, L2 MED, L5 LOW-MED, L6 MED); L3/L4 confirmed-safe.
- Orchestrator independently verified L1 control flow against BaseProxy.ts / ProxyManager.ts.
