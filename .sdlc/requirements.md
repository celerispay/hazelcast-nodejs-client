# hazelcast-client — Requirements

**Type:** Bugfix
**Stack:** TypeScript 2.8.4 / Node.js (bluebird Promises) — @celerispay/hazelcast-client

## Description
Near-cache entries are not expiring. TTL (`timeToLiveSeconds`) and/or max-idle
(`maxIdleSeconds`) entries are not being evicted/removed from the near cache when they
should be, so the client keeps returning entries past their configured expiry.

Area: `src/nearcache/` (NearCache, DataRecord, NearCacheManager, NearCacheConfig).

## Constraints
- No new dependencies — use existing libs only.
- Follow existing conventions strictly — stay bluebird-consistent, match near-cache patterns, no new architectural patterns.
- Must not change the public API / interface (no changes to `NearCacheConfig` surface or client-facing interfaces).

## Out of Scope
- No codec / protocol changes — stay out of `src/codec`, no wire-protocol changes.

## Discussion Notes
Q: Is max-idle reset-on-read the bug, or should it be preserved?
A: Reset-on-read is fine/intentional (max-idle = time since last access). The bug is
   TTL not expiring. Fix the TTL path only; do NOT change max-idle reset-on-read.

Q: Correctness of returned values only, or also memory reclamation?
A: Both. get() must never return a TTL-expired entry, AND expired entries must be
   proactively reclaimed (removed even if never read again) — not left lingering until
   a get touches them. A TTL sweep must work regardless of evictionPolicy=NONE.

Q: How does the bug reproduce?
A: Not certain yet — engineer to confirm exact repro (map read-through via
   NearCachedMapProxy vs. direct put/get) while fixing. Lead suspect: read-through
   publish (`tryPublishReserved` -> `setCreationTime`) re-stamps `expirationTime`,
   sliding TTL forward on every refresh so TTL is measured from last refresh, not put.

### Refined fix scope
1. TTL must be absolute from the original put: the read-through publish path must not
   reset creationTime/expirationTime for an already-reserved record (preserve original
   TTL window). Verify constructor sets expirationTime correctly for ttl>0.
2. Preserve max-idle reset-on-read behavior unchanged.
3. Add proactive reclamation of TTL-expired records independent of evictionPolicy
   (=NONE default) so expired memory is freed, not just hidden on read. Keep
   `0` = unlimited semantics and `expiredCount` accounting.
