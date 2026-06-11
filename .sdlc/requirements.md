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
