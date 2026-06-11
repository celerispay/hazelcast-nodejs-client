# hazelcast-client — Requirements

**Type:** Bugfix
**Stack:** Node.js / TypeScript (Hazelcast client, TS 2.8.4 → CommonJS)
**Size:** TBD

## Description
Near Cache memory grows unbounded — entries are not evicted as expected (eviction
policy / max-size / TTL not being respected). Observed in production with no isolated
reproduction yet; an isolated repro will likely need to be written as part of the fix.

## Constraints
- Follow existing conventions strictly — no new patterns
- Must not change the public API / interface

## Out of Scope
- No upstream version bump (no rebase on newer Hazelcast IMDG base)
- No unrelated refactors (leave oversized ClusterService / ConnectionManager as-is)
- Near Cache only — no changes to failover, serialization, or other subsystems
