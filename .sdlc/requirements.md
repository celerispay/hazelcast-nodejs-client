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

## Discussion Notes
Q: In-scope definition of the bug?
A: Proactive reclamation of TTL/maxIdle entries only. Add proactive expiry so
   configured TTL/maxIdle entries are freed even when never read again. Do NOT change
   config defaults and do NOT change size-eviction (evictionMaxSize/policy) semantics.

Q: How should proactive expiry be triggered?
A: Whichever mechanism best fits existing near-cache conventions — planner/engineer
   chooses (background reaper vs. proactive sweep). If a timer is used it MUST be
   registered/cleared in the create/destroy lifecycle and unref'd so it never blocks
   process exit.

Q: How to verify, given no isolated repro yet?
A: TDD — write a failing near-cache unit test that demonstrates unbounded growth
   (TTL/maxIdle entries not reclaimed without access), then implement the fix to make
   it pass. Follow the existing parameterized Mocha test style in test/nearcache/.
