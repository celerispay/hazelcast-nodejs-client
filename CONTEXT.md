# Context

## Current Task
Near-cache TTL rework (audit follow-up T6) SHIPPED to 3.12.x. Awaiting manual code
review + live-cluster testing before a release tag.

## Key Decisions
- L1 HIGH leak closed: removed per-NearCacheImpl setInterval; one NearCacheManager-owned .unref()'d timer, cleared on shutdown, skips ttl=0.
- TTL sweep reworked to a per-cache FIFO drain-from-front queue: O(expiring)/tick, no per-second full-cache scan (exploits constant-per-cache absolute TTL → put-order==expiration-order).
- Review PASS (0 crit/0 high); 8/8 auto UAT pass; cluster sign-off accepted as deferred.

## Next Steps
- Human: manual review + run `npm test` on a live Hazelcast member for H14 (TTL from original put) + H15 (clean exit after shutdown, no hung timer).
- If green → tag/release. If issues → resume csdlc from .sdlc/state.md (phase: done) with a new build worktree.
- Pointers: .sdlc/state.md, .sdlc/uat.md, HANDOFF.md (H14/H15). Commits eac1b80e..1152d99a.
