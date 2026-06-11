project: hazelcast-client | type: Bugfix | size: S | nature: backend
phase: build | step: 4/4 | sub: audit-followup | branch: 3.12.x | base: 3.12.x
worktree: none
tasks: T1✓ T2✓ T3✓ T4✓ T5✓ T6○
para: A=T1,T2 B=T3 C=T4 D=T5 E=T6(rework sweep -> NearCacheManager-level, gated ttl>0)
next: RESUME -> implement T6 per .sdlc/pre-release-audit.md (fixes L1 HIGH leak + L2/L5/L6). Create a new celeris/build worktree, move TTL sweep to a single NearCacheManager timer gated on timeToLiveSeconds>0, drop per-NearCacheImpl setInterval, re-run reclaim regression test + re-review, then re-ship.
started: 2026-06-11 | updated: 2026-06-11
