project: hazelcast-client | type: Bugfix | size: S | nature: backend
phase: done | step: 4/4 | branch: 3.12.x | base: 3.12.x
worktree: none
tasks: T1✓ T2✓ T3✓ T4✓ T5✓ T6✓(manager timer + fifo expiry queue)
para: A=T1,T2 B=T3 C=T4 D=T5 E=T6(rework sweep -> NearCacheManager-level, gated ttl>0)
next: DONE -> T6 shipped (manager timer + FIFO expiry queue, eac1b80e..c2961b16 on 3.12.x). L1 HIGH closed. Only remaining: deferred cluster sign-off H14/H15 (needs live member) before release.
started: 2026-06-11 | updated: 2026-06-12
