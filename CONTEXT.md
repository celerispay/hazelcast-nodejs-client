# Context

## Current Task
Near-cache TTL-expiry bugfix shipped via csdlc pipeline (merged to 3.12.x), then a
pre-release audit found a HIGH timer/memory leak — fix deferred to next session.

## Key Decisions
- Fix: TTL absolute from original put (setCreationTime guard) + background TTL sweep, decoupled from put/get.
- Audit verdict: regression SAFE, security SAFE (net improvement), but memory LEAK FOUND (L1 HIGH).
- HOLD release until L1 fixed. Planned: move sweep to one NearCacheManager-level timer gated on ttl>0.

## Next Steps
- Resume csdlc (T6): implement the manager-level shared sweep per .sdlc/pre-release-audit.md (fixes L1+L2+L5+L6).
- New celeris/build worktree → drop per-NearCacheImpl setInterval → re-run reclaim test + re-review → re-ship.
- State pointer: .sdlc/state.md (phase: build, sub: audit-followup, T6○).
