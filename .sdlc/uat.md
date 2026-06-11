# UAT Results — hazelcast-client
**Date:** 2026-06-11  **Sign-off:** mixed (auto + human)

## Summary
Scenarios: 15 | Auto: 12 | Human: 3 | Passed: 12 | Failed: 0 | Skipped: 3 (human — deferred to JVM+cluster env)

Note: all 12 static/code checks pass. The 3 runtime scenarios (S13 full mocha suite, H14
read-through TTL repro, H15 clean-shutdown/no-timer-leak) are deferred — they need a live
JVM + hazelcast-remote-controller + cluster, unavailable here. Runnable commands are in the
Results notes / UAT checklist; verify in a JVM-equipped environment before release.

## Results
| Scenario | Type | Result | Note |
|----------|------|--------|------|
| S1 TTL absolute-from-put guard in setCreationTime | auto | ✅ PASS | guard present, early-returns |
| S2 ctor sets expirationTime = creationTime + ttl*1000 | auto | ✅ PASS | |
| S3 reclamation decoupled — doExpiration not on write/read path | auto | ✅ PASS | only call is the setInterval bind |
| S4 background timer (const, startExpirationTask, setInterval, .unref) | auto | ✅ PASS | mirrors RepairingTask |
| S5 sweep TTL-only (isExpired(0) → expireRecord) | auto | ✅ PASS | expiredCount accounting preserved |
| S6 destroy() clears interval + handle + store | auto | ✅ PASS | no timer leak |
| S7 NearCacheManager.destroyNearCache calls destroy() | auto | ✅ PASS | torn down on client shutdown |
| S8 three regression tests present by name | auto | ✅ PASS | |
| S9 reclaim test asserts expiredCount/size, no trigger put, destroy teardown | auto | ✅ PASS | |
| S10 max-idle reset-on-read preserved (setAccessTime) + test off cluster | auto | ✅ PASS | |
| S11 NearCacheConfig + src/codec untouched (negative) | auto | ✅ PASS | constraints honored |
| S12 test file parses (node --check) | auto | ✅ PASS | |
| S13 full near-cache mocha suite green vs live cluster | human | ⏭️ PENDING | needs JVM + remote-controller |
| H14 read-through TTL repro (TTL from original put, not last refresh) | human | ⏭️ PENDING | needs live cluster |
| H15 no timer leak — process exits cleanly after shutdown | human | ⏭️ PENDING | needs live client/cluster |
