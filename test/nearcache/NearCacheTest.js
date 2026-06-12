/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var chai = require('chai');
var chaiAsPromised = require('chai-as-promised');
chai.use(chaiAsPromised);
var expect = chai.expect;
var Config = require('../../.').Config;
var Controller = require('../RC');
var HazelcastClient = require('../../.').Client;
var DataRecord = require('../../lib/nearcache/DataRecord').DataRecord;
var NearCacheImpl = require('../../lib/nearcache/NearCache').NearCacheImpl;
var EvictionPolicy = Config.EvictionPolicy;
var promiseLater = require('../Util').promiseLater;
var SerializationService = require('../../lib/serialization/SerializationService').SerializationServiceV1;
describe('NearCacheImpl', function () {
    var cluster;
    var client;

    var invalidateOnChange = [false, true];
    var ttls = [0, 1];
    var evictionPolicy = [EvictionPolicy.LFU, EvictionPolicy.LRU, EvictionPolicy.RANDOM, EvictionPolicy.NONE];

    var testConfigs = [];

    evictionPolicy.forEach(function (evictionPolicy) {
        invalidateOnChange.forEach(function (ioc) {
            ttls.forEach(function (ttl) {
                var ncc = new Config.NearCacheConfig();
                ncc.invalidateOnChange = ioc;
                ncc.timeToLiveSeconds = ttl;
                ncc.evictionMaxSize = 100;
                ncc.evictionPolicy = evictionPolicy;
                testConfigs.push(ncc);
            });
        });
    });

    function ds(str) {
        return {
            val: str,
            hashCode: function () {
                return str[0] - 'a';
            },
            equals(other) {
                return this.val === other.val;
            }
        }
    }

    before(function () {
        return Controller.createCluster(null, null).then(function (res) {
            cluster = res;
            return Controller.startMember(cluster.id);
        }).then(function () {
            return HazelcastClient.newHazelcastClient();
        }).then(function (cl) {
            client = cl;
        });
    });

    after(function () {
        client.shutdown();
        return Controller.shutdownCluster(cluster.id);
    });

    describe('CacheRecord', function () {

        it('does not expire if ttl is 0', function (done) {
            var rec = new DataRecord(ds('key'), 'value', undefined, 0);
            setTimeout(function () {
                if (rec.isExpired()) {
                    done(new Error('Unlimited ttl record expired'));
                } else {
                    done();
                }
            }, 1000);
        });

        it('expires after ttl', function (done) {
            var rec = new DataRecord(ds('key'), 'value', undefined, 1);
            setTimeout(function () {
                if (rec.isExpired()) {
                    done();
                } else {
                    done(new Error('Record did not expire after ttl'));
                }
            }, 1100);
        });

        it('does not expire before ttl', function (done) {
            var rec = new DataRecord(ds('key'), 'value', undefined, 10);
            setTimeout(function () {
                if (rec.isExpired()) {
                    done(new Error('Record expired before ttl'));
                } else {
                    done();
                }
            }, 100);
        });

        it('expires after maxIdleSeconds', function (done) {
            this.timeout(4000);
            var rec = new DataRecord(ds('key'), 'value', undefined, 100);
            setTimeout(function () {
                if (rec.isExpired(1)) {
                    done();
                } else {
                    done(new Error('did not expire after maxIdleSeconds'));
                }
            }, 2000);
        });

        it('does not expire while active', function (done) {
            var rec = new DataRecord(ds('key'), 'value', undefined, 100);
            setTimeout(function () {
                rec.setAccessTime();
                if (rec.isExpired(1)) {
                    done(new Error('expired'));
                } else {
                    done();
                }
            }, 100);
        });
    });

    testConfigs.forEach(function (testConfig) {

        describe(testConfig.toString(), function () {

            var nearCache;

            beforeEach(function () {
                nearCache = new NearCacheImpl(testConfig, createSerializationService());
                nearCache.setReady();
            });


            it('simple put/get', function () {
                nearCache.put(ds('key'), 'val');
                return nearCache.get(ds('key')).then((res) => {
                    return expect(res).to.equal('val');
                });
            });


            it('returns undefined for non existing value', function () {
                return nearCache.get(ds('random')).then(() => {
                    return expect(nearCache.getStatistics().missCount).to.equal(1);
                });
            });

            it('record does not expire if ttl is 0', function () {
                if (nearCache.timeToLiveSeconds != 0) {
                    this.skip();
                }
                nearCache.put(ds('key'), 'val');
                return expect(promiseAfter(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.equal('val');
            });

            it('ttl expire', function () {
                if (nearCache.timeToLiveSeconds == 0) {
                    this.skip();
                }
                nearCache.put(ds('key'), 'val');
                return expect(promiseAfter(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.be.undefined;
            });

            it('ttl does not expire early', function () {
                nearCache.put(ds('key'), 'val');
                return expect(promiseBefore(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.equal('val');
            });

            it('evicted after maxIdleSeconds', function () {
                if (nearCache.maxIdleSeconds == 0) {
                    this.skip();
                }
                nearCache.put(ds('key'), 'val');
                return expect(promiseAfter(testConfig.maxIdleSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.be.undefined;
            });

            it('not evicted after maxIdleSeconds if maxIdleSeconds is 0(unlimited)', function () {
                if (nearCache.maxIdleSeconds != 0) {
                    this.skip();
                }
                nearCache.put(ds('key'), 'val');
                return expect(promiseAfter(testConfig.maxIdleSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.equal('val');
            });

            it('not evicted before maxIdleSeconds', function () {
                nearCache.put(ds('key'), 'val');
                return expect(promiseBefore(testConfig.maxIdleSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.equal('val');
            });

            it('evicts entries after eviction max size is reached', function () {
                if (nearCache.evictionPolicy == EvictionPolicy.NONE) {
                    this.skip();
                }
                var i;
                for (i = 0; i < nearCache.evictionMaxSize + 1; i++) {
                    nearCache.put(ds('k' + i), 'v' + i);
                }
                expect(nearCache.getStatistics().evictedCount).to.equal(1);
                expect(nearCache.getStatistics().entryCount).to.equal(100);
            });

            it('no need for eviction when some entries are already expired', function (done) {
                if (nearCache.evictionPolicy === EvictionPolicy.NONE || nearCache.timeToLiveSeconds === 0) {
                    this.skip();
                }
                var i;
                for (i = 0; i < nearCache.evictionMaxSize; i++) {
                    nearCache.put(ds('k' + i), 'v' + i);
                }
                promiseAfter(nearCache.timeToLiveSeconds, function () {
                    try {
                        nearCache.put(ds('laterentry'), 'laterval');
                        expect(nearCache.getStatistics().evictedCount).to.equal(0);
                        expect(nearCache.getStatistics().expiredCount).to.greaterThan(0);
                        expect(nearCache.getStatistics().entryCount).to.lessThan(100);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });

            it('ttl expires even with an intervening read inside the window', function () {
                if (nearCache.timeToLiveSeconds == 0) {
                    this.skip();
                }
                nearCache.put(ds('key'), 'val');
                return promiseBefore(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key'))).then(function (mid) {
                    expect(mid).to.equal('val');
                    return expect(promiseAfter(testConfig.timeToLiveSeconds, nearCache.get.bind(nearCache, ds('key')))).to.eventually.be.undefined;
                });
            });

            it('proactively reclaims a ttl-expired entry that is never read again', function (done) {
                if (nearCache.evictionPolicy !== EvictionPolicy.NONE || nearCache.timeToLiveSeconds === 0) {
                    this.skip();
                }
                // The per-cache background timer is gone — the TTL sweep is now owned by
                // NearCacheManager's single shared timer, which calls each cache's public
                // doExpiration() method. With no real manager/cluster here, we drive the
                // sweep directly: put the orphan, wait past the TTL window WITHOUT ever
                // reading it or issuing a second put, then invoke doExpiration() exactly
                // as the manager tick would. promiseAfter waits ttl*1500ms (1500ms for
                // ttl=1), which exceeds the 1000ms TTL so the record is expired by then.
                nearCache.put(ds('orphan'), 'orphanval');
                promiseAfter(nearCache.timeToLiveSeconds, function () {
                    try {
                        // Drive the manager-owned TTL-only sweep directly (under NONE
                        // eviction, nothing else would reclaim the never-read orphan).
                        nearCache.doExpiration();
                        expect(nearCache.getStatistics().expiredCount).to.greaterThan(0);
                        expect(nearCache.getStatistics().entryCount).to.equal(0);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });
        });
    });

    describe('InMemory format', function () {

        it('Object', function () {
            var nearCacheConfig = new Config.NearCacheConfig();
            nearCacheConfig.inMemoryFormat = Config.InMemoryFormat.OBJECT;
            var nearCache = new NearCacheImpl(nearCacheConfig, createSerializationService());
            nearCache.put(ds('k'), 'v');
            expect(nearCache.internalStore.get(ds('k')).value).to.be.a('string');
        });

        it('Binary', function () {
            var nearCacheConfig = new Config.NearCacheConfig();
            nearCacheConfig.inMemoryFormat = Config.InMemoryFormat.BINARY;
            var nearCache = new NearCacheImpl(nearCacheConfig, createSerializationService());
            nearCache.put(ds('k'), 'v');
            expect(nearCache.internalStore.get(ds('k')).value).to.not.be.a('string');
        });
    });

    function createSerializationService() {
        var cfg = new Config.ClientConfig().serializationConfig;
        return new SerializationService(undefined, cfg);
    }

    function promiseBefore(boundaryInSec, func) {
        return promiseLater(boundaryInSec * 250, func);
    }

    function promiseAfter(boundaryInSec, func) {
        return promiseLater(boundaryInSec * 1500, func);
    }

});

// This is a pure in-process test: it constructs a NearCacheImpl directly and
// never touches a real cluster, so it lives in its own top-level describe with
// NO cluster-starting before/after hooks (avoiding needless startup cost).
describe('NearCacheImpl max-idle reset-on-read', function () {

    function ds(str) {
        return {
            val: str,
            hashCode: function () {
                return str[0] - 'a';
            },
            equals(other) {
                return this.val === other.val;
            }
        }
    }

    function createSerializationService() {
        var cfg = new Config.ClientConfig().serializationConfig;
        return new SerializationService(undefined, cfg);
    }

    function promiseBefore(boundaryInSec, func) {
        return promiseLater(boundaryInSec * 250, func);
    }

    it('max-idle still resets on read', function () {
        var ncc = new Config.NearCacheConfig();
        ncc.maxIdleSeconds = 1;
        ncc.evictionMaxSize = 100;
        ncc.evictionPolicy = EvictionPolicy.NONE;
        var nearCache = new NearCacheImpl(ncc, createSerializationService());
        nearCache.setReady();
        nearCache.put(ds('key'), 'val');
        // Each promiseBefore read (~250ms) falls inside the 1s idle window and
        // resets the idle timer. After 4 reads (~1000ms+) the cumulative elapsed
        // time exceeds the ORIGINAL maxIdle window, yet the entry is still present
        // because every read kept it alive — confirming reset-on-read.
        function readKey() {
            return promiseBefore(ncc.maxIdleSeconds, nearCache.get.bind(nearCache, ds('key')));
        }
        return readKey().then(readKey).then(readKey).then(readKey).then(function (res) {
            expect(res).to.equal('val');
        });
    });
});
