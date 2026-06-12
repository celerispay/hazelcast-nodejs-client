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

import * as Long from 'long';
import {EvictionPolicy} from '../config/EvictionPolicy';
import {InMemoryFormat} from '../config/InMemoryFormat';
import {NearCacheConfig} from '../config/NearCacheConfig';
import {DataKeyedHashMap} from '../DataStoreHashMap';
import {Data} from '../serialization/Data';
import {SerializationService} from '../serialization/SerializationService';
import {DeferredPromise, shuffleArray} from '../Util';
import * as AlwaysFreshStaleReadDetectorImpl from './AlwaysFreshStaleReadDetectorImpl';
import {DataRecord} from './DataRecord';
import {StaleReadDetector} from './StaleReadDetector';
import * as Promise from 'bluebird';

/**
 * One pending TTL expiration. Captures the record's key and the absolute
 * expirationTime it had when enqueued, so a later drain can detect a superseded
 * generation (overwrite) by comparing against the live record's expirationTime.
 */
interface ExpirationQueueEntry {
    key: Data;
    expirationTime: number;
}

/**
 * When the expiration queue's logical length (live entries behind the head
 * cursor) exceeds this multiple of internalStore.size, it is compacted to drop
 * stale elements (key gone, or generation superseded). Bounds queue growth under
 * heavy overwrite/delete churn without paying an O(n) search on every delete.
 */
const EXPIRATION_QUEUE_STALE_FACTOR = 2;

export interface NearCacheStatistics {
    creationTime: number;
    evictedCount: number;
    expiredCount: number;
    missCount: number;
    hitCount: number;
    entryCount: number;
}

export interface NearCache {
    put(key: Data, value: any): void;

    get(key: Data): Promise<Data | any>;

    getName(): string;

    invalidate(key: Data): void;

    clear(): void;

    getStatistics(): NearCacheStatistics;

    isInvalidatedOnChange(): boolean;

    setStaleReadDetector(detector: StaleReadDetector): void;

    tryReserveForUpdate(key: Data): Long;

    tryPublishReserved(key: Data, value: any, reservationId: Long): any;

    setReady(): void;

    destroy(): void;

    getTimeToLiveSeconds(): number;

    doExpiration(): void;
}

export class NearCacheImpl implements NearCache {

    internalStore: DataKeyedHashMap<DataRecord>;
    private serializationService: SerializationService;
    private name: string;
    private invalidateOnChange: boolean;
    private maxIdleSeconds: number;
    private inMemoryFormat: InMemoryFormat;
    private timeToLiveSeconds: number;
    private evictionPolicy: EvictionPolicy;
    private evictionMaxSize: number;
    private evictionSamplingCount: number;
    private evictionSamplingPoolSize: number;
    private evictionCandidatePool: DataRecord[];
    private staleReadDetector: StaleReadDetector = AlwaysFreshStaleReadDetectorImpl.INSTANCE;
    private reservationCounter: Long = Long.ZERO;

    private evictedCount: number = 0;
    private expiredCount: number = 0;
    private missCount: number = 0;
    private hitCount: number = 0;
    private creationTime = Date.now();
    private compareFunc: (x: DataRecord, y: DataRecord) => number;
    private ready: Promise.Resolver<void>;

    /**
     * Insertion-ordered FIFO of pending TTL expirations. Within one near cache
     * timeToLiveSeconds is constant and (post-T1) expirationTime is absolute and
     * never slides on read, so put-order == expiration-order: the head is always
     * the soonest to expire. doExpiration drains expired entries from the front
     * and stops at the first non-expired head — O(number expiring this tick),
     * never an O(n) full-store scan or sort.
     *
     * Implemented as a plain array plus a head cursor (expirationQueueHead): the
     * tail grows via push() (O(1) amortized) and the front is consumed by
     * advancing the cursor (O(1)) rather than Array.prototype.shift() (O(n)). The
     * consumed prefix is reclaimed by a slice() compaction once the cursor passes
     * the array midpoint (see compactExpirationQueueIfNeeded). Stale entries (from
     * delete/eviction/overwrite) are dropped lazily on drain; unbounded stale
     * growth under churn is bounded by EXPIRATION_QUEUE_STALE_FACTOR compaction.
     */
    private expirationQueue: ExpirationQueueEntry[] = [];
    private expirationQueueHead: number = 0;

    constructor(nearCacheConfig: NearCacheConfig, serializationService: SerializationService) {
        this.serializationService = serializationService;
        this.name = nearCacheConfig.name;
        this.invalidateOnChange = nearCacheConfig.invalidateOnChange;
        this.maxIdleSeconds = nearCacheConfig.maxIdleSeconds;
        this.inMemoryFormat = nearCacheConfig.inMemoryFormat;
        this.timeToLiveSeconds = nearCacheConfig.timeToLiveSeconds;
        this.evictionPolicy = nearCacheConfig.evictionPolicy;
        this.evictionMaxSize = nearCacheConfig.evictionMaxSize;
        this.evictionSamplingCount = nearCacheConfig.evictionSamplingCount;
        this.evictionSamplingPoolSize = nearCacheConfig.evictionSamplingPoolSize;
        if (this.evictionPolicy === EvictionPolicy.LFU) {
            this.compareFunc = DataRecord.lfuComp;
        } else if (this.evictionPolicy === EvictionPolicy.LRU) {
            this.compareFunc = DataRecord.lruComp;
        } else if (this.evictionPolicy === EvictionPolicy.RANDOM) {
            this.compareFunc = DataRecord.randomComp;
        } else {
            this.compareFunc = undefined;
        }

        this.evictionCandidatePool = [];
        this.internalStore = new DataKeyedHashMap<DataRecord>();
        this.ready = DeferredPromise();
    }

    getTimeToLiveSeconds(): number {
        return this.timeToLiveSeconds;
    }

    setReady(): void {
        this.ready.resolve();
    }

    getName(): string {
        return this.name;
    }

    nextReservationId(): Long {
        const res = this.reservationCounter;
        this.reservationCounter = this.reservationCounter.add(1);
        return res;
    }

    tryReserveForUpdate(key: Data): Long {
        const internalRecord = this.internalStore.get(key);
        const resId = this.nextReservationId();
        if (internalRecord === undefined) {
            this.doEvictionIfRequired();
            const dr = new DataRecord(key, undefined, undefined, this.timeToLiveSeconds);
            dr.casStatus(DataRecord.READ_PERMITTED, resId);
            this.internalStore.set(key, dr);
            this.enqueueExpiration(dr);
            return resId;
        }
        if (internalRecord.casStatus(DataRecord.READ_PERMITTED, resId)) {
            return resId;
        }
        return DataRecord.NOT_RESERVED;
    }

    tryPublishReserved(key: Data, value: any, reservationId: Long): any {
        const internalRecord = this.internalStore.get(key);
        if (internalRecord && internalRecord.casStatus(reservationId, DataRecord.READ_PERMITTED)) {
            if (this.inMemoryFormat === InMemoryFormat.OBJECT) {
                internalRecord.value = this.serializationService.toObject(value);
            } else {
                internalRecord.value = this.serializationService.toData(value);
            }
            internalRecord.setCreationTime();
            this.initInvalidationMetadata(internalRecord);
        } else if (internalRecord === undefined) {
            return undefined;
        } else {
            if (this.inMemoryFormat === InMemoryFormat.BINARY) {
                return this.serializationService.toObject(internalRecord.value);
            } else {
                return internalRecord.value;
            }
        }
    }

    setStaleReadDetector(staleReadDetector: StaleReadDetector): void {
        this.staleReadDetector = staleReadDetector;
    }

    /**
     * Creates a new {DataRecord} for given key and value. Then, puts the record in near cache.
     * If the number of records in near cache exceeds {evictionMaxSize}, it removes expired items first.
     * If there is no expired item, it triggers an invalidation process to create free space.
     * @param key
     * @param value
     */
    put(key: Data, value: any): void {
        this.doEvictionIfRequired();
        if (this.inMemoryFormat === InMemoryFormat.OBJECT) {
            value = this.serializationService.toObject(value);
        } else {
            value = this.serializationService.toData(value);
        }
        const dr = new DataRecord(key, value, undefined, this.timeToLiveSeconds);
        this.initInvalidationMetadata(dr);
        this.internalStore.set(key, dr);
        this.enqueueExpiration(dr);
    }

    /**
     *
     * @param key
     * @returns the value if present in near cache, 'undefined' if not
     */
    get(key: Data): Promise<Data | any> {
        return this.ready.promise.then(() => {
            const dr = this.internalStore.get(key);
            if (dr === undefined) {
                this.missCount++;
                return undefined;
            }
            if (this.staleReadDetector.isStaleRead(key, dr)) {
                this.internalStore.delete(key);
                this.missCount++;
                return undefined;
            }
            if (dr.isExpired(this.maxIdleSeconds)) {
                this.expireRecord(key);
                this.missCount++;
                return undefined;
            }
            dr.setAccessTime();
            dr.hitRecord();
            this.hitCount++;
            if (this.inMemoryFormat === InMemoryFormat.BINARY) {
                return this.serializationService.toObject(dr.value);
            } else {
                return dr.value;
            }
        });
    }

    invalidate(key: Data): void {
        this.internalStore.delete(key);
    }

    clear(): void {
        this.internalStore.clear();
        this.resetExpirationQueue();
    }

    /**
     * Tears down the near cache by clearing the store (and the expiration queue,
     * so it does not survive teardown). The TTL sweep is owned by
     * NearCacheManager's shared timer, not by this instance, so there is no
     * per-cache timer to clear here — a missed proxy-destroy degrades to benign
     * dormant retention rather than a live leaked timer.
     */
    destroy(): void {
        this.internalStore.clear();
        this.resetExpirationQueue();
    }

    isInvalidatedOnChange(): boolean {
        return this.invalidateOnChange;
    }

    getStatistics(): NearCacheStatistics {
        const stats: NearCacheStatistics = {
            creationTime: this.creationTime,
            evictedCount: this.evictedCount,
            expiredCount: this.expiredCount,
            missCount: this.missCount,
            hitCount: this.hitCount,
            entryCount: this.internalStore.size,
        };
        return stats;
    }

    /**
     * Proactively reclaims TTL-expired records regardless of the eviction policy.
     * Driven by NearCacheManager's single shared background timer (the manager
     * skips caches whose timeToLiveSeconds is 0) — fully decoupled from the
     * put/get path so writes never pay for any scan.
     *
     * Drains expired entries from the FRONT of the insertion-ordered expiration
     * queue and stops at the first non-expired head (everything behind it was put
     * later and, with constant ttl + absolute expirationTime, expires later). Cost
     * is O(number of entries expiring this tick), not O(store size). Only the
     * absolute TTL window is considered (isExpired(0) disables the max-idle
     * branch); max-idle eviction stays lazy on the read path. Removal goes through
     * expireRecord() so expiredCount accounting stays consistent.
     *
     * Stale queue elements (key deleted/evicted, or a newer generation re-put with
     * a later expirationTime) are dropped lazily here: a head whose key is gone, or
     * whose live record no longer carries the queued expirationTime, is popped and
     * skipped without touching the store.
     */
    doExpiration(): void {
        const now = Date.now();
        while (this.expirationQueueHead < this.expirationQueue.length) {
            const head = this.expirationQueue[this.expirationQueueHead];
            if (head.expirationTime > now) {
                break;
            }
            this.expirationQueueHead++;
            const cur = this.internalStore.get(head.key);
            if (cur === undefined) {
                continue;
            }
            if (cur.getExpirationTime() !== head.expirationTime) {
                continue;
            }
            if (cur.isExpired(0)) {
                this.expireRecord(head.key);
            }
        }
        this.compactExpirationQueueIfNeeded();
    }

    /**
     * Appends a pending TTL expiration for a record with a finite expirationTime
     * (ttl > 0). ttl=0 (unlimited) records are never enqueued and so are never
     * swept. Re-put of an existing key appends a new (later) element; the prior
     * element becomes stale and is dropped lazily on drain.
     */
    private enqueueExpiration(dr: DataRecord): void {
        const expirationTime = dr.getExpirationTime();
        if (expirationTime > 0) {
            this.expirationQueue.push({key: dr.key, expirationTime});
        }
    }

    private resetExpirationQueue(): void {
        this.expirationQueue = [];
        this.expirationQueueHead = 0;
    }

    /**
     * Reclaims the consumed prefix and bounds stale growth. Two cheap triggers:
     * (1) once the head cursor passes the array midpoint, slice off the consumed
     * prefix so the backing array does not grow without bound; (2) when the number
     * of live (un-drained) entries exceeds EXPIRATION_QUEUE_STALE_FACTOR * store
     * size, rebuild the queue keeping only elements whose key still maps to a
     * record carrying the same expirationTime — dropping accumulated stale churn.
     */
    private compactExpirationQueueIfNeeded(): void {
        const liveLength = this.expirationQueue.length - this.expirationQueueHead;
        const overStale = liveLength > EXPIRATION_QUEUE_STALE_FACTOR * this.internalStore.size;
        if (overStale) {
            // Keep at most ONE element per live record: an element survives only if
            // its key still maps to a record carrying the same expirationTime AND no
            // earlier element for that key was already kept. This collapses duplicate
            // same-key/same-ms generations (rapid overwrite churn) so the retained
            // queue is bounded by the number of distinct live records, not by churn.
            const rebuilt: ExpirationQueueEntry[] = [];
            const seenKeys = new DataKeyedHashMap<boolean>();
            for (let i = this.expirationQueueHead; i < this.expirationQueue.length; i++) {
                const entry = this.expirationQueue[i];
                if (seenKeys.get(entry.key) !== undefined) {
                    continue;
                }
                const cur = this.internalStore.get(entry.key);
                if (cur !== undefined && cur.getExpirationTime() === entry.expirationTime) {
                    rebuilt.push(entry);
                    seenKeys.set(entry.key, true);
                }
            }
            this.expirationQueue = rebuilt;
            this.expirationQueueHead = 0;
        } else if (this.expirationQueueHead > this.expirationQueue.length / 2) {
            this.expirationQueue = this.expirationQueue.slice(this.expirationQueueHead);
            this.expirationQueueHead = 0;
        }
    }

    protected isEvictionRequired(): boolean {
        return this.evictionPolicy !== EvictionPolicy.NONE && this.evictionMaxSize <= this.internalStore.size;
    }

    protected doEvictionIfRequired(): void {
        if (!this.isEvictionRequired()) {
            return;
        }
        const internalSize = this.internalStore.size;
        if (this.recomputeEvictionPool() > 0) {
            return;
        } else {
            this.evictRecord(this.evictionCandidatePool[0].key);
            this.evictionCandidatePool = this.evictionCandidatePool.slice(1);
        }
    }

    /**
     * @returns number of expired elements.
     */
    protected recomputeEvictionPool(): number {
        const arr: DataRecord[] = Array.from(this.internalStore.values());

        shuffleArray<DataRecord>(arr);
        const newCandidates = arr.slice(0, this.evictionSamplingCount);
        const cleanedNewCandidates = newCandidates.filter(this.filterExpiredRecord, this);
        const expiredCount = newCandidates.length - cleanedNewCandidates.length;
        if (expiredCount > 0) {
            return expiredCount;
        }

        this.evictionCandidatePool.push(...cleanedNewCandidates);

        this.evictionCandidatePool.sort(this.compareFunc);

        this.evictionCandidatePool = this.evictionCandidatePool.slice(0, this.evictionSamplingPoolSize);
        return 0;
    }

    protected filterExpiredRecord(candidate: DataRecord): boolean {
        if (candidate.isExpired(this.maxIdleSeconds)) {
            this.expireRecord(candidate.key);
            return false;
        } else {
            return true;
        }
    }

    protected expireRecord(key: any | Data): void {
        if (this.internalStore.delete(key)) {
            this.expiredCount++;
        }
    }

    protected evictRecord(key: any | Data): void {
        if (this.internalStore.delete(key)) {
            this.evictedCount++;
        }
    }

    private initInvalidationMetadata(dr: DataRecord): void {
        if (this.staleReadDetector === AlwaysFreshStaleReadDetectorImpl.INSTANCE) {
            return;
        }
        const partitionId = this.staleReadDetector.getPartitionId(dr.key);
        const metadataContainer = this.staleReadDetector.getMetadataContainer(partitionId);
        dr.setInvalidationSequence(metadataContainer.getSequence());
        dr.setUuid(metadataContainer.getUuid());
    }
}
