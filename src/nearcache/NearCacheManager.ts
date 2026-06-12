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

import {NearCache, NearCacheImpl} from './NearCache';
import {SerializationService} from '../serialization/SerializationService';
import HazelcastClient from '../HazelcastClient';

/**
 * Cadence, in milliseconds, of the shared proactive TTL sweep. A single timer
 * owned by the manager (not one per near cache) ticks at this interval and
 * sweeps every TTL-enabled near cache. The lazy get() path continues to expire
 * individual records on read regardless of this timer.
 */
const EXPIRATION_TASK_INTERVAL_MS = 1000;

export class NearCacheManager {

    protected readonly serializationService: SerializationService;
    private readonly caches: Map<string, NearCache> = new Map();
    private readonly client: HazelcastClient;
    private expirationTaskHandle: any;

    constructor(client: HazelcastClient) {
        this.client = client;
    }

    public getOrCreateNearCache(name: string): NearCache {
        let nearCache = this.caches.get(name);
        if (nearCache == null) {
            nearCache = new NearCacheImpl(this.client.getConfig().getNearCacheConfig(name),
                this.client.getSerializationService());

            this.caches.set(name, nearCache);
            if (this.expirationTaskHandle === undefined) {
                this.startExpirationTask();
            }
        }
        return nearCache;
    }

    public destroyNearCache(name: string): void {
        const nearCache = this.caches.get(name);
        if (nearCache != null) {
            this.caches.delete(name);
            nearCache.destroy();
        }
    }

    public destroyAllNearCaches(): void {
        for (const key of Array.from(this.caches.keys())) {
            this.destroyNearCache(key);
        }
        if (this.expirationTaskHandle != null) {
            clearInterval(this.expirationTaskHandle);
            this.expirationTaskHandle = undefined;
        }
    }

    public listAllNearCaches(): NearCache[] {
        return Array.from(this.caches.values());
    }

    /**
     * Starts the single shared TTL sweep timer. Mirrors RepairingTask: lazily
     * started on first near cache and cleared in destroyAllNearCaches (invoked by
     * HazelcastClient.shutdown). The handle is unref()'d so the timer never keeps
     * the Node process alive on its own.
     */
    private startExpirationTask(): void {
        this.expirationTaskHandle = setInterval(this.doExpiration.bind(this), EXPIRATION_TASK_INTERVAL_MS);
        if (typeof this.expirationTaskHandle.unref === 'function') {
            this.expirationTaskHandle.unref();
        }
    }

    /**
     * One tick of the shared sweep: TTL-only reclamation for every cache that has
     * a finite TTL. Caches with timeToLiveSeconds === 0 (the default no-TTL config)
     * are skipped entirely — they can never expire anything, so scanning them is
     * pure waste.
     */
    private doExpiration(): void {
        // Iterate over a materialized array (not the raw Map iterator): the project
        // compiles to ES5 without downlevelIteration, where `for...of` over a Map
        // iterator degrades to an indexed loop on a non-array (length === undefined)
        // and silently never runs. Array.from matches the safe pattern already used
        // by destroyAllNearCaches/listAllNearCaches.
        for (const nearCache of Array.from(this.caches.values())) {
            if (nearCache.getTimeToLiveSeconds() > 0) {
                nearCache.doExpiration();
            }
        }
    }

}
