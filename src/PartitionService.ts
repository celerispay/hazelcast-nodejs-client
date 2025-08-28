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

import * as Promise from 'bluebird';
import HazelcastClient from './HazelcastClient';
import {ILogger} from './logging/ILogger';
import Address = require('./Address');
import ClientMessage = require('./ClientMessage');
import GetPartitionsCodec = require('./codec/GetPartitionsCodec');

const PARTITION_REFRESH_INTERVAL = 10000;

export class PartitionService {

    private client: HazelcastClient;
    private partitionMap: { [partitionId: number]: Address } = {};
    private partitionCount: number;
    private partitionRefreshTask: any;
    private isShutdown: boolean;
    private logger: ILogger;
    private lastRefreshTime: number = 0;
    private readonly minRefreshInterval: number = 2000; // Minimum 2 seconds between refreshes
    private refreshInProgress: boolean = false;
    private readonly maxRefreshRetries: number = 3;
    private refreshRetryCount: number = 0;

    constructor(client: HazelcastClient) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger();
        this.isShutdown = false;
    }

    initialize(): Promise<void> {
        this.partitionRefreshTask = setInterval(this.refresh.bind(this), PARTITION_REFRESH_INTERVAL);
        return this.refresh();
    }

    shutdown(): void {
        clearInterval(this.partitionRefreshTask);
        this.isShutdown = true;
        this.refreshInProgress = false;
    }

    /**
     * Clears the partition table, forcing a refresh on next operation
     */
    clearPartitionTable(): void {
        this.logger.info('PartitionService', 'Clearing partition table');
        this.partitionMap = {};
        this.partitionCount = 0;
        this.lastRefreshTime = 0;
        this.refreshRetryCount = 0;
    }

    /**
     * Refreshes the internal partition table.
     */
    refresh(): Promise<void> {
        if (this.isShutdown) {
            return Promise.resolve();
        }

        const now = Date.now();
        if (now - this.lastRefreshTime < this.minRefreshInterval) {
            this.logger.debug('PartitionService', 'Skipping refresh, too soon since last refresh');
            return Promise.resolve();
        }

        if (this.refreshInProgress) {
            this.logger.debug('PartitionService', 'Refresh already in progress, skipping');
            return Promise.resolve();
        }

        const ownerConnection = this.client.getClusterService().getOwnerConnection();
        if (ownerConnection == null) {
            this.logger.warn('PartitionService', 'Cannot refresh partitions, no owner connection available');
            // Return a rejected promise instead of resolved to indicate failure
            return Promise.reject(new Error('No owner connection available for partition refresh'));
        }

        this.refreshInProgress = true;
        const clientMessage: ClientMessage = GetPartitionsCodec.encodeRequest();

        // Add timeout to prevent hanging
        const refreshPromise = this.client.getInvocationService()
            .invokeOnConnection(ownerConnection, clientMessage)
            .then((response: ClientMessage) => {
                const receivedPartitionMap = GetPartitionsCodec.decodeResponse(response);
                this.partitionMap = receivedPartitionMap;
                this.partitionCount = Object.keys(this.partitionMap).length;
                this.lastRefreshTime = now;
                this.refreshRetryCount = 0; // Reset retry count on success
                this.logger.debug('PartitionService', `Refreshed partition table with ${this.partitionCount} partitions`);
            }).catch((e) => {
                if (this.client.getLifecycleService().isRunning()) {
                    this.logger.warn('PartitionService', 'Error while fetching cluster partition table from '
                        + this.client.getClusterService().ownerUuid, e);
                    
                    // Increment retry count and clear table if too many failures
                    this.refreshRetryCount++;
                    if (this.refreshRetryCount >= this.maxRefreshRetries) {
                        this.logger.error('PartitionService', `Max refresh retries (${this.maxRefreshRetries}) exceeded, clearing partition table`);
                        this.clearPartitionTable();
                    }
                }
                throw e; // Re-throw the error to propagate it
            }).finally(() => {
                this.refreshInProgress = false;
            });

        // Add timeout to prevent hanging
        const timeoutPromise = new Promise<void>((_, reject) => {
            setTimeout(() => {
                reject(new Error('Partition refresh timed out'));
            }, 10000); // 10 second timeout
        });

        return Promise.race([refreshPromise, timeoutPromise]);
    }

    /**
     * Returns the {@link Address} of the node which owns given partition id.
     * @param partitionId
     * @returns the address of the node.
     */
    getAddressForPartition(partitionId: number): Address {
        const address = this.partitionMap[partitionId];
        if (!address) {
            this.logger.warn('PartitionService', `No address found for partition ${partitionId}, refreshing partition table`);
            // Trigger a refresh if we don't have partition information
            setImmediate(() => this.refresh());
        }
        return address;
    }

    /**
     * Computes the partition id for a given key.
     * @param key
     * @returns the partition id.
     */
    getPartitionId(key: any): number {
        try {
            let partitionHash: number;
            if (typeof key === 'object' && 'getPartitionHash' in key) {
                partitionHash = key.getPartitionHash();
            } else {
                partitionHash = this.client.getSerializationService().toData(key).getPartitionHash();
            }
            return Math.abs(partitionHash) % this.partitionCount;
        } catch (error) {
            this.logger.warn('PartitionService', `Error computing partition ID for key:`, error);
            // Return a safe default partition ID
            return 0;
        }
    }

    getPartitionCount(): number {
        return this.partitionCount;
    }

    /**
     * Checks if the partition service is healthy
     * @returns true if partition table is populated, false otherwise
     */
    isHealthy(): boolean {
        return this.partitionCount > 0 && Object.keys(this.partitionMap).length > 0;
    }

    /**
     * Gets information about the current partition table state
     * @returns object with partition information
     */
    getPartitionTableInfo(): { partitionCount: number, isHealthy: boolean, lastRefreshTime: number } {
        return {
            partitionCount: this.partitionCount,
            isHealthy: this.isHealthy(),
            lastRefreshTime: this.lastRefreshTime
        };
    }
}
