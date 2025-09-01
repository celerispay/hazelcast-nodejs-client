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

import {ILogger} from '../logging/ILogger';
import Address = require('../Address');
import * as net from 'net';

/**
 * Node readiness status
 */
export enum NodeReadinessStatus {
    UNKNOWN = 'unknown',
    STARTING = 'starting',
    READY = 'ready',
    NOT_READY = 'not_ready'
}

/**
 * Node readiness check result
 */
export interface NodeReadinessResult {
    status: NodeReadinessStatus;
    details: string;
    lastCheck: number;
}

/**
 * Detects if a Hazelcast node is ready to accept authenticated connections
 * This prevents connection attempts to nodes that are still starting up
 */
export class NodeReadinessDetector {
    private readonly logger: ILogger;
    private readonly readinessCache: Map<string, NodeReadinessResult>;
    private readonly cacheTimeout: number = 30000; // 30 seconds cache timeout
    private readonly readinessCheckTimeout: number = 5000; // 5 seconds for readiness check
    private readonly maxConsecutiveFailures: number = 3; // Max consecutive failures before marking as not ready

    constructor(logger: ILogger) {
        this.logger = logger;
        this.readinessCache = new Map();
    }

    /**
     * Checks if a node is ready to accept connections
     * @param address The address to check
     * @returns Promise that resolves to the readiness status
     */
    async checkNodeReadiness(address: Address): Promise<NodeReadinessStatus> {
        const addressStr = address.toString();
        
        // Check cache first
        const cached = this.readinessCache.get(addressStr);
        if (cached && (Date.now() - cached.lastCheck) < this.cacheTimeout) {
            this.logger.debug('NodeReadinessDetector', 
                `Using cached readiness status for ${addressStr}: ${cached.status}`);
            return cached.status;
        }

        try {
            // Perform actual readiness check
            const result = await this.performReadinessCheck(address);
            
            // Cache the result
            this.readinessCache.set(addressStr, {
                status: result.status,
                details: result.details,
                lastCheck: Date.now()
            });

            this.logger.debug('NodeReadinessDetector', 
                `Node ${addressStr} readiness: ${result.status} - ${result.details}`);
            
            return result.status;
        } catch (error) {
            this.logger.warn('NodeReadinessDetector', 
                `Error checking readiness for ${addressStr}:`, error);
            
            // On error, assume not ready
            return NodeReadinessStatus.NOT_READY;
        }
    }

    /**
     * Performs the actual readiness check
     * @param address The address to check
     * @returns Promise that resolves to the readiness result
     */
    private async performReadinessCheck(address: Address): Promise<NodeReadinessResult> {
        return new Promise<NodeReadinessResult>((resolve) => {
            const socket = new net.Socket();
            let isResolved = false;

            const timeout = setTimeout(() => {
                if (!isResolved) {
                    isResolved = true;
                    socket.destroy();
                    resolve({
                        status: NodeReadinessStatus.NOT_READY,
                        details: 'Connection timeout during readiness check',
                        lastCheck: Date.now()
                    });
                }
            }, this.readinessCheckTimeout);

            socket.on('connect', () => {
                if (!isResolved) {
                    isResolved = true;
                    clearTimeout(timeout);
                    socket.destroy();
                    
                    // Just being able to connect doesn't mean the node is ready
                    // We need to check if it can handle the Hazelcast protocol
                    resolve({
                        status: NodeReadinessStatus.STARTING,
                        details: 'Node accepts connections but may not be fully ready',
                        lastCheck: Date.now()
                    });
                }
            });

            socket.on('error', (error) => {
                if (!isResolved) {
                    isResolved = true;
                    clearTimeout(timeout);
                    socket.destroy();
                    
                    // Connection refused usually means node is not ready
                    if (error.message.includes('ECONNREFUSED')) {
                        resolve({
                            status: NodeReadinessStatus.NOT_READY,
                            details: 'Connection refused - node not ready',
                            lastCheck: Date.now()
                        });
                    } else {
                        resolve({
                            status: NodeReadinessStatus.UNKNOWN,
                            details: `Connection error: ${error.message}`,
                            lastCheck: Date.now()
                        });
                    }
                }
            });

            socket.on('timeout', () => {
                if (!isResolved) {
                    isResolved = true;
                    clearTimeout(timeout);
                    socket.destroy();
                    resolve({
                        status: NodeReadinessStatus.NOT_READY,
                        details: 'Connection timeout - node not responding',
                        lastCheck: Date.now()
                    });
                }
            });

            // Attempt to connect
            socket.connect(address.port, address.host);
        });
    }

    /**
     * Records a connection failure for a node
     * This helps track if a node is consistently failing
     * @param address The address that failed
     * @param error The error that occurred
     */
    recordConnectionFailure(address: Address, error: Error): void {
        const addressStr = address.toString();
        const cached = this.readinessCache.get(addressStr);
        
        if (cached) {
            // If we've had multiple consecutive failures, mark as not ready
            if (cached.status === NodeReadinessStatus.STARTING) {
                // For now, we'll use a simple approach
                // In a more sophisticated implementation, we could track failure counts
                this.logger.debug('NodeReadinessDetector', 
                    `Recording connection failure for ${addressStr}, may mark as not ready`);
            }
        }
    }

    /**
     * Records a successful connection to a node
     * This helps mark nodes as ready
     * @param address The address that succeeded
     */
    recordConnectionSuccess(address: Address): void {
        const addressStr = address.toString();
        
        this.readinessCache.set(addressStr, {
            status: NodeReadinessStatus.READY,
            details: 'Connection successful - node is ready',
            lastCheck: Date.now()
        });
        
        this.logger.debug('NodeReadinessDetector', 
            `Marked node ${addressStr} as ready after successful connection`);
    }

    /**
     * Clears readiness cache for an address (useful during failover)
     * @param address The address to clear
     */
    clearReadinessCache(address: Address): void {
        const addressStr = address.toString();
        this.readinessCache.delete(addressStr);
        this.logger.debug('NodeReadinessDetector', `Cleared readiness cache for ${addressStr}`);
    }

    /**
     * Clears ALL readiness cache (useful during critical failover)
     */
    clearAllReadinessCache(): void {
        this.readinessCache.clear();
        this.logger.info('NodeReadinessDetector', 'Cleared all readiness cache');
    }

    /**
     * Forces a node to be marked as not ready
     * Useful when we know a node is having issues
     * @param address The address to mark
     * @param reason The reason for marking as not ready
     */
    forceNotReady(address: Address, reason: string): void {
        const addressStr = address.toString();
        
        this.readinessCache.set(addressStr, {
            status: NodeReadinessStatus.NOT_READY,
            details: `Forced not ready: ${reason}`,
            lastCheck: Date.now()
        });
        
        this.logger.info('NodeReadinessDetector', 
            `Forced node ${addressStr} as not ready: ${reason}`);
    }

    /**
     * Gets a summary of current readiness status
     * @returns A string summary of readiness status
     */
    getReadinessSummary(): string {
        const summary: string[] = [];
        
        this.readinessCache.forEach((result, address) => {
            summary.push(`${address}: ${result.status} (${result.details})`);
        });
        
        return summary.length > 0 ? summary.join('; ') : 'none';
    }

    /**
     * Checks if we should attempt a connection based on readiness status
     * @param address The address to check
     * @returns true if connection should be attempted, false otherwise
     */
    shouldAttemptConnection(address: Address): boolean {
        const status = this.readinessCache.get(address.toString());
        
        if (!status) {
            // No cached status, allow connection attempt
            return true;
        }
        
        // Don't attempt if we know the node is not ready
        if (status.status === NodeReadinessStatus.NOT_READY) {
            this.logger.debug('NodeReadinessDetector', 
                `Skipping connection attempt to ${address.toString()} - node not ready`);
            return false;
        }
        
        // Allow connection attempts for other statuses
        return true;
    }
}
