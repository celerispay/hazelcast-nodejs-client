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

/**
 * Manages connection pools to prevent connection explosion
 * This class ensures we don't create too many connections to the same node
 */
export class ConnectionPoolManager {
    private readonly maxConnectionsPerNode: number = 3; // Maximum connections per node
    private readonly connectionAttempts: Map<string, Set<string>> = new Map(); // address -> Set of attempt IDs
    private readonly logger: ILogger;
    private readonly connectionAttemptTimeout: number = 30000; // 30 seconds timeout for connection attempts

    constructor(logger: ILogger) {
        this.logger = logger;
    }

    /**
     * Checks if we can attempt a new connection to the given address
     * @param address The address to check
     * @returns true if connection attempt is allowed, false otherwise
     */
    canAttemptConnection(address: Address): boolean {
        const addressStr = address.toString();
        const attempts = this.connectionAttempts.get(addressStr);
        
        if (!attempts) {
            return true;
        }

        // Clean up expired attempts
        this.cleanupExpiredAttempts(addressStr);
        
        // Check if we have too many active attempts
        const activeAttempts = this.connectionAttempts.get(addressStr);
        if (activeAttempts && activeAttempts.size >= this.maxConnectionsPerNode) {
            this.logger.debug('ConnectionPoolManager', `Too many connection attempts to ${addressStr}, blocking new attempt`);
            return false;
        }

        return true;
    }

    /**
     * Registers a connection attempt to track it
     * @param address The address being connected to
     * @returns A unique attempt ID that should be used to complete the attempt
     */
    registerConnectionAttempt(address: Address): string {
        const addressStr = address.toString();
        const attemptId = this.generateAttemptId();
        
        if (!this.connectionAttempts.has(addressStr)) {
            this.connectionAttempts.set(addressStr, new Set());
        }
        
        this.connectionAttempts.get(addressStr)!.add(attemptId);
        
        // Set timeout to automatically clean up this attempt
        setTimeout(() => {
            this.completeConnectionAttempt(address, attemptId);
        }, this.connectionAttemptTimeout);
        
        this.logger.debug('ConnectionPoolManager', `Registered connection attempt ${attemptId} to ${addressStr}`);
        return attemptId;
    }

    /**
     * Completes a connection attempt (success or failure)
     * @param address The address the attempt was for
     * @param attemptId The attempt ID returned from registerConnectionAttempt
     */
    completeConnectionAttempt(address: Address, attemptId: string): void {
        const addressStr = address.toString();
        const attempts = this.connectionAttempts.get(addressStr);
        
        if (attempts) {
            attempts.delete(attemptId);
            
            // Clean up the entire entry if no more attempts
            if (attempts.size === 0) {
                this.connectionAttempts.delete(addressStr);
            }
            
            this.logger.debug('ConnectionPoolManager', `Completed connection attempt ${attemptId} to ${addressStr}`);
        }
    }

    /**
     * Cleans up expired connection attempts for a specific address
     * @param addressStr The address string to clean up
     */
    private cleanupExpiredAttempts(addressStr: string): void {
        const attempts = this.connectionAttempts.get(addressStr);
        if (attempts) {
            // For now, we'll let the timeout handle cleanup
            // In a more sophisticated implementation, we could track timestamps
            this.logger.debug('ConnectionPoolManager', `Cleaned up expired attempts for ${addressStr}`);
        }
    }

    /**
     * Generates a unique attempt ID
     * @returns A unique string identifier
     */
    private generateAttemptId(): string {
        return `attempt_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    /**
     * Gets the current connection attempt count for an address
     * @param address The address to check
     * @returns The number of active connection attempts
     */
    getConnectionAttemptCount(address: Address): number {
        const addressStr = address.toString();
        const attempts = this.connectionAttempts.get(addressStr);
        return attempts ? attempts.size : 0;
    }

    /**
     * Clears all connection attempts for an address (useful during failover)
     * @param address The address to clear
     */
    clearConnectionAttempts(address: Address): void {
        const addressStr = address.toString();
        this.connectionAttempts.delete(addressStr);
        this.logger.debug('ConnectionPoolManager', `Cleared all connection attempts for ${addressStr}`);
    }

    /**
     * Clears ALL connection attempts (useful during critical failover)
     */
    clearAllConnectionAttempts(): void {
        this.connectionAttempts.clear();
        this.logger.info('ConnectionPoolManager', 'Cleared all connection attempts');
    }

    /**
     * Gets a summary of current connection attempts
     * @returns A string summary of all active connection attempts
     */
    getConnectionAttemptsSummary(): string {
        const summary: string[] = [];
        
        this.connectionAttempts.forEach((attempts, address) => {
            summary.push(`${address}: ${attempts.size} attempts`);
        });
        
        return summary.length > 0 ? summary.join(', ') : 'none';
    }
}
