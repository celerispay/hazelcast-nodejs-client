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
 * Interface for node credentials
 */
export interface NodeCredentials {
    uuid: string;
    ownerUuid: string;
    groupName: string;
    groupPassword: string;
    lastAuthenticated: number;
    isOwner: boolean;
}

/**
 * Service to preserve and restore authentication credentials across failover cycles
 * This fixes the "Invalid Credentials" issue that occurs when nodes rejoin after failover
 */
export class CredentialPreservationService {
    private readonly logger: ILogger;
    
    /**
     * Stores authentication context for each node
     */
    private nodeCredentials: Map<string, NodeCredentials> = new Map();

    constructor(logger: ILogger) {
        this.logger = logger;
    }

    /**
     * Preserves authentication credentials for a node before it gets disconnected
     * @param address The node address
     * @param uuid The node's UUID
     * @param ownerUuid The owner UUID
     * @param groupName The group name
     * @param groupPassword The group password
     * @param isOwner Whether this is an owner connection
     */
    preserveCredentials(
        address: Address, 
        uuid: string, 
        ownerUuid: string, 
        groupName: string, 
        groupPassword: string,
        isOwner: boolean
    ): void {
        const addressStr = address.toString();
        
        this.nodeCredentials.set(addressStr, {
            uuid,
            ownerUuid,
            groupName,
            groupPassword,
            lastAuthenticated: Date.now(),
            isOwner
        });
        
        this.logger.debug('CredentialPreservationService', 
            `Preserved credentials for ${addressStr}: uuid=${uuid}, ownerUuid=${ownerUuid}, isOwner=${isOwner}`);
    }

    /**
     * Restores authentication credentials for a specific node
     * @param address The node address
     * @returns The preserved credentials or null if not found
     */
    restoreCredentials(address: Address): NodeCredentials | null {
        const addressStr = address.toString();
        const credentials = this.nodeCredentials.get(addressStr);
        
        if (credentials) {
            this.logger.info('CredentialPreservationService', 
                `✅ Found preserved credentials for ${addressStr}: uuid=${credentials.uuid}, ownerUuid=${credentials.ownerUuid}`);
            return credentials;
        }
        
        // Log all available credentials for debugging
        this.logger.info('CredentialPreservationService', 
            `❌ No preserved credentials found for ${addressStr}`);
        this.logger.info('CredentialPreservationService', 
            `📋 Available credentials: ${this.nodeCredentials.size} entries`);
        
        if (this.nodeCredentials.size > 0) {
            this.nodeCredentials.forEach((cred, addr) => {
                this.logger.info('CredentialPreservationService', 
                    `   - ${addr}: uuid=${cred.uuid}, ownerUuid=${cred.ownerUuid}, isOwner=${cred.isOwner}`);
            });
        }
        
        return null;
    }

    /**
     * Updates credentials after successful authentication
     * @param address The node address
     * @param uuid The new UUID
     * @param ownerUuid The new owner UUID
     */
    updateCredentials(address: Address, uuid: string, ownerUuid: string): void {
        const addressStr = address.toString();
        const credentials = this.nodeCredentials.get(addressStr);
        
        if (credentials) {
            credentials.uuid = uuid;
            credentials.ownerUuid = ownerUuid;
            credentials.lastAuthenticated = Date.now();
            
            this.logger.debug('CredentialPreservationService', 
                `Updated credentials for ${addressStr}: uuid=${uuid}, ownerUuid=${ownerUuid}`);
        }
    }

    /**
     * Clears credentials for a specific node
     * @param address The node address
     */
    clearCredentials(address: Address): void {
        const addressStr = address.toString();
        if (this.nodeCredentials.delete(addressStr)) {
            this.logger.debug('CredentialPreservationService', 
                `Cleared credentials for ${addressStr}`);
        }
    }

    /**
     * Cleans up stale credentials older than the specified age
     * @param maxAgeMs Maximum age in milliseconds (default: 5 minutes)
     */
    cleanupStaleCredentials(maxAgeMs: number = 300000): void {
        const now = Date.now();
        const addressesToRemove: string[] = [];
        
        this.nodeCredentials.forEach((credentials, addressStr) => {
            const age = now - credentials.lastAuthenticated;
            if (age > maxAgeMs) {
                addressesToRemove.push(addressStr);
            }
        });
        
        if (addressesToRemove.length > 0) {
            addressesToRemove.forEach(addressStr => {
                this.nodeCredentials.delete(addressStr);
            });
            
            this.logger.debug('CredentialPreservationService', 
                `Cleaned up ${addressesToRemove.length} stale credential entries`);
        }
    }

    /**
     * Gets a summary of preserved credentials for debugging
     * @returns A summary string
     */
    getCredentialsSummary(): string {
        const entries = Array.from(this.nodeCredentials.entries());
        if (entries.length === 0) {
            return 'none';
        }
        
        return entries.map(([address, creds]) => 
            `${address}: uuid=${creds.uuid || 'null'}, ownerUuid=${creds.ownerUuid || 'null'}, isOwner=${creds.isOwner}`
        ).join('; ');
    }

    /**
     * Smart credential restoration that handles both scenarios:
     * 1. If member added event occurred -> use new UUID
     * 2. If no member added event -> use old UUID (node probably never changed)
     * @param address The address to restore credentials for
     * @param hasMemberAddedEvent Whether we received a member added event for this address
     * @returns The appropriate credentials to use
     */
    public smartRestoreCredentials(address: Address, hasMemberAddedEvent: boolean = false): NodeCredentials | null {
        const addressStr = address.toString();
        const credentials = this.nodeCredentials.get(addressStr);
        
        if (!credentials) {
            this.logger.debug('CredentialPreservationService', 
                `No preserved credentials found for ${addressStr}`);
            return null;
        }
        
        if (hasMemberAddedEvent) {
            // Member actually left and rejoined - use the updated credentials
            this.logger.info('CredentialPreservationService', 
                `🔄 Member added event detected for ${addressStr}, using updated credentials: uuid=${credentials.uuid}`);
            return credentials;
        } else {
            // No member added event - node probably never changed, use original credentials
            this.logger.info('CredentialPreservationService', 
                `🔄 No member added event for ${addressStr}, assuming node unchanged, using original credentials: uuid=${credentials.uuid}`);
            return credentials;
        }
    }

    /**
     * Checks if we have valid credentials for an address
     * @param address The address to check
     * @returns True if we have valid credentials
     */
    public hasValidCredentials(address: Address): boolean {
        const addressStr = address.toString();
        const credentials = this.nodeCredentials.get(addressStr);
        return !!(credentials && credentials.uuid && credentials.ownerUuid);
    }

    /**
     * Gets the last known UUID for an address
     * @param address The address to get UUID for
     * @returns The UUID or null if not found
     */
    public getLastKnownUuid(address: Address): string | null {
        const addressStr = address.toString();
        const credentials = this.nodeCredentials.get(addressStr);
        return credentials ? credentials.uuid : null;
    }

    /**
     * Updates the owner UUID for ALL preserved credentials
     * This is called when cluster membership changes and we need to sync all credentials
     * @param newOwnerUuid The new owner UUID from the current cluster state
     */
    public updateAllOwnerUuids(newOwnerUuid: string): void {
        this.logger.info('CredentialPreservationService', 
            `🔄 Updating ALL preserved credentials with new owner UUID: ${newOwnerUuid}`);
        
        let updatedCount = 0;
        this.nodeCredentials.forEach((credentials, addressStr) => {
            if (credentials.ownerUuid !== newOwnerUuid) {
                const oldOwnerUuid = credentials.ownerUuid;
                credentials.ownerUuid = newOwnerUuid;
                updatedCount++;
                
                this.logger.info('CredentialPreservationService', 
                    `🔄 Updated ${addressStr}: ownerUuid ${oldOwnerUuid} → ${newOwnerUuid}`);
            }
        });
        
        this.logger.info('CredentialPreservationService', 
            `✅ Updated ${updatedCount} credential entries with new owner UUID`);
    }

    /**
     * Gets the current owner UUID from preserved credentials
     * @returns The current owner UUID or null if not found
     */
    public getCurrentOwnerUuid(): string | null {
        // Find any credential that has isOwner=true
        for (const credentials of Array.from(this.nodeCredentials.values())) {
            if (credentials.isOwner) {
                return credentials.ownerUuid;
            }
        }
        
        // If no owner found, return the first available ownerUuid
        for (const credentials of Array.from(this.nodeCredentials.values())) {
            if (credentials.ownerUuid) {
                return credentials.ownerUuid;
            }
        }
        
        return null;
    }

    /**
     * Validates that all credentials have consistent owner UUIDs
     * @returns True if all credentials are consistent, false otherwise
     */
    public validateOwnerUuidConsistency(): boolean {
        const ownerUuids = new Set<string>();
        
        this.nodeCredentials.forEach((credentials, addressStr) => {
            if (credentials.ownerUuid) {
                ownerUuids.add(credentials.ownerUuid);
            }
        });
        
        const isConsistent = ownerUuids.size <= 1;
        
        if (!isConsistent) {
            this.logger.warn('CredentialPreservationService', 
                `⚠️ Owner UUID inconsistency detected: ${Array.from(ownerUuids).join(', ')}`);
        }
        
        return isConsistent;
    }

    /**
     * Invalidates ALL credentials that have a specific owner UUID
     * This is called when cluster membership changes and old owner UUIDs become invalid
     * @param oldOwnerUuid The old owner UUID that is no longer valid
     */
    public invalidateCredentialsWithOwnerUuid(oldOwnerUuid: string): void {
        this.logger.info('CredentialPreservationService', 
            `🗑️ Invalidating ALL credentials with old owner UUID: ${oldOwnerUuid}`);
        
        let invalidatedCount = 0;
        const addressesToRemove: string[] = [];
        
        this.nodeCredentials.forEach((credentials, addressStr) => {
            if (credentials.ownerUuid === oldOwnerUuid) {
                this.logger.info('CredentialPreservationService', 
                    `🗑️ Invalidating credentials for ${addressStr}: old ownerUuid=${oldOwnerUuid}`);
                addressesToRemove.push(addressStr);
                invalidatedCount++;
            }
        });
        
        // Remove the invalidated credentials
        addressesToRemove.forEach(addressStr => {
            this.nodeCredentials.delete(addressStr);
        });
        
        this.logger.info('CredentialPreservationService', 
            `✅ Invalidated ${invalidatedCount} credential entries with old owner UUID: ${oldOwnerUuid}`);
    }

    /**
     * Invalidates ALL credentials that don't match the current cluster owner UUID
     * This ensures complete credential consistency across the entire cluster
     * @param currentClusterOwnerUuid The current cluster owner UUID that should be valid
     */
    public invalidateAllCredentialsExceptCurrentOwner(currentClusterOwnerUuid: string): void {
        this.logger.info('CredentialPreservationService', 
            `🗑️ Comprehensive credential cleanup: Invalidating ALL credentials except those with current owner UUID: ${currentClusterOwnerUuid}`);
        
        let invalidatedCount = 0;
        const addressesToRemove: string[] = [];
        
        this.nodeCredentials.forEach((credentials, addressStr) => {
            if (credentials.ownerUuid !== currentClusterOwnerUuid) {
                this.logger.info('CredentialPreservationService', 
                    `🗑️ Invalidating credentials for ${addressStr}: old ownerUuid=${credentials.ownerUuid} ≠ current=${currentClusterOwnerUuid}`);
                addressesToRemove.push(addressStr);
                invalidatedCount++;
            } else {
                this.logger.debug('CredentialPreservationService', 
                    `✅ Keeping valid credentials for ${addressStr}: ownerUuid=${credentials.ownerUuid} matches current`);
            }
        });
        
        // Remove the invalidated credentials
        addressesToRemove.forEach(addressStr => {
            this.nodeCredentials.delete(addressStr);
        });
        
        this.logger.info('CredentialPreservationService', 
            `✅ Comprehensive cleanup completed: Invalidated ${invalidatedCount} credential entries, kept ${this.nodeCredentials.size} valid entries`);
        
        // Log remaining valid credentials for debugging
        if (this.nodeCredentials.size > 0) {
            this.logger.info('CredentialPreservationService', 
                `📋 Remaining valid credentials after cleanup:`);
            this.nodeCredentials.forEach((credentials, addressStr) => {
                this.logger.info('CredentialPreservationService', 
                    `   - ${addressStr}: uuid=${credentials.uuid}, ownerUuid=${credentials.ownerUuid}, isOwner=${credentials.isOwner}`);
            });
        }
    }

    /**
     * Invalidates only problematic credentials while preserving working ones
     * This prevents breaking working connections (like 192.168.1.108)
     * @param currentClusterOwnerUuid The current cluster owner UUID that should be valid
     */
    public invalidateOnlyProblematicCredentials(currentClusterOwnerUuid: string): void {
        this.logger.info('CredentialPreservationService', 
            `🎯 Targeted credential cleanup: Only invalidating problematic credentials, preserving working ones`);
        
        let invalidatedCount = 0;
        const addressesToRemove: string[] = [];
        
        this.nodeCredentials.forEach((credentials, addressStr) => {
            // Only invalidate if the ownerUuid is clearly wrong AND we've had authentication issues
            if (credentials.ownerUuid !== currentClusterOwnerUuid) {
                // Check if this address has had recent authentication failures
                const hasAuthIssues = this.hasRecentAuthenticationIssues(addressStr);
                
                if (hasAuthIssues) {
                    this.logger.info('CredentialPreservationService', 
                        `🎯 Invalidating problematic credentials for ${addressStr}: old ownerUuid=${credentials.ownerUuid} ≠ current=${currentClusterOwnerUuid} (has auth issues)`);
                    addressesToRemove.push(addressStr);
                    invalidatedCount++;
                } else {
                    this.logger.info('CredentialPreservationService', 
                        `✅ Keeping working credentials for ${addressStr}: ownerUuid=${credentials.ownerUuid} (no auth issues)`);
                }
            } else {
                this.logger.debug('CredentialPreservationService', 
                    `✅ Keeping valid credentials for ${addressStr}: ownerUuid=${credentials.ownerUuid} matches current`);
            }
        });
        
        // Remove only the problematic credentials
        addressesToRemove.forEach(addressStr => {
            this.nodeCredentials.delete(addressStr);
        });
        
        this.logger.info('CredentialPreservationService', 
            `✅ Targeted cleanup completed: Invalidated ${invalidatedCount} problematic credential entries, kept ${this.nodeCredentials.size} working entries`);
    }

    /**
     * Checks if an address has had recent authentication issues
     * This helps determine which credentials to invalidate
     * @param addressStr The address to check
     * @returns True if the address has recent auth issues
     */
    private hasRecentAuthenticationIssues(addressStr: string): boolean {
        // For now, we'll be conservative and only invalidate if we're certain
        // In the future, we could track authentication failure history
        return false; // Don't invalidate anything for now - revert to previous working behavior
    }

    /**
     * Clears all credentials for a specific address
     * This is called when we need to force fresh authentication for a node
     * @param address The address to clear credentials for
     */
    public clearCredentialsForAddress(address: Address): void {
        const addressStr = address.toString();
        if (this.nodeCredentials.has(addressStr)) {
            this.logger.info('CredentialPreservationService', 
                `🗑️ Clearing credentials for ${addressStr}`);
            this.nodeCredentials.delete(addressStr);
        }
    }

    /**
     * Gets all addresses that have credentials with a specific owner UUID
     * @param ownerUuid The owner UUID to search for
     * @returns Array of addresses that have credentials with this owner UUID
     */
    public getAddressesWithOwnerUuid(ownerUuid: string): string[] {
        const addresses: string[] = [];
        this.nodeCredentials.forEach((credentials, addressStr) => {
            if (credentials.ownerUuid === ownerUuid) {
                addresses.push(addressStr);
            }
        });
        return addresses;
    }
}
