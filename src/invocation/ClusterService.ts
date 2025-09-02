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

import {ClientConnection} from './ClientConnection';
import * as Promise from 'bluebird';
import {ClientAddMembershipListenerCodec} from '../codec/ClientAddMembershipListenerCodec';
import {Member} from '../core/Member';
import {LoggingService} from '../logging/LoggingService';
import {ClientInfo} from '../ClientInfo';
import HazelcastClient from '../HazelcastClient';
import {IllegalStateError} from '../HazelcastError';
import * as assert from 'assert';
import {MemberSelector} from '../core/MemberSelector';
import {AddressHelper, DeferredPromise} from '../Util';
import {MemberAttributeEvent, MemberAttributeOperationType} from '../core/MemberAttributeEvent';
import {MembershipListener} from '../core/MembershipListener';
import {MembershipEvent} from '../core/MembershipEvent';
import {UuidUtil} from '../util/UuidUtil';
import {ILogger} from '../logging/ILogger';
import Address = require('../Address');
import ClientMessage = require('../ClientMessage');
import {HazelcastFailoverManager} from './HazelcastFailoverManager';

export enum MemberEvent {
    ADDED = 1,
    REMOVED = 2,
}

/**
 * Manages the relationship of this client with the cluster.
 */
export class ClusterService {

    /**
     * The unique identifier of the owner server node. This node is responsible for resource cleanup
     */
    public ownerUuid: string = null;

    /**
     * The unique identifier of this client instance. Assigned by owner node on authentication
     */
    public uuid: string = null;

    private knownAddresses: Address[] = [];
    private members: Member[] = [];
    private client: HazelcastClient;
    private ownerConnection: ClientConnection;
    private membershipListeners: Map<string, MembershipListener> = new Map();
    private logger: ILogger;
    private failoverInProgress: boolean = false;
    private lastFailoverAttempt: number = 0;
    private readonly failoverCooldown: number = 5000; // 5 seconds cooldown between failover attempts
    private downAddresses: Map<string, number> = new Map(); // address -> timestamp when marked down
    private readonly addressBlockDuration: number = 15000; // Reduced from 30000ms to 15000ms
    private reconnectionTask: any = null;
    private readonly reconnectionInterval: number = 10000; // 10 seconds between reconnection attempts
    private failoverManager: HazelcastFailoverManager;

    constructor(client: HazelcastClient) {
        this.client = client;
        this.logger = this.client.getLoggingService().getLogger();
        this.members = [];
        this.startReconnectionTask();
        this.startStateLoggingTask();
        this.failoverManager = new HazelcastFailoverManager(client, this.logger);
    }

    /**
     * Starts cluster service.
     * @returns
     */
    start(): Promise<void> {
        this.initHeartbeatListener();
        this.initConnectionListener();
        return this.connectToCluster();
    }

    /**
     * Connects to cluster. It uses the addresses provided in the configuration.
     * @returns
     */
    connectToCluster(): Promise<void> {
        return this.getPossibleMemberAddresses().then((res) => {
            this.knownAddresses = [];
            res.forEach((value) => {
                this.knownAddresses = this.knownAddresses.concat(AddressHelper.getSocketAddresses(value));
            });

            const attemptLimit = this.client.getConfig().networkConfig.connectionAttemptLimit;
            const attemptPeriod = this.client.getConfig().networkConfig.connectionAttemptPeriod;
            return this.tryConnectingToAddresses(0, attemptLimit, attemptPeriod);
        });
    }

    getPossibleMemberAddresses(): Promise<string[]> {
        const addresses: Set<string> = new Set();

        this.getMembers().forEach(function (member): void {
            addresses.add(member.address.toString());
        });

        let providerAddresses: Set<string> = new Set();
        const promises: Array<Promise<void>> = [];
        this.client.getConnectionManager().addressProviders.forEach((addressProvider) => {
            promises.push(addressProvider.loadAddresses().then((res) => {
                providerAddresses = new Set([...Array.from(providerAddresses), ...res]);
            }).catch((err) => {
                this.logger.warn('Error from AddressProvider: ' + addressProvider, err);
            }));
        });
        return Promise.all(promises).then(() => {
            return Array.from(new Set([...Array.from(addresses), ...Array.from(providerAddresses)]));
        });
    }

    /**
     * Returns the owner connection if available
     */
    getOwnerConnection(): ClientConnection | null {
        return this.ownerConnection;
    }

    /**
     * Returns whether failover is currently in progress
     */
    isFailoverInProgress(): boolean {
        return this.failoverInProgress;
    }

    /**
     * Returns the list of known addresses in the cluster
     */
    getKnownAddresses(): Address[] {
        return [...this.knownAddresses]; // Return a copy to prevent external modification
    }

    /**
     * Returns the list of members in the cluster.
     * @returns
     */
    getMembers(selector?: MemberSelector): Member[] {
        if (selector === undefined) {
            return this.members;
        } else {
            const members: Member[] = [];
            this.members.forEach(function (member): void {
                if (selector.select(member)) {
                    members.push(member);
                }
            });
            return members;
        }
    }

    getMember(uuid: string): Member {
        for (const member of this.members) {
            if (member.uuid === uuid) {
                return member;
            }
        }
        return null;
    }

    /**
     * Returns the number of nodes in cluster.
     * @returns {number}
     */
    getSize(): number {
        return this.members.length;
    }

    /**
     * Returns information about this client.
     * @returns {ClientInfo}
     */
    getClientInfo(): ClientInfo {
        const info = new ClientInfo();
        info.uuid = this.uuid;
        
        const ownerConnection = this.getOwnerConnection();
        if (ownerConnection) {
            info.localAddress = ownerConnection.getLocalAddress();
        } else {
            info.localAddress = null;
        }
        
        return info;
    }

    /**
     * Adds MembershipListener to listen for membership updates. There is no check for duplicate registrations,
     * so if you register the listener twice, it will get events twice.
     * @param {MembershipListener} The listener to be registered
     * @return The registration ID
     */
    addMembershipListener(membershipListener: MembershipListener): string {
        const registrationId = UuidUtil.generate().toString();
        this.membershipListeners.set(registrationId, membershipListener);
        return registrationId;
    }

    /**
     * Removes registered MembershipListener.
     * @param {string} The registration ID
     * @return {boolean} true if successfully removed, false otherwise
     */
    removeMembershipListener(registrationId: string): boolean {
        if (registrationId === null) {
            throw new RangeError('registrationId cannot be null');
        }
        return this.membershipListeners.delete(registrationId);
    }

    initMembershipListener(): Promise<void> {
        const request = ClientAddMembershipListenerCodec.encodeRequest(false);

        const handler = (m: ClientMessage) => {
            const handleMember = this.handleMember.bind(this);
            const handleMemberList = this.handleMemberList.bind(this);
            const handleAttributeChange = this.handleMemberAttributeChange.bind(this);
            ClientAddMembershipListenerCodec.handle(m, handleMember, handleMemberList, handleAttributeChange, null);
        };
        
        const ownerConnection = this.getOwnerConnection();
        if (!ownerConnection) {
            return Promise.reject(new Error('Cannot initialize membership listener: no owner connection available'));
        }
        
        return this.client.getInvocationService().invokeOnConnection(ownerConnection, request, handler)
            .then((resp: ClientMessage) => {
                this.logger.trace('ClusterService', 'Registered listener with id '
                    + ClientAddMembershipListenerCodec.decodeResponse(resp).response);
            });
    }

    private initHeartbeatListener(): void {
        this.client.getHeartbeat().addListener({
            onHeartbeatStopped: this.onHeartbeatStopped.bind(this),
        });
    }

    private initConnectionListener(): void {
        this.client.getConnectionManager().on('connectionClosed', this.onConnectionClosed.bind(this));
    }

    private onConnectionClosed(connection: ClientConnection): void {
        this.logger.warn('ClusterService', 'Connection closed to ' + connection.toString());
        
        // Mark the address as down when connection is closed
        this.markAddressAsDown(connection.getAddress());
        
        if (connection.isAuthenticatedAsOwner()) {
            this.ownerConnection = null;
            this.triggerFailover();
        }
    }

    private onHeartbeatStopped(connection: ClientConnection): void {
        this.logger.warn('ClusterService', connection.toString() + ' stopped heartbeating.');
        
        // Mark the address as down when heartbeat stops
        this.markAddressAsDown(connection.getAddress());
        
        if (connection.isAuthenticatedAsOwner()) {
            this.client.getConnectionManager().destroyConnection(connection.getAddress());
            this.ownerConnection = null;
            this.triggerFailover();
        }
    }

    private triggerFailover(): void {
        const now = Date.now();
        if (this.failoverInProgress || (now - this.lastFailoverAttempt) < this.failoverCooldown) {
            this.logger.debug('ClusterService', 'Failover already in progress or too soon since last attempt');
            return;
        }

        this.failoverInProgress = true;
        this.lastFailoverAttempt = now;

        // Check if this is a single-node scenario
        const isSingleNode = this.knownAddresses.length === 1;
        
        if (isSingleNode) {
            this.logger.info('ClusterService', '🔄 SINGLE-NODE CLUSTER RESET: Node restart detected, starting fresh...');
            this.handleSingleNodeClusterReset();
        } else {
            this.logger.info('ClusterService', '🚀 Starting failover process - SERVER-FIRST APPROACH...');
            
            // SERVER-FIRST: No credential preservation needed
            // We trust the server will provide correct member information
            this.logger.info('ClusterService', '🎯 SERVER-FIRST: No credential management - trusting server data');
            this.handleMultiNodeFailover();
        }
    }

    /**
     * Handles single-node cluster reset - treats node restart as fresh cluster
     */
    private handleSingleNodeClusterReset(): void {
        this.logger.info('ClusterService', '🧹 SINGLE-NODE RESET: Clearing all credentials and state...');
        
        // Clear all stored credentials - node restart means fresh cluster
        this.client.getConnectionManager().clearAllCredentials();
        
        // Reset client UUIDs - will be assigned fresh by server
        this.uuid = null;
        this.ownerUuid = null;
        
        // Log state before reset
        this.logCurrentState();
        
        // Force cleanup of all dead connections
        this.client.getConnectionManager().forceCleanupDeadConnections();
        
        // Clear partition information
        this.client.getPartitionService().clearPartitionTable();
        
        // Direct reconnection without waiting for member events
        this.logger.info('ClusterService', '🔄 SINGLE-NODE RESET: Attempting direct reconnection...');
        this.connectToCluster()
            .then(() => {
                this.logger.info('ClusterService', '✅ Single-node cluster reset completed successfully');
                this.logCurrentState();
            })
            .catch((error) => {
                this.logger.error('ClusterService', 'Single-node cluster reset failed', error);
                this.logCurrentState();
            })
            .finally(() => {
                this.failoverInProgress = false;
            });
    }

    /**
     * Handles multi-node failover - preserves existing logic
     */
    private handleMultiNodeFailover(): void {
        // Log state before failover
        this.logCurrentState();
        
        // Force cleanup of all dead connections to prevent leakage
        this.client.getConnectionManager().forceCleanupDeadConnections();
        
        // Clear any stale partition information
        this.client.getPartitionService().clearPartitionTable();
        
        // Attempt to reconnect to cluster
        this.connectToCluster()
            .then(() => {
                this.logger.info('ClusterService', '✅ Failover completed successfully - SERVER-FIRST approach');
                
                // No credential management needed - server handles everything
                
                this.logCurrentState(); // Log state after successful failover
            })
            .catch((error) => {
                this.logger.error('ClusterService', 'Failover failed', error);
                
                // If failover fails, try to unblock at least one address to allow recovery
                this.attemptEmergencyRecovery();
                
                this.logCurrentState(); // Log state after failed failover
                // Don't shutdown immediately, give recovery a chance
            })
            .finally(() => {
                this.failoverInProgress = false;
            });
    }

    /**
     * Attempts emergency recovery when failover fails
     */
    private attemptEmergencyRecovery(): void {
        this.logger.warn('ClusterService', 'Attempting emergency recovery...');
        
        // Unblock at least one address to allow recovery
        if (this.downAddresses.size > 0) {
            const firstBlockedAddress = Array.from(this.downAddresses.keys())[0];
            this.logger.info('ClusterService', `Emergency unblocking address ${firstBlockedAddress}`);
            this.downAddresses.delete(firstBlockedAddress);
            
            // Try to connect to the unblocked address
            try {
                const [host, portStr] = firstBlockedAddress.split(':');
                const port = parseInt(portStr, 10);
                if (host && !isNaN(port)) {
                    const address = new Address(host, port);
                    this.logger.info('ClusterService', `Attempting emergency connection to ${firstBlockedAddress}`);
                    
                    // Try to connect without blocking
                    this.client.getConnectionManager().getOrConnect(address, false)
                        .then((connection: ClientConnection) => {
                            this.logger.info('ClusterService', `Emergency connection successful to ${firstBlockedAddress}`);
                            this.evaluateOwnershipChange(address, connection);
                            this.client.getPartitionService().refresh();
                        })
                        .catch((error) => {
                            this.logger.warn('ClusterService', `Emergency connection failed to ${firstBlockedAddress}:`, error);
                        });
                }
            } catch (error) {
                this.logger.error('ClusterService', 'Error during emergency recovery:', error);
            }
        }
    }

    private isAddressKnownDown(address: Address): boolean {
        const addressStr = address.toString();
        const downTime = this.downAddresses.get(addressStr);
        
        if (!downTime) {
            return false;
        }
        
        const now = Date.now();
        const timeSinceDown = now - downTime;
        
        // If address has been down for longer than block duration, unblock it
        if (timeSinceDown > this.addressBlockDuration) {
            this.logger.debug('ClusterService', `Unblocking address ${addressStr} after ${this.addressBlockDuration}ms`);
            this.downAddresses.delete(addressStr);
            return false;
        }
        
        // Address is still blocked
        const remainingBlockTime = this.addressBlockDuration - timeSinceDown;
        this.logger.debug('ClusterService', `Address ${addressStr} is blocked for ${remainingBlockTime}ms more`);
        return true;
    }

    private markAddressAsDown(address: Address): void {
        const addressStr = address.toString();
        const now = Date.now();
        
        this.downAddresses.set(addressStr, now);
        this.logger.warn('ClusterService', `Marked address ${addressStr} as down, will be blocked for ${this.addressBlockDuration}ms`);
        
        // Schedule cleanup of this address after block duration
        setTimeout(() => {
            if (this.downAddresses.has(addressStr)) {
                this.logger.info('ClusterService', `Unblocking address ${addressStr} after block duration`);
                this.downAddresses.delete(addressStr);
            }
        }, this.addressBlockDuration);
    }

    private getDownAddressesInfo(): string {
        const now = Date.now();
        const downInfo: string[] = [];
        
        this.downAddresses.forEach((downTime, address) => {
            const timeSinceDown = now - downTime;
            const remainingTime = Math.max(0, this.addressBlockDuration - timeSinceDown);
            downInfo.push(`${address} (${Math.ceil(remainingTime / 1000)}s remaining)`);
        });
        
        return downInfo.length > 0 ? downInfo.join(', ') : 'none';
    }

    private tryConnectingToAddresses(index: number, remainingAttemptLimit: number,
                                     attemptPeriod: number, cause?: Error): Promise<void> {
        this.logger.debug('ClusterService', 'Trying to connect to addresses, remaining attempt limit: ' + remainingAttemptLimit
            + ', attempt period: ' + attemptPeriod + ', down addresses: ' + this.getDownAddressesInfo());
        
        if (this.knownAddresses.length <= index) {
            remainingAttemptLimit = remainingAttemptLimit - 1;
            if (remainingAttemptLimit <= 0) {
                const errorMessage = 'Unable to connect to any of the following addresses: ' +
                    this.knownAddresses.map((element: Address) => {
                        return element.toString();
                    }).join(', ');
                this.logger.debug('ClusterService', errorMessage);
                const error = new IllegalStateError(errorMessage, cause);
                return Promise.reject(error);
            } else {
                const deferred = DeferredPromise<void>();
                setTimeout(
                    () => {
                        this.tryConnectingToAddresses(0, remainingAttemptLimit, attemptPeriod).then(() => {
                            deferred.resolve();
                        }).catch((e) => {
                            deferred.reject(e);
                        });
                    },
                    attemptPeriod,
                );
                return deferred.promise;
            }
        } else {
            const currentAddress = this.knownAddresses[index];
            
            // Skip addresses that are known to be down
            if (this.isAddressKnownDown(currentAddress)) {
                this.logger.debug('ClusterService', `Skipping known down address: ${currentAddress.toString()}`);
                return this.tryConnectingToAddresses(index + 1, remainingAttemptLimit, attemptPeriod, cause);
            }

            return this.client.getConnectionManager().getOrConnect(currentAddress, true).then((connection: ClientConnection) => {
                connection.setAuthenticatedAsOwner(true);
                this.ownerConnection = connection;
                this.logger.info('ClusterService', `Successfully connected to owner node: ${currentAddress.toString()}`);
                return this.initMembershipListener();
            }).catch((e) => {
                this.logger.warn('ClusterService', `Failed to connect to ${currentAddress.toString()}:`, e);
                this.markAddressAsDown(currentAddress);
                return this.tryConnectingToAddresses(index + 1, remainingAttemptLimit, attemptPeriod, e);
            });
        }
    }

    private handleMember(member: Member, eventType: number): void {
        if (eventType === MemberEvent.ADDED) {
            this.logger.info('ClusterService', member.toString() + ' added to cluster');
            this.memberAdded(member);
        } else if (eventType === MemberEvent.REMOVED) {
            this.logger.info('ClusterService', member.toString() + ' removed from cluster');
            this.memberRemoved(member);
        }
        this.client.getPartitionService().refresh();
    }

    private handleMemberList(members: Member[]): void {
        const prevMembers = this.members;
        this.members = members;
        this.client.getPartitionService().refresh();
        this.logger.info('ClusterService', 'Members received.', this.members);
        
        // Log current state after member list update
        this.logCurrentState();
        
        const events = this.detectMembershipEvents(prevMembers);
        for (const event of events) {
            this.fireMembershipEvent(event);
        }
    }

    private detectMembershipEvents(prevMembers: Member[]): MembershipEvent[] {
        const events: MembershipEvent[] = [];
        const eventMembers = Array.from(this.members);
        const addedMembers: Member[] = [];
        const deletedMembers = Array.from(prevMembers);

        for (const member of this.members) {
            const idx = deletedMembers.findIndex(member.equals, member);
            if (idx === -1) {
                addedMembers.push(member);
            } else {
                deletedMembers.splice(idx, 1);
            }
        }

        // removal events should be added before added events
        for (const member of deletedMembers) {
            events.push(new MembershipEvent(member, MemberEvent.REMOVED, eventMembers));
        }
        for (const member of addedMembers) {
            events.push(new MembershipEvent(member, MemberEvent.ADDED, eventMembers));
        }

        return events;
    }

    private fireMembershipEvent(membershipEvent: MembershipEvent): void {
        this.membershipListeners.forEach((membershipListener, registrationId) => {
            if (membershipEvent.eventType === MemberEvent.ADDED) {
                if (membershipListener && membershipListener.memberAdded) {
                    membershipListener.memberAdded(membershipEvent);
                }
            } else if (membershipEvent.eventType === MemberEvent.REMOVED) {
                if (membershipListener && membershipListener.memberRemoved) {
                    membershipListener.memberRemoved(membershipEvent);
                }
            }
        });
    }

    private handleMemberAttributeChange(
        uuid: string, key: string, operationType: MemberAttributeOperationType, value: string): void {

        this.membershipListeners.forEach((membershipListener, registrationId) => {
            if (membershipListener && membershipListener.memberAttributeChanged) {
                const member = this.getMember(uuid);
                const memberAttributeEvent = new MemberAttributeEvent(member, key, operationType, value);
                membershipListener.memberAttributeChanged(memberAttributeEvent);
            }
        });
    }

    private memberAdded(member: Member): void {
        this.members.push(member);
        
        // Handle member added and update preserved credentials
        this.handleMemberAdded(member);
        
        const membershipEvent = new MembershipEvent(member, MemberEvent.ADDED, this.members);
        this.fireMembershipEvent(membershipEvent);
    }

    private memberRemoved(member: Member): void {
        const memberIndex = this.members.findIndex(member.equals, member);
        if (memberIndex !== -1) {
            const removedMemberList = this.members.splice(memberIndex, 1);
            assert(removedMemberList.length === 1);
        }
        
        // Check if we have a healthy connection to this member
        const connectionManager = this.client.getConnectionManager();
        const existingConnection = connectionManager.getConnection(member.address);
        
        if (existingConnection && existingConnection.isHealthy()) {
            // If the connection is healthy, don't destroy it immediately
            // This prevents unnecessary disconnections during temporary network issues
            this.logger.info('ClusterService', `Member removed but connection is healthy: ${member.address.toString()}, preserving connection`);
            
            // Only destroy if we're not in failover mode
            if (!this.failoverInProgress) {
                this.logger.debug('ClusterService', `Destroying healthy connection to removed member: ${member.address.toString()}`);
                connectionManager.destroyConnection(member.address);
            } else {
                this.logger.debug('ClusterService', `Preserving healthy connection during failover: ${member.address.toString()}`);
            }
        } else {
            // If connection is unhealthy, destroy it
            this.logger.info('ClusterService', `Member removed with unhealthy connection: ${member.address.toString()}, destroying connection`);
            connectionManager.destroyConnection(member.address);
        }
        
        const membershipEvent = new MembershipEvent(member, MemberEvent.REMOVED, this.members);
        this.fireMembershipEvent(membershipEvent);
    }

    /**
     * Performs comprehensive credential cleanup when cluster membership changes
     * This ensures ALL credentials are consistent with the current cluster owner UUID
     * @param connectionManager The connection manager instance
     * @param currentClusterOwnerUuid The current cluster owner UUID
     */




    /**
     * Handles member added event - SERVER-FIRST APPROACH
     * We trust what the server tells us and store it as credentials
     * @param member The member that was added
     */
    private handleMemberAdded(member: any): void {
        this.logger.info('ClusterService', `✅ SERVER CONFIRMED: Member[ uuid: ${member.uuid}, address: ${member.address.toString()}] added to cluster`);
        
        // SERVER-FIRST: Store server data as credentials
        // The server is the authority - we store what it tells us
        
        this.logger.info('ClusterService', 
            `🎯 SERVER-FIRST: Storing server member data as credentials - server is authority`);
        
        const connectionManager = this.client.getConnectionManager();
        
        // Store the server-provided UUID as the authoritative credential
        if (connectionManager && typeof connectionManager.updatePreservedCredentials === 'function') {
            connectionManager.updatePreservedCredentials(member.address, member.uuid);
        }
        
        // Record that we received a member added event for this address
        if (connectionManager && typeof connectionManager.recordMemberAddedEvent === 'function') {
            connectionManager.recordMemberAddedEvent(member.address);
        }
        
        // Find the current owner from the cluster state
        const currentOwner = this.findCurrentOwner();
        if (currentOwner) {
            this.logger.info('ClusterService', 
                `🔄 SERVER-FIRST: Updating ALL credentials with current owner UUID: ${currentOwner.uuid}`);
            
            // Update all credentials with the current owner UUID from server
            if (connectionManager && typeof connectionManager.updateAllCredentialsWithNewOwnerUuid === 'function') {
                connectionManager.updateAllCredentialsWithNewOwnerUuid(currentOwner.uuid);
            }
            
            // CRITICAL FIX: Update client's own UUIDs to match server expectations
            this.logger.info('ClusterService', 
                `🔄 SERVER-FIRST: Updating client UUIDs to match server state`);
            this.logger.info('ClusterService', 
                `   - Old Client UUID: ${this.uuid || 'NOT SET'}`);
            this.logger.info('ClusterService', 
                `   - Old Owner UUID: ${this.ownerUuid || 'NOT SET'}`);
            
            // Update client's own UUIDs with server-provided data
            // The client UUID should match the owner's UUID for authentication
            this.uuid = currentOwner.uuid;
            this.ownerUuid = currentOwner.uuid;
            
            this.logger.info('ClusterService', 
                `   - New Client UUID: ${this.uuid}`);
            this.logger.info('ClusterService', 
                `   - New Owner UUID: ${this.ownerUuid}`);
        }
        
        // Refresh partition table (KEEPING REFRESH METHOD UNTOUCHED as requested)
        this.client.getPartitionService().refresh();
        
        this.logger.info('ClusterService', 
            `✅ SERVER-FIRST: Member ${member.uuid} at ${member.address.toString()} credentials stored from server data`);
    }

    /**
     * Finds the current owner from the cluster state
     * @returns The current owner member or null if not found
     */
    private findCurrentOwner(): any | null {
        // Check if we have an active owner connection
        const ownerConnection = this.ownerConnection;
        if (ownerConnection && ownerConnection.isAlive()) {
            const ownerAddress = ownerConnection.getAddress();
            // Find the member with this address
            for (const member of this.members) {
                if (member.address.toString() === ownerAddress.toString()) {
                    this.logger.debug('ClusterService', 
                        `Found current owner: ${member.uuid} at ${member.address.toString()}`);
                    return member;
                }
            }
        }
        
        // Fallback: look for any member that might be the owner
        this.logger.debug('ClusterService', 
            `No active owner connection found, checking member list for potential owner`);
        return null;
    }

    /**
     * Logs the current state for debugging purposes
     */
    private logCurrentState(): void {
        const activeConnections = Object.keys(this.client.getConnectionManager().getEstablishedConnections()).length;
        const memberCount = this.members.length;
        const downAddressesCount = this.downAddresses.size;
        const hasOwner = !!this.ownerConnection;
        
        this.logger.info('ClusterService', `Current State - Members: ${memberCount}, Active Connections: ${activeConnections}, Down Addresses: ${downAddressesCount}, Has Owner: ${hasOwner}`);
        
        if (this.ownerConnection) {
            this.logger.info('ClusterService', `Owner Connection: ${this.ownerConnection.getAddress().toString()}, Alive: ${this.ownerConnection.isAlive()}`);
        }
        
        // Log all active connections
        const connections = this.client.getConnectionManager().getEstablishedConnections();
        Object.keys(connections).forEach(addressStr => {
            const connection = connections[addressStr];
            this.logger.debug('ClusterService', `Connection to ${addressStr}: Alive=${connection.isAlive()}, Owner=${connection.isAuthenticatedAsOwner()}`);
        });
        
        // Log down addresses
        if (downAddressesCount > 0) {
            const downAddresses = Array.from(this.downAddresses.keys());
            this.logger.debug('ClusterService', `Down Addresses: ${downAddresses.join(', ')}`);
        }
    }

    private startReconnectionTask(): void {
        // Periodically attempt to reconnect to previously failed addresses
        this.reconnectionTask = setInterval(() => {
            this.attemptReconnectionToFailedNodes();
        }, this.reconnectionInterval);
    }

    /**
     * Starts a periodic task to log the current state for debugging
     */
    private startStateLoggingTask(): void {
        // Log state every 30 seconds for debugging
        setInterval(() => {
            if (this.client.getLifecycleService().isRunning()) {
                this.logCurrentState();
            }
        }, 30000);
    }

    private attemptReconnectionToFailedNodes(): void {
        // Allow reconnection even during failover, but be more careful
        if (this.failoverInProgress) {
            this.logger.debug('ClusterService', 'Skipping reconnection attempt during failover');
            return;
        }

        const now = Date.now();
        const addressesToReconnect: Address[] = [];
        const totalDownAddresses = this.downAddresses.size;

        // If we have no down addresses, we can skip
        if (totalDownAddresses === 0) {
            return;
        }

        // Find addresses that are no longer blocked
        this.downAddresses.forEach((downTime, addressStr) => {
            const timeSinceDown = now - downTime;
            if (timeSinceDown > this.addressBlockDuration) {
                // Parse the address string back to Address object
                try {
                    const [host, portStr] = addressStr.split(':');
                    const port = parseInt(portStr, 10);
                    if (host && !isNaN(port)) {
                        const address = new Address(host, port);
                        
                        // Check if we already have a connection to this address
                        if (this.client.getConnectionManager().hasConnection(address)) {
                            this.logger.debug('ClusterService', `Already have active connection to ${addressStr}, removing from down addresses`);
                            this.downAddresses.delete(addressStr);
                            return;
                        }
                        
                        addressesToReconnect.push(address);
                    }
                } catch (error) {
                    this.logger.warn('ClusterService', `Failed to parse address ${addressStr} for reconnection:`, error);
                }
            }
        });

        if (addressesToReconnect.length > 0) {
            this.logger.info('ClusterService', `Attempting to reconnect to ${addressesToReconnect.length} previously failed nodes: ${addressesToReconnect.map(addr => addr.toString()).join(', ')}`);
            
            // Attempt to establish connections to each unblocked address
            addressesToReconnect.forEach(address => {
                this.attemptReconnectionToAddress(address);
            });
        } else if (totalDownAddresses > 0) {
            // Log remaining blocked addresses for debugging
            const remainingBlocked = Array.from(this.downAddresses.keys()).map(addr => {
                const downTime = this.downAddresses.get(addr);
                const timeSinceDown = now - downTime;
                const remainingTime = Math.max(0, this.addressBlockDuration - timeSinceDown);
                return `${addr} (${Math.ceil(remainingTime / 1000)}s remaining)`;
            });
            this.logger.debug('ClusterService', `Still waiting for ${totalDownAddresses} addresses to unblock: ${remainingBlocked.join(', ')}`);
        }
        
        // Log current state after reconnection attempts
        this.logCurrentState();
    }

    /**
     * Attempts to establish a connection to a specific address
     * @param address The address to reconnect to
     */
    private attemptReconnectionToAddress(address: Address): void {
        const addressStr = address.toString();
        
        // Check if we already have a connection to this address
        if (this.client.getConnectionManager().hasConnection(address)) {
            this.logger.debug('ClusterService', `Already have active connection to ${addressStr}, skipping reconnection`);
            this.downAddresses.delete(addressStr);
            return;
        }
        
        // Check if we're already trying to connect to this address
        const connectionManager = this.client.getConnectionManager();
        const establishedConnections = connectionManager.getEstablishedConnections();
        const pendingConnections = Object.keys(connectionManager.getPendingConnections()).length;
        
        if (pendingConnections > 0) {
            this.logger.debug('ClusterService', `Already have pending connections, skipping reconnection to ${addressStr}`);
            return;
        }
        
        // Remove from down addresses to allow connection attempt
        this.downAddresses.delete(addressStr);
        this.logger.debug('ClusterService', `Attempting reconnection to ${addressStr}`);
        
        // Attempt to establish connection (not as owner, just as regular member connection)
        this.client.getConnectionManager().getOrConnect(address, false)
            .then((connection: ClientConnection) => {
                this.logger.info('ClusterService', `Successfully reconnected to ${addressStr}`);
                
                // Only evaluate ownership change if we don't have an owner or current owner is unhealthy
                if (!this.ownerConnection || !this.ownerConnection.isHealthy()) {
                    this.logger.info('ClusterService', `Evaluating ownership change for ${addressStr}`);
                    this.evaluateOwnershipChange(address, connection);
                } else {
                    this.logger.debug('ClusterService', `Keeping ${addressStr} as member connection, current owner is healthy`);
                }
                
                // Trigger partition service refresh to update routing information
                this.client.getPartitionService().refresh();
                
            }).catch((error) => {
                this.logger.warn('ClusterService', `Reconnection attempt to ${addressStr} failed:`, error);
                
                // Mark the address as down again, but with a shorter block duration for reconnection attempts
                const shorterBlockDuration = Math.min(this.addressBlockDuration / 2, 15000); // Max 15 seconds
                this.markAddressAsDownWithDuration(address, shorterBlockDuration);
            });
    }

    /**
     * Evaluates whether we should switch ownership to a reconnected node
     * @param address The address of the reconnected node
     * @param connection The connection to the reconnected node
     */
    private evaluateOwnershipChange(address: Address, connection: ClientConnection): void {
        // If we don't have an owner connection, this reconnected node becomes the owner
        if (!this.ownerConnection) {
            this.logger.info('ClusterService', `Promoting reconnected node ${address.toString()} to owner status`);
            this.promoteToOwner(connection, address);
            return;
        }

        // If our current owner connection is having issues, consider switching
        if (this.ownerConnection && !this.ownerConnection.isAlive()) {
            this.logger.info('ClusterService', `Current owner is unhealthy, switching to reconnected node ${address.toString()}`);
            this.promoteToOwner(connection, address);
            return;
        }

        // Don't switch ownership if current owner is healthy
        this.logger.debug('ClusterService', `Current owner is healthy, keeping ${address.toString()} as member connection`);
    }

    /**
     * Promotes a connection to owner status
     * @param connection The connection to promote
     * @param address The address of the promoted connection
     */
    private promoteToOwner(connection: ClientConnection, address: Address): void {
        try {
            // Close the old owner connection if it exists
            if (this.ownerConnection && this.ownerConnection !== connection) {
                this.logger.info('ClusterService', `Closing previous owner connection to ${this.ownerConnection.getAddress().toString()}`);
                this.client.getConnectionManager().destroyConnection(this.ownerConnection.getAddress());
            }

            // Set the new owner connection
            connection.setAuthenticatedAsOwner(true);
            this.ownerConnection = connection;
            
            this.logger.info('ClusterService', `Successfully promoted ${address.toString()} to owner status`);
            
            // Refresh partition information with the new owner
            this.client.getPartitionService().refresh();
            
        } catch (error) {
            this.logger.error('ClusterService', `Failed to promote ${address.toString()} to owner:`, error);
            // If promotion fails, mark the address as down again
            this.markAddressAsDown(address);
        }
    }

    /**
     * Marks an address as down with a custom block duration
     * @param address The address to mark as down
     * @param blockDuration The duration to block the address (in milliseconds)
     */
    private markAddressAsDownWithDuration(address: Address, blockDuration: number): void {
        const addressStr = address.toString();
        const now = Date.now();
        
        // Don't block if we already have a healthy connection to this address
        if (this.client.getConnectionManager().hasConnection(address)) {
            this.logger.debug('ClusterService', `Not blocking ${addressStr} as we have a healthy connection`);
            return;
        }
        
        // Don't block if this would leave us with no available nodes
        const totalDownAddresses = this.downAddresses.size;
        const totalMembers = this.members.length;
        if (totalDownAddresses >= totalMembers - 1) {
            this.logger.warn('ClusterService', `Not blocking ${addressStr} as it would leave us with no available nodes`);
            return;
        }
        
        this.downAddresses.set(addressStr, now);
        this.logger.warn('ClusterService', `Marked address ${addressStr} as down, will be blocked for ${blockDuration}ms`);
        
        // Schedule cleanup of this address after block duration
        setTimeout(() => {
            if (this.downAddresses.has(addressStr)) {
                this.logger.info('ClusterService', `Unblocking address ${addressStr} after block duration`);
                this.downAddresses.delete(addressStr);
            }
        }, blockDuration);
    }

    /**
     * Handles ownership change when failover occurs
     * @param newOwnerAddress The address of the new owner
     * @param newOwnerConnection The connection to the new owner
     */
    handleOwnershipChange(newOwnerAddress: Address, newOwnerConnection: ClientConnection): void {
        this.logger.info('ClusterService', `Handling ownership change to ${newOwnerAddress.toString()}`);
        
        // Update owner connection
        this.ownerConnection = newOwnerConnection;
        
        // Note: Owner UUID will be updated when authentication completes
        // For now, we'll keep the existing owner UUID
        
        // Clear any stale partition information
        this.client.getPartitionService().clearPartitionTable();
        
        // Refresh partition information with the new owner
        this.client.getPartitionService().refresh();
        
        this.logger.info('ClusterService', `Ownership change completed, new owner: ${newOwnerAddress.toString()}`);
    }

    /**
     * Gets the failover manager for external access
     * @returns The failover manager instance
     */
    getFailoverManager(): HazelcastFailoverManager {
        return this.failoverManager;
    }

    shutdown(): void {
        if (this.reconnectionTask) {
            clearInterval(this.reconnectionTask);
            this.reconnectionTask = null;
        }
        this.downAddresses.clear();
    }
}
