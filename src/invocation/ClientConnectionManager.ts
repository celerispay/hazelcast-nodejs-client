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

import {Buffer} from 'safe-buffer';
import * as Promise from 'bluebird';
import {EventEmitter} from 'events';
import HazelcastClient from '../HazelcastClient';
import {ClientNotActiveError, HazelcastError, IllegalStateError} from '../HazelcastError';
import {ClientConnection} from './ClientConnection';
import {ConnectionAuthenticator} from './ConnectionAuthenticator';
import * as net from 'net';
import * as tls from 'tls';
import {DeferredPromise, loadNameFromPath} from '../Util';
import {BasicSSLOptionsFactory} from '../connection/BasicSSLOptionsFactory';
import {AddressTranslator} from '../connection/AddressTranslator';
import {AddressProvider} from '../connection/AddressProvider';
import {ILogger} from '../logging/ILogger';
import Address = require('../Address');
import {SSLOptionsFactory} from '../connection/SSLOptionsFactory';
import {CredentialPreservationService} from './CredentialPreservationService';

const EMIT_CONNECTION_CLOSED = 'connectionClosed';
const EMIT_CONNECTION_OPENED = 'connectionOpened';

/**
 * Maintains connections between the client and members of the cluster.
 */
export class ClientConnectionManager extends EventEmitter {
    establishedConnections: { [address: string]: ClientConnection } = {};
    readonly addressProviders: AddressProvider[];
    private readonly client: HazelcastClient;
    private pendingConnections: { [address: string]: Promise.Resolver<ClientConnection> } = {};
    private logger: ILogger;
    private readonly addressTranslator: AddressTranslator;
    private connectionHealthCheckInterval: any; // Use any instead of NodeJS.Timeout for compatibility
    private failedConnections: Set<string> = new Set();
    private readonly maxConnectionRetries: number = 3;
    private readonly connectionRetryDelay: number = 1000;
    private connectionCleanupTask: any = null;
    private readonly connectionCleanupInterval: number = 15000; // 15 seconds between cleanup checks

    constructor(client: HazelcastClient, addressTranslator: AddressTranslator, addressProviders: AddressProvider[]) {
        super();
        this.client = client;
        this.logger = this.client.getLoggingService().getLogger();
        this.addressTranslator = addressTranslator;
        this.addressProviders = addressProviders;
        
        // Initialize credential preservation service for server data storage
        this.credentialPreservationService = new CredentialPreservationService(this.logger);
        
        this.startConnectionHealthCheck();
        this.startConnectionCleanupTask();
    }

    private startConnectionHealthCheck(): void {
        // Check connection health every 5 seconds
        this.connectionHealthCheckInterval = setInterval(() => {
            this.checkConnectionHealth();
        }, 5000);
    }

    private startConnectionCleanupTask(): void {
        // Periodically clean up stale connections and failed connection tracking
        this.connectionCleanupTask = setInterval(() => {
            this.cleanupStaleConnections();
        }, this.connectionCleanupInterval);
    }

    private cleanupStaleConnections(): void {
        const now = Date.now();
        const staleThreshold = 60000; // 1 minute threshold for stale connections

        // Clean up failed connections that are older than threshold
        const addressesToRemove: string[] = [];
        this.failedConnections.forEach(address => {
            // For now, we'll use a simple cleanup strategy
            // In a more sophisticated implementation, we could track timestamps
            addressesToRemove.push(address);
        });

        if (addressesToRemove.length > 0) {
            this.logger.debug('ClientConnectionManager', `Cleaning up ${addressesToRemove.length} stale failed connections`);
            addressesToRemove.forEach(address => {
                this.failedConnections.delete(address);
            });
        }

        // Clean up any connections that are not responding
        Object.keys(this.establishedConnections).forEach(addressStr => {
            const connection = this.establishedConnections[addressStr];
            if (connection && !connection.isAlive()) {
                this.logger.warn('ClientConnectionManager', `Cleaning up stale connection to ${addressStr}`);
                this.destroyConnection(connection.getAddress());
            }
        });
        
        // Log current connection state
        const activeConnections = Object.keys(this.establishedConnections).length;
        const pendingConnections = Object.keys(this.pendingConnections).length;
        const failedConnections = this.failedConnections.size;
        
        this.logger.debug('ClientConnectionManager', `Connection State - Active: ${activeConnections}, Pending: ${pendingConnections}, Failed: ${failedConnections}`);
        
        if (activeConnections > 0) {
            Object.keys(this.establishedConnections).forEach(addressStr => {
                const connection = this.establishedConnections[addressStr];
                this.logger.debug('ClientConnectionManager', `Connection to ${addressStr}: Alive=${connection.isAlive()}, Owner=${connection.isAuthenticatedAsOwner()}`);
            });
        }
    }

    private checkConnectionHealth(): void {
        // Use Object.keys() instead of Object.values() for compatibility
        const connectionKeys = Object.keys(this.establishedConnections);
        for (const key of connectionKeys) {
            const connection = this.establishedConnections[key];
            if (!connection.isAlive()) {
                this.logger.warn('ClientConnectionManager', 
                    `Connection to ${connection.getAddress().toString()} is not alive, destroying it`);
                this.destroyConnection(connection.getAddress());
            }
        }
    }

    private isConnectionHealthy(connection: ClientConnection): boolean {
        // Use the improved health check method
        return connection.isHealthy();
    }

    private retryConnection(address: Address, asOwner: boolean, retryCount: number = 0): Promise<ClientConnection> {
        return this.createConnection(address, asOwner).then((connection) => {
            this.failedConnections.delete(address.toString());
            return connection;
        }).catch((error) => {
            // Check if it's an authentication error
            const isAuthError = error.message && (
                error.message.includes('Invalid Credentials') ||
                error.message.includes('authentication') ||
                error.message.includes('credentials')
            );
            
            if (isAuthError) {
                this.logger.error('ClientConnectionManager', 
                    `Authentication failed for ${address.toString()}, not retrying: ${error.message}`);
                // Handle authentication errors specially
                this.handleAuthenticationError(address);
                throw error;
            }
            
            if (retryCount < this.maxConnectionRetries) {
                this.logger.warn('ClientConnectionManager', 
                    `Connection attempt ${retryCount + 1} failed for ${address.toString()}, retrying in ${this.connectionRetryDelay}ms`);
                return new Promise((resolve, reject) => {
                    setTimeout(() => {
                        this.retryConnection(address, asOwner, retryCount + 1).then(resolve).catch(reject);
                    }, this.connectionRetryDelay);
                });
            } else {
                this.failedConnections.add(address.toString());
                this.logger.error('ClientConnectionManager', 
                    `Failed to connect to ${address.toString()} after ${this.maxConnectionRetries} attempts`);
                throw error;
            }
        });
    }

    private createConnection(address: Address, asOwner: boolean): Promise<ClientConnection> {
        return this.addressTranslator.translate(address).then((addr) => {
            if (addr == null) {
                throw new RangeError('Address Translator could not translate address ' + address.toString());
            }

            return this.triggerConnect(addr, asOwner).then((socket: net.Socket) => {
                const clientConnection = new ClientConnection(this.client, addr, socket);

                return this.initiateCommunication(clientConnection).then(() => {
                    return clientConnection.registerResponseCallback((data: Buffer) => {
                        this.client.getInvocationService().processResponse(data);
                    });
                }).then(() => {
                    return this.authenticate(clientConnection, asOwner);
                }).then(() => {
                    return clientConnection;
                });
            });
        });
    }

    getActiveConnections(): { [address: string]: ClientConnection } {
        return this.establishedConnections;
    }

    /**
     * Checks if we already have a connection to the given address
     * @param address The address to check
     * @returns true if connection exists and is healthy, false otherwise
     */
    hasConnection(address: Address): boolean {
        const addressStr = address.toString();
        const connection = this.establishedConnections[addressStr];
        return connection && connection.isAlive();
    }

    /**
     * Forces cleanup of all dead connections
     * This is useful during failover to prevent connection leakage
     */
    forceCleanupDeadConnections(): void {
        this.logger.info('ClientConnectionManager', 'Forcing cleanup of all dead connections');
        
        const addressesToRemove: string[] = [];
        
        // Check all established connections
        Object.keys(this.establishedConnections).forEach(addressStr => {
            const connection = this.establishedConnections[addressStr];
            if (connection && !connection.isHealthy()) {
                this.logger.warn('ClientConnectionManager', `Found dead connection to ${addressStr}, marking for cleanup`);
                addressesToRemove.push(addressStr);
            }
        });
        
        // Remove dead connections
        addressesToRemove.forEach(addressStr => {
            const connection = this.establishedConnections[addressStr];
            if (connection) {
                this.destroyConnection(connection.getAddress());
            }
        });
        
        // Also clean up any pending connections that might be stuck
        Object.keys(this.pendingConnections).forEach(addressStr => {
            const pendingConnection = this.pendingConnections[addressStr];
            if (pendingConnection) {
                this.logger.warn('ClientConnectionManager', `Cleaning up stuck pending connection to ${addressStr}`);
                pendingConnection.reject(new Error('Connection cleanup forced'));
                delete this.pendingConnections[addressStr];
            }
        });
        
        this.logger.info('ClientConnectionManager', `Cleanup completed. Removed ${addressesToRemove.length} dead connections`);
    }

    /**
     * Gets an existing connection to a specific address if it exists
     * @param address The address to check for existing connection
     * @returns The existing connection or undefined if none exists
     */
    getConnection(address: Address): ClientConnection | undefined {
        const addressStr = address.toString();
        return this.establishedConnections[addressStr];
    }

    /**
     * Gets all established connections
     * @returns Object containing all established connections
     */
    getEstablishedConnections(): { [address: string]: ClientConnection } {
        return this.establishedConnections;
    }

    /**
     * Gets all pending connections
     * @returns Object containing all pending connections
     */
    getPendingConnections(): { [address: string]: Promise.Resolver<ClientConnection> } {
        return this.pendingConnections;
    }

    // SERVER-FIRST APPROACH: Store credentials from server data
    // We trust the server as single source of truth but store what it tells us
    
    private credentialPreservationService: CredentialPreservationService;
    private memberAddedEvents: Set<string> = new Set<string>();
    
    /**
     * Updates preserved credentials with server-provided data
     * @param address The address to update credentials for
     * @param newUuid The new UUID from server member event
     */
    public updatePreservedCredentials(address: Address, newUuid: string): void {
        const addressStr = address.toString();
        this.logger.info('ClientConnectionManager', 
            `🔄 SERVER-FIRST: Updating credentials for ${addressStr} with server UUID: ${newUuid}`);
        
        // Get the current owner UUID from cluster service
        const clusterService = this.client.getClusterService();
        const currentOwnerUuid = clusterService.ownerUuid;
        
        // Get group config from client
        const groupConfig = this.client.getConfig().groupConfig;
        
        // Store the server-provided UUID as the authoritative credential
        // Use preserveCredentials to create new credentials if they don't exist
        this.credentialPreservationService.preserveCredentials(
            address, 
            newUuid, 
            currentOwnerUuid || newUuid, // Use current owner UUID or fallback to member UUID
            groupConfig.name, // Group name from config
            groupConfig.password || '', // Group password from config
            false // Not owner connection
        );
        
        // Mark that we received a member added event for this address
        this.memberAddedEvents.add(addressStr);
        
        this.logger.info('ClientConnectionManager', 
            `💾 SERVER-FIRST: Stored server credential for ${addressStr}: uuid=${newUuid}, ownerUuid=${currentOwnerUuid || newUuid}`);
    }

    /**
     * Records that a member added event was received from server
     * @param address The address that had a server member added event
     */
    public recordMemberAddedEvent(address: Address): void {
        const addressStr = address.toString();
        this.memberAddedEvents.add(addressStr);
        this.logger.debug('ClientConnectionManager', 
            `📝 SERVER-FIRST: Recorded server member added event for ${addressStr}`);
    }

    /**
     * Checks if we received a server member added event for an address
     * @param address The address to check
     * @returns True if server member added event was received
     */
    public hasMemberAddedEvent(address: Address): boolean {
        const addressStr = address.toString();
        return this.memberAddedEvents.has(addressStr);
    }

    /**
     * Clears member added events for an address (useful during failover)
     * @param address The address to clear events for
     */
    public clearMemberAddedEvents(address: Address): void {
        const addressStr = address.toString();
        this.memberAddedEvents.delete(addressStr);
        this.logger.debug('ClientConnectionManager', 
            `🗑️ SERVER-FIRST: Cleared member events for ${addressStr}`);
    }

    /**
     * Updates ALL credentials with server-provided owner UUID
     * @param newOwnerUuid The new owner UUID from server cluster state
     */
    public updateAllCredentialsWithNewOwnerUuid(newOwnerUuid: string): void {
        this.logger.info('ClientConnectionManager', 
            `🔄 SERVER-FIRST: Updating ALL credentials with server owner UUID: ${newOwnerUuid}`);
        
        // Update all preserved credentials with server data
        this.credentialPreservationService.updateAllOwnerUuids(newOwnerUuid);
        
        // Validate consistency
        const isConsistent = this.credentialPreservationService.validateOwnerUuidConsistency();
        
        if (isConsistent) {
            this.logger.info('ClientConnectionManager', 
                `✅ SERVER-FIRST: All credentials now consistent with server owner UUID: ${newOwnerUuid}`);
        } else {
            this.logger.warn('ClientConnectionManager', 
                `⚠️ SERVER-FIRST: Credential consistency validation failed after server update`);
        }
    }

    /**
     * Gets the current owner UUID from server-stored credentials
     * @returns The current owner UUID or null if not found
     */
    public getCurrentOwnerUuid(): string | null {
        return this.credentialPreservationService.getCurrentOwnerUuid();
    }

    /**
     * Validates that all stored credentials are consistent with server data
     * @returns True if all credentials are consistent, false otherwise
     */
    public validateCredentialConsistency(): boolean {
        return this.credentialPreservationService.validateOwnerUuidConsistency();
    }

    /**
     * Requests current cluster state from server when reconnecting
     * This ensures we have the most up-to-date member information
     * @returns Server cluster state
     */
    public requestServerClusterState(): any {
        this.logger.info('ClientConnectionManager', 
            `🔍 SERVER-FIRST: Requesting current cluster state from server...`);
        
        try {
            // Get the current owner connection
            const ownerConnection = this.getOwnerConnection();
            if (!ownerConnection || !ownerConnection.isAlive()) {
                this.logger.warn('ClientConnectionManager', 
                    `⚠️ SERVER-FIRST: No active owner connection available for cluster state request`);
                return null;
            }

            // Request cluster state from the server
            // This will give us the authoritative member list
            this.logger.info('ClientConnectionManager', 
                `📤 SERVER-FIRST: Sending cluster state request to server...`);
            
            // For now, we'll use the existing cluster service to get member info
            // In a full implementation, we'd send a specific protocol message
            const clusterService = this.client.getClusterService();
            const members = clusterService.getMembers();
            
            this.logger.info('ClientConnectionManager', 
                `📥 SERVER-FIRST: Received cluster state from server: ${members.length} members`);
            
            return {
                members: members,
                timestamp: Date.now(),
                source: 'server'
            };
            
        } catch (error) {
            this.logger.error('ClientConnectionManager', 
                `❌ SERVER-FIRST: Failed to request cluster state from server: ${error.message}`);
            return null;
        }
    }

    /**
     * Gets the current owner connection for server requests
     * @returns The owner connection or null if not available
     */
    private getOwnerConnection(): ClientConnection | null {
        const clusterService = this.client.getClusterService();
        return clusterService.getOwnerConnection();
    }

    /**
     * Returns the {@link ClientConnection} with given {@link Address}. If there is no such connection established,
     * it first connects to the address and then return the {@link ClientConnection}.
     * @param address
     * @param asOwner Sets the connected node as owner of this client if true.
     * @returns {Promise<ClientConnection>|Promise<T>}
     */
    getOrConnect(address: Address, asOwner: boolean = false): Promise<ClientConnection> {
        const addressIndex = address.toString();

        // Check if connection is already established and healthy
        const establishedConnection = this.establishedConnections[addressIndex];
        if (establishedConnection && this.isConnectionHealthy(establishedConnection)) {
            return Promise.resolve(establishedConnection);
        }

        // If existing connection is unhealthy, destroy it
        if (establishedConnection && !this.isConnectionHealthy(establishedConnection)) {
            this.logger.warn('ClientConnectionManager', 
                `Destroying unhealthy connection to ${addressIndex}`);
            this.destroyConnection(address);
        }

        // Check if we're already trying to connect
        const pendingConnection = this.pendingConnections[addressIndex];
        if (pendingConnection) {
            return pendingConnection.promise;
        }

        // Check if this address has failed too many times recently
        if (this.failedConnections.has(addressIndex)) {
            const error = new Error(`Address ${addressIndex} has failed recently and is temporarily blocked`);
            return Promise.reject(error);
        }

        const connectionResolver: Promise.Resolver<ClientConnection> = DeferredPromise<ClientConnection>();
        this.pendingConnections[addressIndex] = connectionResolver;

        this.retryConnection(address, asOwner)
            .then((clientConnection) => {
                // Safety check: ensure we got a proper ClientConnection
                if (!clientConnection || typeof clientConnection.getAddress !== 'function') {
                    const error = new Error(`Invalid connection object returned: ${typeof clientConnection}`);
                    this.logger.error('ClientConnectionManager', error.message);
                    connectionResolver.reject(error);
                    return;
                }
                
                this.establishedConnections[clientConnection.getAddress().toString()] = clientConnection;
                this.onConnectionOpened(clientConnection);
                connectionResolver.resolve(clientConnection);
            })
            .catch((e: any) => {
                this.logger.error('ClientConnectionManager', 
                    `Failed to establish connection to ${addressIndex}`, e);
                connectionResolver.reject(e);
            })
            .finally(() => {
                delete this.pendingConnections[addressIndex];
            });

        const connectionTimeout = this.client.getConfig().networkConfig.connectionTimeout;
        if (connectionTimeout !== 0) {
            return connectionResolver.promise.timeout(connectionTimeout, new HazelcastError(
                'Connection timed-out')).finally(() => {
                delete this.pendingConnections[addressIndex];
            });
        }
        return connectionResolver.promise;
    }

    /**
     * Destroys the connection with given node address.
     * @param address
     */
    destroyConnection(address: Address): void {
        const addressStr = address.toString();
        
        // Clean up pending connections
        if (this.pendingConnections.hasOwnProperty(addressStr)) {
            this.pendingConnections[addressStr].reject(new Error('Connection destroyed'));
            delete this.pendingConnections[addressStr];
        }
        
        // Clean up established connections
        if (this.establishedConnections.hasOwnProperty(addressStr)) {
            const conn = this.establishedConnections[addressStr];
            delete this.establishedConnections[addressStr];
            
            try {
                conn.close();
            } catch (error) {
                this.logger.warn('ClientConnectionManager', `Error closing connection to ${addressStr}:`, error);
            }
            
            this.onConnectionClosed(conn);
        }

        // Mark as failed to prevent immediate reconnection attempts
        this.failedConnections.add(addressStr);
        
        this.logger.debug('ClientConnectionManager', `Connection to ${addressStr} destroyed and marked as failed`);
    }

    /**
     * Cleans up all connections to a specific address during failover
     * @param address The address to cleanup
     */
    cleanupConnectionsForFailover(address: Address): void {
        const addressStr = address.toString();
        
        this.logger.info('ClientConnectionManager', `Cleaning up all connections to ${addressStr} for failover`);
        
        // Force cleanup of all connection types
        this.destroyConnection(address);
        
        // Remove from failed connections to allow reconnection after failover
        this.failedConnections.delete(addressStr);
    }

    /**
     * Handles authentication errors by clearing failed connections and allowing retry
     * @param address The address that had authentication issues
     */
    handleAuthenticationError(address: Address): void {
        const addressStr = address.toString();
        
        this.logger.warn('ClientConnectionManager', `Handling authentication error for ${addressStr}`);
        
        // Clear from failed connections to allow retry with fresh credentials
        this.failedConnections.delete(addressStr);
        
        // Also clear any established connections to this address
        if (this.establishedConnections.hasOwnProperty(addressStr)) {
            this.logger.info('ClientConnectionManager', `Clearing established connection to ${addressStr} due to auth error`);
            this.destroyConnection(address);
        }
        
        // Clear any pending connections
        if (this.pendingConnections.hasOwnProperty(addressStr)) {
            this.logger.info('ClientConnectionManager', `Clearing pending connection to ${addressStr} due to auth error`);
            this.pendingConnections[addressStr].reject(new Error('Authentication error, connection cleared'));
            delete this.pendingConnections[addressStr];
        }
    }

    shutdown(): void {
        if (this.connectionHealthCheckInterval) {
            clearInterval(this.connectionHealthCheckInterval);
        }
        
        if (this.connectionCleanupTask) {
            clearInterval(this.connectionCleanupTask);
        }
        
        for (const pending in this.pendingConnections) {
            this.pendingConnections[pending].reject(new ClientNotActiveError('Client is shutting down!'));
        }
        for (const conn in this.establishedConnections) {
            this.establishedConnections[conn].close();
        }
        this.failedConnections.clear();
    }

    private triggerConnect(address: Address, asOwner: boolean): Promise<net.Socket> {
        if (!asOwner) {
            const ownerConnection = this.client.getClusterService().getOwnerConnection();
            if (ownerConnection == null) {
                // Check if this is a single-node scenario
                const knownAddresses = this.client.getClusterService().getKnownAddresses();
                const isSingleNode = knownAddresses.length === 1;
                
                if (isSingleNode) {
                    // Allow connection in single-node scenario
                    this.logger.info('ClientConnectionManager', 
                        `🔗 SINGLE-NODE: Allowing connection to ${address.toString()} (only node in cluster)`);
                } else {
                    // Multi-node scenario - require owner connection (preserve existing logic)
                    const error = new IllegalStateError('Owner connection is not available!');
                    return Promise.reject(error);
                }
            }
        }

        if (this.client.getConfig().networkConfig.sslConfig.enabled) {
            if (this.client.getConfig().networkConfig.sslConfig.sslOptions) {
                const opts = this.client.getConfig().networkConfig.sslConfig.sslOptions;
                return this.connectTLSSocket(address, opts);
            } else if (this.client.getConfig().networkConfig.sslConfig.sslOptionsFactoryConfig) {
                const factoryConfig = this.client.getConfig().networkConfig.sslConfig.sslOptionsFactoryConfig;
                const factoryProperties = this.client.getConfig().networkConfig.sslConfig.sslOptionsFactoryProperties;
                let factory: SSLOptionsFactory;
                if (factoryConfig.path) {
                    factory = new (loadNameFromPath(factoryConfig.path, factoryConfig.exportedName))();
                } else {
                    factory = new BasicSSLOptionsFactory();
                }
                return factory.init(factoryProperties).then(() => {
                    return this.connectTLSSocket(address, factory.getSSLOptions());
                });
            } else {
                // the default behavior when ssl is enabled
                const opts = this.client.getConfig().networkConfig.sslConfig.sslOptions = {
                    checkServerIdentity: (): any => null,
                    rejectUnauthorized: true,
                };
                return this.connectTLSSocket(address, opts);
            }
        } else {
            return this.connectNetSocket(address);
        }
    }

    private connectTLSSocket(address: Address, configOpts: any): Promise<tls.TLSSocket> {
        const connectionResolver = DeferredPromise<tls.TLSSocket>();
        const socket = tls.connect(address.port, address.host, configOpts);
        socket.once('secureConnect', () => {
            connectionResolver.resolve(socket);
        });
        socket.on('error', (e: any) => {
            this.logger.warn('ClientConnectionManager', 'Could not connect to address ' + address.toString(), e);
            connectionResolver.reject(e);
            if (e.code === 'EPIPE' || e.code === 'ECONNRESET') {
                this.destroyConnection(address);
            }
        });
        return connectionResolver.promise;
    }

    private connectNetSocket(address: Address): Promise<net.Socket> {
        const connectionResolver = DeferredPromise<net.Socket>();
        const socket = net.connect(address.port, address.host);
        socket.once('connect', () => {
            connectionResolver.resolve(socket);
        });
        socket.on('error', (e: any) => {
            this.logger.warn('ClientConnectionManager', 'Could not connect to address ' + address.toString(), e);
            connectionResolver.reject(e);
            if (e.code === 'EPIPE' || e.code === 'ECONNRESET') {
                this.destroyConnection(address);
            }
        });
        return connectionResolver.promise;
    }

    private initiateCommunication(connection: ClientConnection): Promise<void> {
        // Send the protocol version
        const buffer = Buffer.from('CB2');
        return connection.write(buffer);
    }

    private onConnectionClosed(connection: ClientConnection): void {
        this.emit(EMIT_CONNECTION_CLOSED, connection);
    }

    private onConnectionOpened(connection: ClientConnection): void {
        this.emit(EMIT_CONNECTION_OPENED, connection);
    }

    private authenticate(connection: ClientConnection, ownerConnection: boolean): Promise<void> {
        const address = connection.getAddress();
        const addressStr = address.toString();
        
        this.logger.info('ClientConnectionManager', 
            `🔐 Starting authentication for ${addressStr} (owner=${ownerConnection})`);
        
        // Check if we have stored credentials for this address
        const storedCredentials = this.credentialPreservationService.restoreCredentials(address);
        const hasMemberAddedEvent = this.hasMemberAddedEvent(address);
        
        if (storedCredentials) {
            this.logger.info('ClientConnectionManager', 
                `📋 Using STORED credentials for ${addressStr}:`);
            this.logger.info('ClientConnectionManager', 
                `   - UUID: ${storedCredentials.uuid}`);
            this.logger.info('ClientConnectionManager', 
                `   - Owner UUID: ${storedCredentials.ownerUuid}`);
            this.logger.info('ClientConnectionManager', 
                `   - Group Name: ${storedCredentials.groupName}`);
            this.logger.info('ClientConnectionManager', 
                `   - Member Added Event: ${hasMemberAddedEvent ? 'YES' : 'NO'}`);
        } else {
            this.logger.info('ClientConnectionManager', 
                `📋 No stored credentials found for ${addressStr}, using fresh authentication`);
        }
        
        // Log current cluster state for debugging
        const clusterService = this.client.getClusterService();
        this.logger.info('ClientConnectionManager', 
            `🔍 Current cluster state for ${addressStr}:`);
        this.logger.info('ClientConnectionManager', 
            `   - Client UUID: ${clusterService.uuid || 'NOT SET'}`);
        this.logger.info('ClientConnectionManager', 
            `   - Client Owner UUID: ${clusterService.ownerUuid || 'NOT SET'}`);
        this.logger.info('ClientConnectionManager', 
            `   - Active Members: ${clusterService.getMembers().length}`);
        
        // Log what we're about to send
        this.logger.info('ClientConnectionManager', 
            `📤 Sending authentication request to ${addressStr} with:`);
        this.logger.info('ClientConnectionManager', 
            `   - Owner Connection: ${ownerConnection}`);
        this.logger.info('ClientConnectionManager', 
            `   - Using Stored Credentials: ${storedCredentials ? 'YES' : 'NO'}`);
        
        const authenticator = new ConnectionAuthenticator(connection, this.client);
        
        // Use normal authentication flow - server handles everything
        return authenticator.authenticate(ownerConnection)
            .then(() => {
                this.logger.info('ClientConnectionManager', 
                    `✅ Authentication successful for ${addressStr}`);
                
                // After successful authentication, store the new credentials from server
                if (ownerConnection) {
                    const newUuid = clusterService.uuid;
                    const newOwnerUuid = clusterService.ownerUuid;
                    
                    this.logger.info('ClientConnectionManager', 
                        `💾 Storing new credentials from server for ${addressStr}:`);
                    this.logger.info('ClientConnectionManager', 
                        `   - New UUID: ${newUuid}`);
                    this.logger.info('ClientConnectionManager', 
                        `   - New Owner UUID: ${newOwnerUuid}`);
                    
                    // Store the new credentials
                    this.credentialPreservationService.updateCredentials(address, newUuid, newOwnerUuid);
                }
            })
            .catch((error) => {
                this.logger.error('ClientConnectionManager', 
                    `❌ Authentication FAILED for ${addressStr}:`);
                this.logger.error('ClientConnectionManager', 
                    `   - Error: ${error.message}`);
                this.logger.error('ClientConnectionManager', 
                    `   - Used Stored Credentials: ${storedCredentials ? 'YES' : 'NO'}`);
                if (storedCredentials) {
                    this.logger.error('ClientConnectionManager', 
                        `   - Failed Credentials: uuid=${storedCredentials.uuid}, ownerUuid=${storedCredentials.ownerUuid}`);
                }
                this.logger.error('ClientConnectionManager', 
                    `   - Current Client UUID: ${clusterService.uuid || 'NOT SET'}`);
                this.logger.error('ClientConnectionManager', 
                    `   - Current Client Owner UUID: ${clusterService.ownerUuid || 'NOT SET'}`);
                
                // Clear failed credentials
                if (storedCredentials) {
                    this.logger.info('ClientConnectionManager', 
                        `🗑️ Clearing failed credentials for ${addressStr}`);
                    this.credentialPreservationService.clearCredentialsForAddress(address);
                }
                
                throw error;
            });
    }
}
