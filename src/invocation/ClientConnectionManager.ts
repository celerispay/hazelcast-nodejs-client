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

    constructor(client: HazelcastClient, addressTranslator: AddressTranslator, addressProviders: AddressProvider[]) {
        super();
        this.client = client;
        this.logger = this.client.getLoggingService().getLogger();
        this.addressTranslator = addressTranslator;
        this.addressProviders = addressProviders;
        this.startConnectionHealthCheck();
    }

    private startConnectionHealthCheck(): void {
        // Check connection health every 5 seconds
        this.connectionHealthCheckInterval = setInterval(() => {
            this.checkConnectionHealth();
        }, 5000);
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
        // Only check if connection is alive - isAuthenticated() method doesn't exist
        return connection.isAlive();
    }

    private retryConnection(address: Address, asOwner: boolean, retryCount: number = 0): Promise<ClientConnection> {
        return this.createConnection(address, asOwner).then((connection) => {
            this.failedConnections.delete(address.toString());
            return connection;
        }).catch((error) => {
            if (retryCount < this.maxConnectionRetries) {
                this.logger.warn('ClientConnectionManager', 
                    `Connection attempt ${retryCount + 1} failed for ${address.toString()}, retrying in ${this.connectionRetryDelay}ms`);
                return new Promise((resolve) => {
                    setTimeout(() => {
                        this.retryConnection(address, asOwner, retryCount + 1).then(resolve).catch(resolve);
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
        if (this.pendingConnections.hasOwnProperty(addressStr)) {
            this.pendingConnections[addressStr].reject(null);
        }
        if (this.establishedConnections.hasOwnProperty(addressStr)) {
            const conn = this.establishedConnections[addressStr];
            delete this.establishedConnections[addressStr];
            conn.close();
            this.onConnectionClosed(conn);
        }
    }

    shutdown(): void {
        if (this.connectionHealthCheckInterval) {
            clearInterval(this.connectionHealthCheckInterval);
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
            if (this.client.getClusterService().getOwnerConnection() == null) {
                const error = new IllegalStateError('Owner connection is not available!');
                return Promise.reject(error);
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
        const authenticator = new ConnectionAuthenticator(connection, this.client);
        return authenticator.authenticate(ownerConnection);
    }
}
