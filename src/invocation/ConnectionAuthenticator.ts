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
import {ClientAuthenticationCodec} from '../codec/ClientAuthenticationCodec';
import HazelcastClient from '../HazelcastClient';
import {ClientAuthenticationCustomCodec} from '../codec/ClientAuthenticationCustomCodec';
import {ClientConnection} from './ClientConnection';
import {ClusterService} from './ClusterService';
import {AuthenticationError} from '../HazelcastError';
import {ILogger} from '../logging/ILogger';
import ClientMessage = require('../ClientMessage');
import {BuildInfo} from '../BuildInfo';

const enum AuthenticationStatus {
    AUTHENTICATED = 0,
    CREDENTIALS_FAILED = 1,
    SERIALIZATION_VERSION_MISMATCH = 2,
}

export class ConnectionAuthenticator {

    private connection: ClientConnection;
    private client: HazelcastClient;
    private clusterService: ClusterService;
    private logger: ILogger;

    constructor(connection: ClientConnection, client: HazelcastClient) {
        this.connection = connection;
        this.client = client;
        this.logger = this.client.getLoggingService().getLogger();
        this.clusterService = this.client.getClusterService();
    }

    authenticate(asOwner: boolean): Promise<void> {
        const addressStr = this.connection.getAddress().toString();
        
        this.logger.info('ConnectionAuthenticator', 
            `🔐 Starting authentication for ${addressStr} (asOwner=${asOwner})`);
        
        const credentials: ClientMessage = this.createCredentials(asOwner);
        
        this.logger.info('ConnectionAuthenticator', 
            `📤 Sending authentication request to ${addressStr}...`);
        
        return this.client.getInvocationService()
            .invokeOnConnection(this.connection, credentials)
            .then((msg: ClientMessage) => {
                this.logger.info('ConnectionAuthenticator', 
                    `📥 Received authentication response from ${addressStr}`);
                
                const authResponse = ClientAuthenticationCodec.decodeResponse(msg);
                
                this.logger.info('ConnectionAuthenticator', 
                    `🔍 Authentication response for ${addressStr}:`);
                this.logger.info('ConnectionAuthenticator', 
                    `   - Status: ${authResponse.status} (${this.getStatusDescription(authResponse.status)})`);
                this.logger.info('ConnectionAuthenticator', 
                    `   - Server UUID: ${authResponse.uuid || 'NOT PROVIDED'}`);
                this.logger.info('ConnectionAuthenticator', 
                    `   - Server Owner UUID: ${authResponse.ownerUuid || 'NOT PROVIDED'}`);
                this.logger.info('ConnectionAuthenticator', 
                    `   - Server Address: ${authResponse.address ? authResponse.address.toString() : 'NOT PROVIDED'}`);
                this.logger.info('ConnectionAuthenticator', 
                    `   - Server Version: ${authResponse.serverHazelcastVersion || 'NOT PROVIDED'}`);
                
                switch (authResponse.status) {
                    case AuthenticationStatus.AUTHENTICATED:
                        this.logger.info('ConnectionAuthenticator', 
                            `✅ Authentication SUCCESSFUL for ${addressStr}`);
                        
                        this.connection.setAddress(authResponse.address);
                        this.connection.setConnectedServerVersion(authResponse.serverHazelcastVersion);
                        
                        if (asOwner) {
                            const oldUuid = this.clusterService.uuid;
                            const oldOwnerUuid = this.clusterService.ownerUuid;
                            
                            this.clusterService.uuid = authResponse.uuid;
                            this.clusterService.ownerUuid = authResponse.ownerUuid;
                            
                            this.logger.info('ConnectionAuthenticator', 
                                `🔄 Updated cluster service for ${addressStr}:`);
                            this.logger.info('ConnectionAuthenticator', 
                                `   - UUID: ${oldUuid || 'NOT SET'} → ${authResponse.uuid}`);
                            this.logger.info('ConnectionAuthenticator', 
                                `   - Owner UUID: ${oldOwnerUuid || 'NOT SET'} → ${authResponse.ownerUuid}`);
                        }
                        
                        this.logger.info('ConnectionAuthenticator',
                            `✅ Connection to ${addressStr} authenticated successfully`);
                        break;
                        
                    case AuthenticationStatus.CREDENTIALS_FAILED:
                        this.logger.error('ConnectionAuthenticator', 
                            `❌ Authentication FAILED for ${addressStr}: Invalid Credentials`);
                        this.logger.error('ConnectionAuthenticator', 
                            `   - Server rejected our credentials`);
                        this.logger.error('ConnectionAuthenticator', 
                            `   - Check if UUIDs and group credentials are correct`);
                        throw new Error('Invalid Credentials, could not authenticate connection to ' + addressStr);
                        
                    case AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH:
                        this.logger.error('ConnectionAuthenticator', 
                            `❌ Authentication FAILED for ${addressStr}: Serialization version mismatch`);
                        throw new Error('Serialization version mismatch, could not authenticate connection to ' + addressStr);
                        
                    default:
                        this.logger.error('ConnectionAuthenticator', 
                            `❌ Authentication FAILED for ${addressStr}: Unknown status ${authResponse.status}`);
                        throw new AuthenticationError('Unknown authentication status: ' + authResponse.status +
                            ' , could not authenticate connection to ' + addressStr);
                }
            })
            .catch((error) => {
                this.logger.error('ConnectionAuthenticator', 
                    `💥 Authentication ERROR for ${addressStr}: ${error.message}`);
                throw error;
            });
    }

    /**
     * Gets a human-readable description of authentication status
     */
    private getStatusDescription(status: number): string {
        switch (status) {
            case AuthenticationStatus.AUTHENTICATED: return 'AUTHENTICATED';
            case AuthenticationStatus.CREDENTIALS_FAILED: return 'CREDENTIALS_FAILED';
            case AuthenticationStatus.SERIALIZATION_VERSION_MISMATCH: return 'SERIALIZATION_VERSION_MISMATCH';
            default: return `UNKNOWN_STATUS_${status}`;
        }
    }

    /**
     * Creates credentials with optional restoration from preservation service
     * @param asOwner Whether this is an owner connection
     * @param preservedCredentials Optional preserved credentials to use
     * @returns The credentials message
     */
    createCredentialsWithRestoration(asOwner: boolean, preservedCredentials?: any): ClientMessage {
        if (preservedCredentials && preservedCredentials.uuid && preservedCredentials.ownerUuid) {
            this.logger.debug('ConnectionAuthenticator', 
                `Using preserved credentials: uuid=${preservedCredentials.uuid}, ownerUuid=${preservedCredentials.ownerUuid}`);
            
            // Use preserved credentials instead of current cluster state
            const groupConfig = this.client.getConfig().groupConfig;
            const customCredentials = this.client.getConfig().customCredentials;
            let clientMessage: ClientMessage;
            const clientVersion = BuildInfo.getClientVersion();

            if (customCredentials != null) {
                const credentialsPayload = this.client.getSerializationService().toData(customCredentials);
                clientMessage = ClientAuthenticationCustomCodec.encodeRequest(
                    credentialsPayload, preservedCredentials.uuid, preservedCredentials.ownerUuid, asOwner, 'NJS', 1, clientVersion);
            } else {
                clientMessage = ClientAuthenticationCodec.encodeRequest(
                    preservedCredentials.groupName, preservedCredentials.groupPassword, 
                    preservedCredentials.uuid, preservedCredentials.ownerUuid, asOwner, 'NJS', 1, clientVersion);
            }
            
            return clientMessage;
        }
        
        // Fall back to normal credential creation
        return this.createCredentials(asOwner);
    }

    createCredentials(asOwner: boolean): ClientMessage {
        const groupConfig = this.client.getConfig().groupConfig;
        const uuid: string = this.clusterService.uuid;
        const ownerUuid: string = this.clusterService.ownerUuid;

        const customCredentials = this.client.getConfig().customCredentials;

        // LOG EXACTLY WHAT CREDENTIALS WE'RE SENDING
        this.logger.info('ConnectionAuthenticator', 
            `🔐 Creating authentication credentials for ${this.connection.getAddress().toString()}:`);
        this.logger.info('ConnectionAuthenticator', 
            `   - As Owner: ${asOwner}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - UUID: ${uuid || 'NOT SET'}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - Owner UUID: ${ownerUuid || 'NOT SET'}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - Group Name: ${groupConfig.name || 'NOT SET'}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - Group Password: ${groupConfig.password ? '***SET***' : 'NOT SET'}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - Custom Credentials: ${customCredentials ? 'YES' : 'NO'}`);
        this.logger.info('ConnectionAuthenticator', 
            `   - Client Version: ${BuildInfo.getClientVersion()}`);

        let clientMessage: ClientMessage;

        const clientVersion = BuildInfo.getClientVersion();

        if (customCredentials != null) {
            const credentialsPayload = this.client.getSerializationService().toData(customCredentials);

            this.logger.info('ConnectionAuthenticator', 
                `📤 Sending CUSTOM authentication request with:`);
            this.logger.info('ConnectionAuthenticator', 
                `   - Custom Credentials: ${typeof credentialsPayload}`);
            this.logger.info('ConnectionAuthenticator', 
                `   - UUID: ${uuid || 'NOT SET'}`);
            this.logger.info('ConnectionAuthenticator', 
                `   - Owner UUID: ${ownerUuid || 'NOT SET'}`);

            clientMessage = ClientAuthenticationCustomCodec.encodeRequest(
                credentialsPayload, uuid, ownerUuid, asOwner, 'NJS', 1, clientVersion);
        } else {
            this.logger.info('ConnectionAuthenticator', 
                `📤 Sending STANDARD authentication request with:`);
            this.logger.info('ConnectionAuthenticator', 
                `   - Group Name: ${groupConfig.name || 'NOT SET'}`);
            this.logger.info('ConnectionAuthenticator', 
                `   - Group Password: ${groupConfig.password ? '***SET***' : 'NOT SET'}`);
            this.logger.info('ConnectionAuthenticator', 
                `   - UUID: ${uuid || 'NOT SET'}`);
            this.logger.info('ConnectionAuthenticator', 
                `   - Owner UUID: ${ownerUuid || 'NOT SET'}`);

            clientMessage = ClientAuthenticationCodec.encodeRequest(
                groupConfig.name, groupConfig.password, uuid, ownerUuid, asOwner, 'NJS', 1, clientVersion);

        }

        this.logger.info('ConnectionAuthenticator', 
            `📋 Final authentication message created and ready to send`);
        
        return clientMessage;
    }
}
