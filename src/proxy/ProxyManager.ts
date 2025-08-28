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
import {ClientAddDistributedObjectListenerCodec} from '../codec/ClientAddDistributedObjectListenerCodec';
import {ClientCreateProxyCodec} from '../codec/ClientCreateProxyCodec';
import {ClientDestroyProxyCodec} from '../codec/ClientDestroyProxyCodec';
import {ClientRemoveDistributedObjectListenerCodec} from '../codec/ClientRemoveDistributedObjectListenerCodec';
import {Member} from '../core/Member';
import {DistributedObject} from '../DistributedObject';
import HazelcastClient from '../HazelcastClient';
import {ClientNotActiveError, HazelcastError} from '../HazelcastError';
import {Invocation} from '../invocation/InvocationService';
import {ListenerMessageCodec} from '../ListenerMessageCodec';
import {AtomicLongProxy} from './AtomicLongProxy';
import {FlakeIdGeneratorProxy} from './FlakeIdGeneratorProxy';
import {ListProxy} from './ListProxy';
import {LockProxy} from './LockProxy';
import {MapProxy} from './MapProxy';
import {MultiMapProxy} from './MultiMapProxy';
import {NearCachedMapProxy} from './NearCachedMapProxy';
import {PNCounterProxy} from './PNCounterProxy';
import {QueueProxy} from './QueueProxy';
import {ReplicatedMapProxy} from './ReplicatedMapProxy';
import {RingbufferProxy} from './ringbuffer/RingbufferProxy';
import {SemaphoreProxy} from './SemaphoreProxy';
import {SetProxy} from './SetProxy';
import {ReliableTopicProxy} from './topic/ReliableTopicProxy';
import {DistributedObjectEvent, DistributedObjectListener} from '../core/DistributedObjectListener';
import {DeferredPromise} from '../Util';
import {ILogger} from '../logging/ILogger';
import Address = require('../Address');
import ClientMessage = require('../ClientMessage');

export class ProxyManager {
    public static readonly MAP_SERVICE: string = 'hz:impl:mapService';
    public static readonly SET_SERVICE: string = 'hz:impl:setService';
    public static readonly LOCK_SERVICE: string = 'hz:impl:lockService';
    public static readonly QUEUE_SERVICE: string = 'hz:impl:queueService';
    public static readonly LIST_SERVICE: string = 'hz:impl:listService';
    public static readonly MULTIMAP_SERVICE: string = 'hz:impl:multiMapService';
    public static readonly RINGBUFFER_SERVICE: string = 'hz:impl:ringbufferService';
    public static readonly REPLICATEDMAP_SERVICE: string = 'hz:impl:replicatedMapService';
    public static readonly SEMAPHORE_SERVICE: string = 'hz:impl:semaphoreService';
    public static readonly ATOMICLONG_SERVICE: string = 'hz:impl:atomicLongService';
    public static readonly FLAKEID_SERVICE: string = 'hz:impl:flakeIdGeneratorService';
    public static readonly PNCOUNTER_SERVICE: string = 'hz:impl:PNCounterService';
    public static readonly RELIABLETOPIC_SERVICE: string = 'hz:impl:reliableTopicService';

    public readonly service: { [serviceName: string]: any } = {};
    private readonly proxies: { [namespace: string]: Promise<DistributedObject>; } = {};
    private readonly client: HazelcastClient;
    private readonly logger: ILogger;
    private readonly invocationTimeoutMillis: number;
    private readonly invocationRetryPauseMillis: number;

    constructor(client: HazelcastClient) {
        this.client = client;
        this.logger = this.client.getLoggingService().getLogger();
        this.invocationTimeoutMillis = this.client.getInvocationService().getInvocationTimeoutMillis();
        this.invocationRetryPauseMillis = this.client.getInvocationService().getInvocationRetryPauseMillis();
    }

    public init(): void {
        this.service[ProxyManager.MAP_SERVICE] = MapProxy;
        this.service[ProxyManager.SET_SERVICE] = SetProxy;
        this.service[ProxyManager.QUEUE_SERVICE] = QueueProxy;
        this.service[ProxyManager.LIST_SERVICE] = ListProxy;
        this.service[ProxyManager.LOCK_SERVICE] = LockProxy;
        this.service[ProxyManager.MULTIMAP_SERVICE] = MultiMapProxy;
        this.service[ProxyManager.RINGBUFFER_SERVICE] = RingbufferProxy;
        this.service[ProxyManager.REPLICATEDMAP_SERVICE] = ReplicatedMapProxy;
        this.service[ProxyManager.SEMAPHORE_SERVICE] = SemaphoreProxy;
        this.service[ProxyManager.ATOMICLONG_SERVICE] = AtomicLongProxy;
        this.service[ProxyManager.FLAKEID_SERVICE] = FlakeIdGeneratorProxy;
        this.service[ProxyManager.PNCOUNTER_SERVICE] = PNCounterProxy;
        this.service[ProxyManager.RELIABLETOPIC_SERVICE] = ReliableTopicProxy;
    }

    public getOrCreateProxy(name: string, serviceName: string, createAtServer = true): Promise<DistributedObject> {
        const fullName = serviceName + name;
        if (this.proxies[fullName]) {
            return this.proxies[fullName];
        }

        // Check if cluster is healthy before creating proxy
        if (!this.isClusterHealthy()) {
            const error = new Error('Cluster is not healthy, cannot create proxy for ' + name);
            this.logger.error('ProxyManager', error.message);
            return Promise.reject(error);
        }

        const deferred = DeferredPromise<DistributedObject>();
        let newProxy: DistributedObject;
        if (serviceName === ProxyManager.MAP_SERVICE && this.client.getConfig().getNearCacheConfig(name)) {
            newProxy = new NearCachedMapProxy(this.client, serviceName, name);
        } else if (serviceName === ProxyManager.RELIABLETOPIC_SERVICE) {
            newProxy = new ReliableTopicProxy(this.client, serviceName, name);
            if (createAtServer) {
                (newProxy as ReliableTopicProxy<any>).setRingbuffer().then(() => {
                    return this.createProxy(newProxy);
                }).then(function (): void {
                    deferred.resolve(newProxy);
                });
            }
            this.proxies[fullName] = deferred.promise;
            return deferred.promise;
        } else {
            newProxy = new this.service[serviceName](this.client, serviceName, name);
        }

        if (createAtServer) {
            this.createProxy(newProxy).then(function (): void {
                deferred.resolve(newProxy);
            }).catch((error) => {
                this.logger.error('ProxyManager', 'Failed to create proxy for ' + name + ': ' + error);
                deferred.reject(error);
            });
        }

        this.proxies[fullName] = deferred.promise;
        return deferred.promise;
    }

    destroyProxy(name: string, serviceName: string): Promise<void> {
        delete this.proxies[serviceName + name];
        const clientMessage = ClientDestroyProxyCodec.encodeRequest(name, serviceName);
        clientMessage.setPartitionId(-1);
        return this.client.getInvocationService().invokeOnRandomTarget(clientMessage).return();
    }

    addDistributedObjectListener(distributedObjectListener: DistributedObjectListener): Promise<string> {
        const handler = function (clientMessage: ClientMessage): void {
            const converterFunc = (objectName: string, serviceName: string, eventType: string) => {
                eventType = eventType.toLowerCase();
                const distributedObjectEvent = new DistributedObjectEvent(eventType, serviceName, objectName);
                distributedObjectListener(distributedObjectEvent);
            };
            ClientAddDistributedObjectListenerCodec.handle(clientMessage, converterFunc, null);
        };
        const codec = this.createDistributedObjectListener();
        return this.client.getListenerService().registerListener(codec, handler);
    }

    removeDistributedObjectListener(listenerId: string): Promise<boolean> {
        return this.client.getListenerService().deregisterListener(listenerId);
    }

    protected isRetryable(error: HazelcastError): boolean {
        if (error instanceof ClientNotActiveError) {
            return false;
        }
        return true;
    }

    private createProxy(proxyObject: DistributedObject): Promise<ClientMessage> {
        const promise = DeferredPromise<ClientMessage>();
        this.initializeProxy(proxyObject, promise, Date.now() + this.invocationTimeoutMillis);
        return promise.promise;
    }

    private findNextAddress(): Address {
        const members = this.client.getClusterService().getMembers();
        
        // If no members available, return null but log the issue
        if (!members || members.length === 0) {
            this.logger.warn('ProxyManager', 'No cluster members available for proxy creation');
            return null;
        }
        
        let liteMember: Member = null;
        let dataMember: Member = null;
        
        for (const member of members) {
            if (member != null && member.isLiteMember === false) {
                dataMember = member;
                break; // Prefer data members
            } else if (member != null && member.isLiteMember) {
                liteMember = member;
            }
        }

        // Return data member if available, otherwise lite member, otherwise null
        if (dataMember != null) {
            return dataMember.address;
        } else if (liteMember != null) {
            return liteMember.address;
        } else {
            this.logger.warn('ProxyManager', 'No valid members found for proxy creation');
            return null;
        }
    }

    private initializeProxy(proxyObject: DistributedObject, promise: Promise.Resolver<ClientMessage>, deadline: number): void {
        if (Date.now() > deadline) {
            const error = new Error('Create proxy request timed-out for ' + proxyObject.getName());
            this.logger.error('ProxyManager', error.message);
            promise.reject(error);
            return;
        }
        
        const address: Address = this.findNextAddress();
        if (!address) {
            const error = new Error('No cluster members available for proxy creation: ' + proxyObject.getName());
            this.logger.error('ProxyManager', error.message);
            promise.reject(error);
            return;
        }
        
        const request = ClientCreateProxyCodec.encodeRequest(proxyObject.getName(), proxyObject.getServiceName(), address);
        const invocation = new Invocation(this.client, request);
        invocation.address = address;
        
        this.client.getInvocationService().invoke(invocation).then((response) => {
            promise.resolve(response);
        }).catch((error) => {
            if (this.isRetryable(error)) {
                this.logger.warn('ProxyManager', 'Create proxy request for ' + proxyObject.getName() +
                    ' failed. Retrying in ' + this.invocationRetryPauseMillis + 'ms. ' + error);
                setTimeout(() => {
                    this.initializeProxy(proxyObject, promise, deadline);
                }, this.invocationRetryPauseMillis);
            } else {
                this.logger.error('ProxyManager', 'Create proxy request for ' + proxyObject.getName() + ' failed ' + error);
                promise.reject(error);
            }
        });
    }

    private createDistributedObjectListener(): ListenerMessageCodec {
        return {
            encodeAddRequest(localOnly: boolean): ClientMessage {
                return ClientAddDistributedObjectListenerCodec.encodeRequest(localOnly);
            },
            decodeAddResponse(msg: ClientMessage): string {
                return ClientAddDistributedObjectListenerCodec.decodeResponse(msg).response;
            },
            encodeRemoveRequest(listenerId: string): ClientMessage {
                return ClientRemoveDistributedObjectListenerCodec.encodeRequest(listenerId);
            },
        };
    }

    /**
     * Checks if the cluster is healthy enough to create proxies
     */
    private isClusterHealthy(): boolean {
        const members = this.client.getClusterService().getMembers();
        const hasMembers = members && members.length > 0;
        
        if (!hasMembers) {
            this.logger.warn('ProxyManager', 'No cluster members available');
            return false;
        }
        
        // Check if we have at least one data member
        const hasDataMember = members.some(member => member && !member.isLiteMember);
        if (!hasDataMember) {
            this.logger.warn('ProxyManager', 'No data members available in cluster');
            return false;
        }
        
        // Check if we have an owner connection
        const ownerConnection = this.client.getClusterService().getOwnerConnection();
        if (!ownerConnection || !ownerConnection.isHealthy()) {
            this.logger.warn('ProxyManager', 'No healthy owner connection available');
            return false;
        }
        
        return true;
    }
}
