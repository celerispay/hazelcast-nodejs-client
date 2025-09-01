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
import {ClientConnection} from './ClientConnection';
import Address = require('../Address');
import HazelcastClient from '../HazelcastClient';

/**
 * Node state in the cluster
 */
export enum NodeState {
    UNKNOWN = 'unknown',
    CONNECTING = 'connecting',
    CONNECTED = 'connected',
    OWNER = 'owner',
    CHILD = 'child',
    DISCONNECTED = 'disconnected',
    FAILED = 'failed'
}

/**
 * Node information
 */
export interface NodeInfo {
    address: Address;
    state: NodeState;
    connection: ClientConnection | null;
    lastSeen: number;
    failureCount: number;
    isOwner: boolean;
}

/**
 * Implements the Java client's failover behavior
 * This ensures seamless node transitions and proper ownership handling
 */
export class HazelcastFailoverManager {
    private readonly logger: ILogger;
    private readonly client: HazelcastClient;
    private readonly nodes: Map<string, NodeInfo> = new Map();
    private readonly failoverInProgress: boolean = false;
    private readonly failoverTimeout: number = 30000; // 30 seconds
    private currentOwner: Address | null = null;

    constructor(client: HazelcastClient, logger: ILogger) {
        this.client = client;
        this.logger = logger;
    }

    /**
     * Registers a node in the cluster
     * @param address The node address
     * @param connection The connection to the node
     * @param isOwner Whether this node is the owner
     */
    registerNode(address: Address, connection: ClientConnection, isOwner: boolean = false): void {
        const addressStr = address.toString();
        
        const nodeInfo: NodeInfo = {
            address,
            state: isOwner ? NodeState.OWNER : NodeState.CHILD,
            connection,
            lastSeen: Date.now(),
            failureCount: 0,
            isOwner
        };

        this.nodes.set(addressStr, nodeInfo);
        
        if (isOwner) {
            this.currentOwner = address;
            this.logger.info('HazelcastFailoverManager', `Registered owner node: ${addressStr}`);
        } else {
            this.logger.info('HazelcastFailoverManager', `Registered child node: ${addressStr}`);
        }
    }

    /**
     * Handles node disconnection gracefully
     * @param address The disconnected node address
     */
    handleNodeDisconnection(address: Address): void {
        const addressStr = address.toString();
        const nodeInfo = this.nodes.get(addressStr);
        
        if (!nodeInfo) {
            return;
        }

        this.logger.warn('HazelcastFailoverManager', `Node ${addressStr} disconnected, state: ${nodeInfo.state}`);
        
        if (nodeInfo.isOwner) {
            // Owner node disconnected - initiate failover
            this.handleOwnerDisconnection(address);
        } else {
            // Child node disconnected - just mark as disconnected
            this.handleChildDisconnection(address);
        }
    }

    /**
     * Handles owner node disconnection
     * @param address The disconnected owner node address
     */
    private handleOwnerDisconnection(address: Address): void {
        const addressStr = address.toString();
        this.logger.warn('HazelcastFailoverManager', `Owner node ${addressStr} disconnected, initiating failover`);
        
        // Mark current owner as failed
        const nodeInfo = this.nodes.get(addressStr);
        if (nodeInfo) {
            nodeInfo.state = NodeState.FAILED;
            nodeInfo.connection = null;
        }
        
        this.currentOwner = null;
        
        // Find the best candidate for new owner
        const newOwner = this.selectNewOwner();
        if (newOwner) {
            this.promoteToOwner(newOwner);
        } else {
            this.logger.error('HazelcastFailoverManager', 'No suitable node found for ownership, cluster may be down');
        }
    }

    /**
     * Handles child node disconnection
     * @param address The disconnected child node address
     */
    private handleChildDisconnection(address: Address): void {
        const addressStr = address.toString();
        this.logger.info('HazelcastFailoverManager', `Child node ${addressStr} disconnected`);
        
        // Mark as disconnected but keep in nodes map
        const nodeInfo = this.nodes.get(addressStr);
        if (nodeInfo) {
            nodeInfo.state = NodeState.DISCONNECTED;
            nodeInfo.connection = null;
        }
        
        // No failover needed for child nodes
    }

    /**
     * Selects the best candidate for new owner
     * @returns The address of the best candidate, or null if none available
     */
    private selectNewOwner(): Address | null {
        const candidates: NodeInfo[] = [];
        
        this.nodes.forEach((nodeInfo) => {
            if (nodeInfo.state === NodeState.CONNECTED && 
                nodeInfo.connection && 
                nodeInfo.connection.isAlive() &&
                !nodeInfo.isOwner) {
                candidates.push(nodeInfo);
            }
        });
        
        if (candidates.length === 0) {
            return null;
        }
        
        // Select the node with the lowest failure count and most recent connection
        candidates.sort((a, b) => {
            if (a.failureCount !== b.failureCount) {
                return a.failureCount - b.failureCount;
            }
            return b.lastSeen - a.lastSeen;
        });
        
        return candidates[0].address;
    }

    /**
     * Promotes a node to owner
     * @param address The address of the node to promote
     */
    private promoteToOwner(address: Address): void {
        const addressStr = address.toString();
        const nodeInfo = this.nodes.get(addressStr);
        
        if (!nodeInfo) {
            this.logger.error('HazelcastFailoverManager', `Cannot promote ${addressStr} to owner - node not found`);
            return;
        }
        
        this.logger.info('HazelcastFailoverManager', `Promoting ${addressStr} to owner`);
        
        // Update node state
        nodeInfo.state = NodeState.OWNER;
        nodeInfo.isOwner = true;
        
        // Update current owner
        this.currentOwner = address;
        
        // Notify client of ownership change
        this.client.getClusterService().handleOwnershipChange(address, nodeInfo.connection!);
        
        this.logger.info('HazelcastFailoverManager', `Ownership transferred to ${addressStr}`);
    }

    /**
     * Handles node reconnection
     * @param address The reconnected node address
     * @param connection The new connection
     */
    handleNodeReconnection(address: Address, connection: ClientConnection): void {
        const addressStr = address.toString();
        const nodeInfo = this.nodes.get(addressStr);
        
        if (!nodeInfo) {
            // New node joining the cluster
            this.registerNode(address, connection, false);
            return;
        }
        
        this.logger.info('HazelcastFailoverManager', `Node ${addressStr} reconnected, previous state: ${nodeInfo.state}`);
        
        // Update connection and state
        nodeInfo.connection = connection;
        nodeInfo.lastSeen = Date.now();
        
        if (nodeInfo.state === NodeState.FAILED) {
            // Failed node recovered - treat as child
            nodeInfo.state = NodeState.CHILD;
            nodeInfo.isOwner = false;
            this.logger.info('HazelcastFailoverManager', `Failed node ${addressStr} recovered and rejoined as child`);
        } else if (nodeInfo.state === NodeState.DISCONNECTED) {
            // Disconnected node reconnected
            nodeInfo.state = nodeInfo.isOwner ? NodeState.OWNER : NodeState.CHILD;
            this.logger.info('HazelcastFailoverManager', `Disconnected node ${addressStr} reconnected`);
        }
        
        // Stop any ongoing failover/reconnection logic
        this.stopFailoverLogic();
    }

    /**
     * Stops all failover and reconnection logic
     */
    private stopFailoverLogic(): void {
        this.logger.info('HazelcastFailoverManager', 'Stopping all failover and reconnection logic - cluster stable');
        
        // Reset failure counts for all nodes
        this.nodes.forEach((nodeInfo) => {
            nodeInfo.failureCount = 0;
        });
    }

    /**
     * Gets the current owner address
     * @returns The current owner address or null if no owner
     */
    getCurrentOwner(): Address | null {
        return this.currentOwner;
    }

    /**
     * Gets all connected nodes
     * @returns Array of connected node addresses
     */
    getConnectedNodes(): Address[] {
        const connected: Address[] = [];
        
        this.nodes.forEach((nodeInfo) => {
            if (nodeInfo.state === NodeState.CONNECTED || 
                nodeInfo.state === NodeState.OWNER) {
                connected.push(nodeInfo.address);
            }
        });
        
        return connected;
    }

    /**
     * Gets the cluster state summary
     * @returns Summary of cluster state
     */
    getClusterStateSummary(): any {
        const summary: any = {
            totalNodes: this.nodes.size,
            currentOwner: this.currentOwner ? this.currentOwner.toString() : 'none',
            connectedNodes: this.getConnectedNodes().map(addr => addr.toString()),
            nodeStates: {}
        };
        
        this.nodes.forEach((nodeInfo, addressStr) => {
            summary.nodeStates[addressStr] = {
                state: nodeInfo.state,
                isOwner: nodeInfo.isOwner,
                failureCount: nodeInfo.failureCount,
                lastSeen: new Date(nodeInfo.lastSeen).toISOString()
            };
        });
        
        return summary;
    }

    /**
     * Cleans up failed nodes that haven't recovered
     * @param maxFailureAge Maximum age for failed nodes (default: 5 minutes)
     */
    cleanupFailedNodes(maxFailureAge: number = 300000): void {
        const now = Date.now();
        const toRemove: string[] = [];
        
        this.nodes.forEach((nodeInfo, addressStr) => {
            if (nodeInfo.state === NodeState.FAILED) {
                const timeSinceFailure = now - nodeInfo.lastSeen;
                if (timeSinceFailure > maxFailureAge) {
                    toRemove.push(addressStr);
                }
            }
        });
        
        if (toRemove.length > 0) {
            this.logger.info('HazelcastFailoverManager', `Cleaning up ${toRemove.length} failed nodes`);
            toRemove.forEach(addressStr => {
                this.nodes.delete(addressStr);
                this.logger.debug('HazelcastFailoverManager', `Removed failed node: ${addressStr}`);
            });
        }
    }
}
