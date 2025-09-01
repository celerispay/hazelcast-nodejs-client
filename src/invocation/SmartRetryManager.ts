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
 * Error types that determine retry strategy
 */
export enum ErrorType {
    AUTHENTICATION = 'authentication',
    NETWORK = 'network',
    NODE_STARTUP = 'node_startup',
    TEMPORARY = 'temporary',
    PERMANENT = 'permanent'
}

/**
 * Retry strategy configuration
 */
export interface RetryStrategy {
    maxRetries: number;
    baseDelay: number;
    maxDelay: number;
    backoffMultiplier: number;
    shouldRetry: (error: Error, attemptCount: number) => boolean;
}

/**
 * Manages smart retry logic based on error types
 * This prevents rapid retries for different types of errors
 */
export class SmartRetryManager {
    private readonly logger: ILogger;
    private readonly retryStrategies: Map<ErrorType, RetryStrategy>;
    private readonly errorHistory: Map<string, Array<{error: Error, timestamp: number, type: ErrorType}>>;
    private readonly errorHistoryWindow: number = 60000; // 1 minute window

    constructor(logger: ILogger) {
        this.logger = logger;
        this.errorHistory = new Map();
        this.retryStrategies = this.initializeRetryStrategies();
    }

    /**
     * Initializes retry strategies for different error types
     */
    private initializeRetryStrategies(): Map<ErrorType, RetryStrategy> {
        const strategies = new Map<ErrorType, RetryStrategy>();

        // Authentication errors - retry with exponential backoff, but not too aggressively
        strategies.set(ErrorType.AUTHENTICATION, {
            maxRetries: 3,
            baseDelay: 2000, // 2 seconds
            maxDelay: 10000, // 10 seconds
            backoffMultiplier: 2,
            shouldRetry: (error: Error, attemptCount: number) => {
                // Don't retry authentication errors too many times
                return attemptCount < 3;
            }
        });

        // Network errors - retry more aggressively
        strategies.set(ErrorType.NETWORK, {
            maxRetries: 5,
            baseDelay: 1000, // 1 second
            maxDelay: 8000,  // 8 seconds
            backoffMultiplier: 1.5,
            shouldRetry: (error: Error, attemptCount: number) => {
                return attemptCount < 5;
            }
        });

        // Node startup errors - wait longer between retries
        strategies.set(ErrorType.NODE_STARTUP, {
            maxRetries: 8,
            baseDelay: 3000, // 3 seconds
            maxDelay: 15000, // 15 seconds
            backoffMultiplier: 1.8,
            shouldRetry: (error: Error, attemptCount: number) => {
                return attemptCount < 8;
            }
        });

        // Temporary errors - quick retries
        strategies.set(ErrorType.TEMPORARY, {
            maxRetries: 3,
            baseDelay: 500,  // 0.5 seconds
            maxDelay: 2000,  // 2 seconds
            backoffMultiplier: 1.2,
            shouldRetry: (error: Error, attemptCount: number) => {
                return attemptCount < 3;
            }
        });

        // Permanent errors - no retries
        strategies.set(ErrorType.PERMANENT, {
            maxRetries: 0,
            baseDelay: 0,
            maxDelay: 0,
            backoffMultiplier: 1,
            shouldRetry: (error: Error, attemptCount: number) => {
                return false;
            }
        });

        return strategies;
    }

    /**
     * Classifies an error to determine the appropriate retry strategy
     * @param error The error to classify
     * @returns The error type
     */
    classifyError(error: Error): ErrorType {
        const errorMessage = error.message.toLowerCase();
        
        // Authentication errors
        if (errorMessage.includes('invalid credentials') || 
            errorMessage.includes('authentication') || 
            errorMessage.includes('credentials')) {
            return ErrorType.AUTHENTICATION;
        }
        
        // Network errors
        if (errorMessage.includes('connection') || 
            errorMessage.includes('timeout') || 
            errorMessage.includes('network') ||
            errorMessage.includes('econnreset') ||
            errorMessage.includes('epipe')) {
            return ErrorType.NETWORK;
        }
        
        // Node startup errors (when node is starting but not ready)
        if (errorMessage.includes('not ready') || 
            errorMessage.includes('starting') ||
            errorMessage.includes('initializing')) {
            return ErrorType.NODE_STARTUP;
        }
        
        // Temporary errors
        if (errorMessage.includes('temporary') || 
            errorMessage.includes('retry') ||
            errorMessage.includes('busy')) {
            return ErrorType.TEMPORARY;
        }
        
        // Default to network error for unknown cases
        return ErrorType.NETWORK;
    }

    /**
     * Determines if an error should be retried
     * @param error The error to check
     * @param address The address the error occurred for
     * @returns true if the error should be retried, false otherwise
     */
    shouldRetryError(error: Error, address: Address): boolean {
        const addressStr = address.toString();
        const errorType = this.classifyError(error);
        const strategy = this.retryStrategies.get(errorType);
        
        if (!strategy) {
            return false;
        }

        // Check error history for this address
        const recentErrors = this.getRecentErrors(addressStr);
        const errorCount = recentErrors.length;
        
        // Record this error
        this.recordError(addressStr, error, errorType);
        
        // Check if we should retry based on strategy and history
        return strategy.shouldRetry(error, errorCount);
    }

    /**
     * Calculates the delay before the next retry attempt
     * @param error The error that occurred
     * @param address The address the error occurred for
     * @returns The delay in milliseconds
     */
    calculateRetryDelay(error: Error, address: Address): number {
        const addressStr = address.toString();
        const errorType = this.classifyError(error);
        const strategy = this.retryStrategies.get(errorType);
        
        if (!strategy) {
            return 0;
        }

        const recentErrors = this.getRecentErrors(addressStr);
        const attemptCount = recentErrors.length;
        
        if (attemptCount >= strategy.maxRetries) {
            return 0; // No more retries
        }

        // Calculate exponential backoff
        let delay = strategy.baseDelay * Math.pow(strategy.backoffMultiplier, attemptCount);
        
        // Cap at maximum delay
        delay = Math.min(delay, strategy.maxDelay);
        
        // Add some jitter to prevent thundering herd
        const jitter = Math.random() * 0.1 * delay; // 10% jitter
        delay += jitter;
        
        this.logger.debug('SmartRetryManager', 
            `Calculated retry delay for ${addressStr}: ${Math.round(delay)}ms (attempt ${attemptCount + 1})`);
        
        return Math.round(delay);
    }

    /**
     * Records an error for tracking purposes
     * @param addressStr The address string
     * @param error The error that occurred
     * @param errorType The classified error type
     */
    private recordError(addressStr: string, error: Error, errorType: ErrorType): void {
        if (!this.errorHistory.has(addressStr)) {
            this.errorHistory.set(addressStr, []);
        }
        
        const now = Date.now();
        const errors = this.errorHistory.get(addressStr)!;
        
        // Add new error
        errors.push({
            error,
            timestamp: now,
            type: errorType
        });
        
        // Clean up old errors outside the window
        const cutoff = now - this.errorHistoryWindow;
        const filteredErrors = errors.filter(e => e.timestamp > cutoff);
        
        if (filteredErrors.length !== errors.length) {
            this.errorHistory.set(addressStr, filteredErrors);
        }
        
        this.logger.debug('SmartRetryManager', 
            `Recorded ${errorType} error for ${addressStr}, total recent errors: ${filteredErrors.length}`);
    }

    /**
     * Gets recent errors for an address within the time window
     * @param addressStr The address string
     * @returns Array of recent errors
     */
    private getRecentErrors(addressStr: string): Array<{error: Error, timestamp: number, type: ErrorType}> {
        const errors = this.errorHistory.get(addressStr);
        if (!errors) {
            return [];
        }
        
        const now = Date.now();
        const cutoff = now - this.errorHistoryWindow;
        
        return errors.filter(e => e.timestamp > cutoff);
    }

    /**
     * Clears error history for an address (useful during failover)
     * @param address The address to clear
     */
    clearErrorHistory(address: Address): void {
        const addressStr = address.toString();
        this.errorHistory.delete(addressStr);
        this.logger.debug('SmartRetryManager', `Cleared error history for ${addressStr}`);
    }

    /**
     * Clears ALL error history (useful during critical failover)
     */
    clearAllErrorHistory(): void {
        this.errorHistory.clear();
        this.logger.info('SmartRetryManager', 'Cleared all error history');
    }

    /**
     * Gets a summary of current error history
     * @returns A string summary of error history
     */
    getErrorHistorySummary(): string {
        const summary: string[] = [];
        
        this.errorHistory.forEach((errors, address) => {
            const errorTypes = new Map<ErrorType, number>();
            errors.forEach(e => {
                errorTypes.set(e.type, (errorTypes.get(e.type) || 0) + 1);
            });
            
            const typeSummary = Array.from(errorTypes.entries())
                .map(([type, count]) => `${type}:${count}`)
                .join(',');
            
            summary.push(`${address}: ${errors.length} errors [${typeSummary}]`);
        });
        
        return summary.length > 0 ? summary.join('; ') : 'none';
    }
}
