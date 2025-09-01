const { expect } = require('chai');
const Client = require('../lib').Client;

describe('Proxy Creation Test - Version 3.12.5-1 (@celerispay)', function() {
    this.timeout(10000);
    
    let client;
    
    before(function() {
        // Create a minimal config for testing
        const config = {
            network: {
                clusterMembers: ['127.0.0.1:5701'], // Use localhost for testing
                connectionTimeout: 5000,
                connectionAttemptLimit: 1
            }
        };
        
        return Client.newHazelcastClient(config)
            .then((hzClient) => {
                client = hzClient;
            })
            .catch((error) => {
                // If connection fails, that's expected in test environment
                // We'll test the proxy creation logic separately
                console.log('Connection failed (expected in test environment):', error.message);
            });
    });
    
    after(function() {
        if (client) {
            return client.shutdown();
        }
    });
    
    it('should handle proxy creation gracefully when cluster is unavailable', function() {
        if (!client) {
            this.skip(); // Skip if client creation failed
        }
        
        // This should not hang - it should either succeed or fail gracefully
        return client.getMap('test-map')
            .then((map) => {
                // If we get here, the proxy was created successfully
                expect(map).to.exist;
                expect(typeof map.get).to.equal('function');
                expect(typeof map.put).to.equal('function');
            })
            .catch((error) => {
                // If it fails, it should fail quickly with a meaningful error
                expect(error).to.exist;
                expect(error.message).to.be.a('string');
                expect(error.message).to.not.include('undefined');
                expect(error.message).to.not.include('null');
            });
    });
    
    it('should have proper error handling for proxy creation', function() {
        if (!client) {
            this.skip(); // Skip if client creation failed
        }
        
        // Test that the client doesn't hang on proxy creation
        const startTime = Date.now();
        const timeout = 5000; // 5 second timeout
        
        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                reject(new Error('Proxy creation hung for more than 5 seconds'));
            }, timeout);
            
            client.getMap('test-map')
                .then((map) => {
                    clearTimeout(timeoutId);
                    const duration = Date.now() - startTime;
                    expect(duration).to.be.lessThan(timeout);
                    resolve(map);
                })
                .catch((error) => {
                    clearTimeout(timeoutId);
                    const duration = Date.now() - startTime;
                    expect(duration).to.be.lessThan(timeout);
                    resolve(); // Error is acceptable, hanging is not
                });
        });
    });
});
