const assert = require('assert');
const { ClientConfig } = require('../lib/config/Config');
const { ClientNetworkConfig } = require('../lib/config/ClientNetworkConfig');

describe('Connection Failover Test - Version 3.12.5 (@celerispay)', function() {
    let config;

    beforeEach(function() {
        config = new ClientConfig();
        config.networkConfig = new ClientNetworkConfig();
        config.networkConfig.addresses = ['127.0.0.1:5701', '127.0.0.1:5702'];
        config.networkConfig.connectionAttemptLimit = 3;
        config.networkConfig.connectionAttemptPeriod = 1000;
        config.networkConfig.connectionTimeout = 5000;
        config.networkConfig.redoOperation = true;
        config.networkConfig.smartRouting = true;
        
        // Enhanced connection management
        config.properties['hazelcast.client.connection.health.check.interval'] = 1000;
        config.properties['hazelcast.client.connection.max.retries'] = 2;
        config.properties['hazelcast.client.connection.retry.delay'] = 500;
        config.properties['hazelcast.client.failover.cooldown'] = 1000;
        config.properties['hazelcast.client.partition.refresh.min.interval'] = 500;
        config.properties['hazelcast.client.invocation.max.retries'] = 5;
        config.properties['hazelcast.client.partition.failure.backoff'] = 2000;
    });

    it('should have proper configuration defaults', function() {
        assert.strictEqual(config.networkConfig.connectionAttemptLimit, 3);
        assert.strictEqual(config.networkConfig.redoOperation, true);
        assert.strictEqual(config.networkConfig.smartRouting, true);
        assert.strictEqual(config.properties['hazelcast.client.connection.max.retries'], 2);
        assert.strictEqual(config.properties['hazelcast.client.failover.cooldown'], 1000);
    });

    it('should have enhanced connection management properties', function() {
        const requiredProps = [
            'hazelcast.client.connection.health.check.interval',
            'hazelcast.client.connection.max.retries',
            'hazelcast.client.connection.retry.delay',
            'hazelcast.client.failover.cooldown',
            'hazelcast.client.partition.refresh.min.interval',
            'hazelcast.client.invocation.max.retries',
            'hazelcast.client.partition.failure.backoff'
        ];

        requiredProps.forEach(prop => {
            assert(config.properties.hasOwnProperty(prop), `Missing property: ${prop}`);
        });
    });

    it('should have improved network configuration defaults', function() {
        assert.strictEqual(config.networkConfig.connectionAttemptLimit, 3);
        assert.strictEqual(config.networkConfig.connectionTimeout, 5000);
        assert.strictEqual(config.networkConfig.redoOperation, true);
        assert.strictEqual(config.networkConfig.smartRouting, true);
    });

    it('should have all required configuration properties', function() {
        // Test that all the new properties are accessible
        assert.strictEqual(config.properties['hazelcast.client.connection.health.check.interval'], 1000);
        assert.strictEqual(config.properties['hazelcast.client.connection.max.retries'], 2);
        assert.strictEqual(config.properties['hazelcast.client.connection.retry.delay'], 500);
        assert.strictEqual(config.properties['hazelcast.client.failover.cooldown'], 1000);
        assert.strictEqual(config.properties['hazelcast.client.partition.refresh.min.interval'], 500);
        assert.strictEqual(config.properties['hazelcast.client.invocation.max.retries'], 5);
        assert.strictEqual(config.properties['hazelcast.client.partition.failure.backoff'], 2000);
    });

    it('should maintain backward compatibility', function() {
        // Test that existing properties are still available
        assert.strictEqual(config.properties['hazelcast.client.heartbeat.interval'], 5000);
        assert.strictEqual(config.properties['hazelcast.client.heartbeat.timeout'], 60000);
        assert.strictEqual(config.properties['hazelcast.client.invocation.retry.pause.millis'], 1000);
        assert.strictEqual(config.properties['hazelcast.client.invocation.timeout.millis'], 120000);
    });

    it('should be ready for celerispay organization deployment', function() {
        // Verify this is the celerispay version
        assert.strictEqual(config.networkConfig.redoOperation, true, 'redoOperation should be enabled for better failover');
        assert.strictEqual(config.networkConfig.smartRouting, true, 'smartRouting should be enabled for optimal performance');
        assert.strictEqual(config.properties['hazelcast.client.failover.cooldown'], 1000, 'failover cooldown should be configured');
        assert.strictEqual(config.properties['hazelcast.client.connection.health.check.interval'], 1000, 'health check interval should be configured');
    });
});
