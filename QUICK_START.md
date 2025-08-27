# Quick Start - Fixed Hazelcast Node.js Client 3.12.5

## Installation

```bash
# Install the fixed version
npm install @celerispay/hazelcast-client@3.12.5

# Or if you're using yarn
yarn add @celerispay/hazelcast-client@3.12.5
```

## Basic Usage

```javascript
const { HazelcastClient } = require('@celerispay/hazelcast-client');

async function createClient() {
    const client = await HazelcastClient.newHazelcastClient({
        networkConfig: {
            addresses: ['10.0.20.30:5701', '10.0.20.31:5701'],
            connectionAttemptLimit: 5,
            connectionTimeout: 10000,
            redoOperation: true,
            smartRouting: true
        },
        properties: {
            'hazelcast.client.connection.health.check.interval': 5000,
            'hazelcast.client.connection.max.retries': 3,
            'hazelcast.client.failover.cooldown': 5000,
            'hazelcast.client.invocation.max.retries': 10
        }
    });
    
    return client;
}
```

## Key Improvements

✅ **Connection Health Monitoring** - Active health checks every 5 seconds  
✅ **Automatic Failover** - Seamless switching to healthy nodes  
✅ **Connection Cleanup** - No more connection leakage  
✅ **Smart Retry Logic** - Intelligent retry with backoff  
✅ **Partition Table Refresh** - Automatic partition ownership updates  
✅ **Address Blocking** - Temporary blocking of failed addresses to prevent repeated failures  

## Configuration

### Essential Properties

```javascript
properties: {
    // Connection health monitoring
    'hazelcast.client.connection.health.check.interval': 5000,
    'hazelcast.client.connection.max.retries': 3,
    'hazelcast.client.connection.retry.delay': 1000,
    
    // Failover control
    'hazelcast.client.failover.cooldown': 5000,
    'hazelcast.client.partition.refresh.min.interval': 2000,
    
    // Retry behavior
    'hazelcast.client.invocation.max.retries': 10,
    'hazelcast.client.partition.failure.backoff': 2000
}
```

### Network Configuration

```javascript
networkConfig: {
    addresses: ['node1:5701', 'node2:5701'],
    connectionAttemptLimit: 5,        // Increased from 2
    connectionTimeout: 10000,         // Increased from 5000
    redoOperation: true,              // Changed from false
    smartRouting: true
}
```

## Testing the Fix

```bash
# Run the failover tests
npm test -- --grep "Connection Failover Test"

# Run all tests
npm test
```

## Migration from Previous Versions

1. **Update package.json**:
   ```json
   "dependencies": {
     "@celerispay/hazelcast-client": "3.12.5"
   }
   ```

2. **Update import statement**:
   ```javascript
   // Before
   const { HazelcastClient } = require('hazelcast-client');
   
   // After
   const { HazelcastClient } = require('@celerispay/hazelcast-client');
   ```

3. **No other code changes required** - All fixes are backward compatible

## What's Fixed

- ❌ **Before**: Client hangs on partition owner failure
- ✅ **After**: Automatic failover to healthy nodes

- ❌ **Before**: Connection count keeps increasing
- ✅ **After**: Failed connections are properly cleaned up

- ❌ **Before**: Invocations hang indefinitely
- ✅ **After**: Operations fail gracefully with retry limits

- ❌ **Before**: No health monitoring
- ✅ **After**: Active connection health checking

- ❌ **Before**: Repeated attempts to failed nodes
- ✅ **After**: Temporary blocking of failed addresses (30 seconds)

## Production Recommendations

1. **Enable Statistics**: `'hazelcast.client.statistics.enabled': true`
2. **Monitor Logs**: Watch for failover events and address blocking
3. **Load Test**: Verify failover behavior under load
4. **Health Checks**: Use connection health metrics

## Support

- **Documentation**: See `FAILOVER_FIXES.md` for detailed information
- **Tests**: Run test suite to verify functionality
- **Issues**: Report problems in the repository

---

**Note**: This version (3.12.5) includes critical connection failover fixes and is published by CelerisPay. Consider upgrading to Hazelcast 4.x or 5.x for long-term support.
