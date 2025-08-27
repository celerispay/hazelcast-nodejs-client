# Hazelcast Node.js Client - Quick Start Guide

## Version Information
- **Package**: `@celerispay/hazelcast-client`
- **Version**: `3.12.5-1`
- **Publisher**: CelerisPay
- **Base Version**: 3.12.5 (Hazelcast Inc.)

## Overview
This is a production-ready, enhanced version of the Hazelcast Node.js client with critical failover fixes that resolve connection management issues in production environments.

## Installation

### Install the Fixed Version
```bash
npm install @celerispay/hazelcast-client@3.12.5-1
```

### Verify Installation
```bash
npm list @celerispay/hazelcast-client
# Should show version 3.12.5-1
```

## Basic Usage

### Import the Client
```javascript
const { ClientConfig, HazelcastClient } = require('@celerispay/hazelcast-client');
```

### Create Client Configuration
```javascript
const config = new ClientConfig();
config.networkConfig.addresses = ['127.0.0.1:5701', '127.0.0.1:5702'];

// Enhanced failover settings (automatically configured)
config.properties['hazelcast.client.connection.health.check.interval'] = 5000;
config.properties['hazelcast.client.failover.cooldown'] = 5000;
config.properties['hazelcast.client.partition.refresh.min.interval'] = 2000;
```

### Connect and Use
```javascript
async function main() {
    const client = await HazelcastClient.newHazelcastClient(config);
    
    const map = await client.getMap('myMap');
    await map.put('key', 'value');
    
    const value = await map.get('key');
    console.log('Value:', value);
    
    await client.shutdown();
}

main().catch(console.error);
```

## Key Improvements

### 1. **Robust Failover**
- Automatic detection of node failures
- Intelligent failover to healthy nodes
- Failover cooldown to prevent rapid switching

### 2. **Connection Health Monitoring**
- Continuous connection health checks
- Automatic cleanup of failed connections
- Prevention of connection leakage

### 3. **Address Blocking System**
- Temporary blocking of failed addresses (30 seconds)
- Automatic unblocking after block duration
- Prevention of repeated connection attempts

### 4. **Enhanced Error Handling**
- Near cache crash prevention during failover
- Graceful degradation during cluster changes
- Comprehensive error logging

### 5. **Intelligent Reconnection**
- Automatic reconnection to recovered nodes
- Smart ownership management
- Partition table refresh management

## Configuration Properties

### Connection Management
```javascript
config.properties['hazelcast.client.connection.health.check.interval'] = 5000;  // 5 seconds
config.properties['hazelcast.client.connection.max.retries'] = 3;              // Max 3 retries
config.properties['hazelcast.client.connection.retry.delay'] = 1000;           // 1 second delay
```

### Failover Control
```javascript
config.properties['hazelcast.client.failover.cooldown'] = 5000;                // 5 seconds cooldown
config.properties['hazelcast.client.partition.refresh.min.interval'] = 2000;   // 2 seconds minimum
```

### Retry Behavior
```javascript
config.properties['hazelcast.client.invocation.max.retries'] = 10;            // Max 10 retries
config.properties['hazelcast.client.partition.failure.backoff'] = 2000;       // 2 seconds backoff
```

## Testing

### Run the Test Suite
```bash
npm test
```

### Verify Configuration
```bash
node -e "
const { ClientConfig } = require('@celerispay/hazelcast-client');
const config = new ClientConfig();
console.log('Enhanced properties:', Object.keys(config.properties).filter(p => p.includes('connection') || p.includes('failover')));
"
```

## Migration from Original Client

### 1. **Update Package Name**
```bash
# Remove original
npm uninstall hazelcast-client

# Install fixed version
npm install @celerispay/hazelcast-client@3.12.5-1
```

### 2. **Update Import Statement**
```javascript
// Before
const { ClientConfig } = require('hazelcast-client');

// After
const { ClientConfig } = require('@celerispay/hazelcast-client');
```

### 3. **No Code Changes Required**
- All existing code will work unchanged
- Failover behavior will automatically improve
- Connection management will be more robust

## Production Recommendations

### 1. **Enable Statistics**
```javascript
config.properties['hazelcast.client.statistics.enabled'] = true;
```

### 2. **Monitor Logs**
Watch for these log messages:
- `"Starting failover process..."`
- `"Failover completed successfully"`
- `"Connection health check interval"`
- `"Address blocked for X seconds"`

### 3. **Load Testing**
Test failover scenarios under load:
- Stop partition owner nodes
- Monitor automatic failover
- Verify reconnection to recovered nodes

### 4. **Network Configuration**
```javascript
config.networkConfig.connectionAttemptLimit = 5;    // Increased from default
config.networkConfig.connectionTimeout = 10000;     // Increased from default
config.networkConfig.redoOperation = true;          // Enable for better failover
```

## What's Fixed

### ✅ **Connection Bombardment**
- No more repeated connection attempts to failed nodes
- Intelligent address blocking prevents network spam
- Connection health monitoring detects failures early

### ✅ **Failover Failures**
- Automatic failover to healthy nodes
- Proper partition table management during failover
- Failover cooldown prevents rapid switching

### ✅ **Near Cache Crashes**
- Comprehensive error handling prevents crashes
- Graceful degradation during cluster changes
- Safe fallback values during failover

### ✅ **Connection Leakage**
- Automatic cleanup of failed connections
- Periodic connection health checks
- Memory leak prevention

### ✅ **Hanging Operations**
- Maximum retry limits prevent infinite loops
- Proper error handling and logging
- Graceful failure with user feedback

## Support

### **Package Information**
- **Name**: `@celerispay/hazelcast-client`
- **Version**: `3.12.5-1`
- **Publisher**: CelerisPay

### **Documentation**
- **Technical Details**: See `FAILOVER_FIXES.md`
- **Release Notes**: See `CHANGELOG.md`
- **Repository**: https://github.com/celerispay/hazelcast-nodejs-client

### **Issues and Support**
- **GitHub Issues**: https://github.com/celerispay/hazelcast-nodejs-client/issues
- **Professional Support**: Available from CelerisPay

## Expected Results

After implementing this fixed version, you should see:

1. **Stable Connections**: No more connection bombardment to failed nodes
2. **Automatic Failover**: Seamless switching to healthy nodes when failures occur
3. **Better Performance**: Reduced network traffic and improved response times
4. **Cleaner Logs**: Fewer error messages and better failure information
5. **Production Stability**: Reliable operation even during cluster topology changes

---

**Ready for Production**: This version has been thoroughly tested and is ready for production deployment with professional support from CelerisPay.

