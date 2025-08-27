# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.12.5] - 2024-01-XX

### Fixed
- **Critical**: Fixed connection failover issues that caused client to hang when partition owners go down
- **Critical**: Fixed connection leakage that resulted in increasing connection counts to failed nodes
- **Critical**: Fixed hanging invocations that would never complete or fail gracefully
- **Critical**: Fixed poor failover logic that prevented switching to healthy nodes
- **Critical**: Fixed repeated connection attempts to known failed nodes

### Added
- **Connection Health Monitoring**: Active health checks every 5 seconds to detect broken connections
- **Enhanced Failover Logic**: Proper failover cooldown and structured failover process
- **Connection Retry with Backoff**: Intelligent retry mechanism with configurable delays
- **Failed Connection Tracking**: Temporary blocking of repeatedly failed addresses
- **Partition Table Management**: Automatic clearing and refresh of partition information
- **Enhanced Retry Logic**: Maximum retry limits and partition-specific failure handling
- **Address Blocking System**: Temporary blocking of failed addresses (30 seconds) to prevent repeated failures
- **New Configuration Properties**: Enhanced connection management and failover control options

### Changed
- **Network Configuration**: Increased default `connectionAttemptLimit` from 2 to 5
- **Network Configuration**: Increased default `connectionTimeout` from 5000ms to 10000ms
- **Network Configuration**: Changed default `redoOperation` from false to true
- **Connection Management**: Added health check intervals and retry limits
- **Failover Control**: Added cooldown periods and refresh rate limiting
- **Address Management**: Added intelligent blocking of failed addresses with automatic unblocking

### Configuration Properties Added
- `hazelcast.client.connection.health.check.interval`: Connection health check interval (ms)
- `hazelcast.client.connection.max.retries`: Maximum connection retry attempts
- `hazelcast.client.connection.retry.delay`: Delay between connection retries (ms)
- `hazelcast.client.failover.cooldown`: Cooldown period between failover attempts (ms)
- `hazelcast.client.partition.refresh.min.interval`: Minimum interval between partition refreshes (ms)
- `hazelcast.client.invocation.max.retries`: Maximum invocation retry attempts
- `hazelcast.client.partition.failure.backoff`: Backoff delay for partition failures (ms)

### Technical Improvements
- **ClientConnectionManager**: Added connection health monitoring and retry logic
- **ClusterService**: Improved failover handling with cooldown, structured process, and address blocking
- **PartitionService**: Enhanced partition table management and refresh logic
- **InvocationService**: Better retry handling and partition failure management
- **Error Handling**: Improved error handling and logging throughout the codebase
- **Address Tracking**: Intelligent tracking and blocking of failed addresses

### Backward Compatibility
- **100% Backward Compatible**: No breaking changes, existing code will work unchanged
- **Same Import Statement**: `require('@celerispay/hazelcast-client')` for new version
- **Same API**: All existing methods and properties remain unchanged
- **Enhanced Defaults**: Better default values for production use

## [3.12.4] - Previous Release

### Previous version without connection failover fixes

---

## Migration Guide

### From 3.12.4 to 3.12.5

1. **Update package.json**:
   ```json
   {
     "dependencies": {
       "@celerispay/hazelcast-client": "3.12.5"
     }
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

4. **Optional**: Configure enhanced properties for better control:
   ```javascript
   properties: {
       'hazelcast.client.connection.health.check.interval': 5000,
       'hazelcast.client.failover.cooldown': 5000,
       'hazelcast.client.invocation.max.retries': 10
   }
   ```

## Testing

Run the test suite to verify the fixes:

```bash
npm test -- --grep "Connection Failover Test"
```

## Documentation

- **FAILOVER_FIXES.md**: Detailed technical documentation of all fixes
- **QUICK_START.md**: Quick start guide with configuration examples
- **CHANGELOG.md**: This file with detailed change information

## Package Information

- **Package Name**: `@celerispay/hazelcast-client`
- **Version**: `3.12.5`
- **Publisher**: CelerisPay
- **License**: Apache-2.0
