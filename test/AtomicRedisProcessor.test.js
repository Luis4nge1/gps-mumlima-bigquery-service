import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { AtomicRedisProcessor } from '../src/services/AtomicRedisProcessor.js';

describe('AtomicRedisProcessor', () => {
  let processor;
  let mockRedisRepo;

  beforeEach(() => {
    processor = new AtomicRedisProcessor();
    
    // Create comprehensive mock for RedisRepository
    mockRedisRepo = {
      connect: async () => true,
      disconnect: async () => true,
      ping: async () => true,
      getGPSStats: async () => ({ totalRecords: 0, memoryUsage: 0 }),
      getMobileStats: async () => ({ totalRecords: 0, memoryUsage: 0 }),
      getListData: async () => [],
      clearListData: async () => true,
      addMultipleToList: async () => 1
    };
  });

  afterEach(async () => {
    if (processor) {
      await processor.cleanup();
    }
  });

  describe('constructor', () => {
    it('should create instance with correct initial state', () => {
      assert.strictEqual(processor.isInitialized, false);
      assert.ok(processor.redisRepo);
    });
  });

  describe('extractAndClearGPSData', () => {
    it('should extract GPS data and clear Redis immediately when data exists', async () => {
      // Mock data for GPS extraction
      const mockGPSData = [
        { id: 1, lat: 40.7128, lng: -74.0060, timestamp: '2025-01-25T10:00:00Z' },
        { id: 2, lat: 40.7589, lng: -73.9851, timestamp: '2025-01-25T10:01:00Z' },
        { id: 3, lat: 40.7505, lng: -73.9934, timestamp: '2025-01-25T10:02:00Z' }
      ];

      let clearCalled = false;
      let extractionOrder = [];
      let statsCallCount = 0;

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => {
          extractionOrder.push('getGPSStats');
          statsCallCount++;
          // Return initial stats on first call, final stats (0) on subsequent calls
          if (statsCallCount === 1) {
            return { totalRecords: 3, memoryUsage: 1024 };
          } else {
            return { totalRecords: 0, memoryUsage: 0 };
          }
        },
        getListData: async (key) => {
          extractionOrder.push('getListData');
          assert.strictEqual(key, 'gps:history:global');
          return mockGPSData;
        },
        clearListData: async (key) => {
          extractionOrder.push('clearListData');
          clearCalled = true;
          assert.strictEqual(key, 'gps:history:global');
          return true;
        }
      };

      const result = await processor.extractAndClearGPSData();

      // Verify extraction was successful
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 3);
      assert.deepStrictEqual(result.data, mockGPSData);
      assert.strictEqual(result.cleared, true);
      assert.strictEqual(result.key, 'gps:history:global');
      assert.strictEqual(result.initialRecords, 3);
      assert.strictEqual(result.finalRecords, 0);

      // Verify Redis was cleared immediately after extraction
      assert.strictEqual(clearCalled, true);
      
      // Verify correct order: stats -> extract -> clear -> final stats
      assert.deepStrictEqual(extractionOrder, ['getGPSStats', 'getListData', 'clearListData', 'getGPSStats']);

      // Verify timing information is present
      assert.ok(result.extractionTime >= 0);
      assert.ok(result.clearTime >= 0);
      assert.ok(result.totalTime >= 0);
    });

    it('should handle empty GPS data gracefully', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 0, memoryUsage: 0 })
      };

      const result = await processor.extractAndClearGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 0);
      assert.deepStrictEqual(result.data, []);
      assert.strictEqual(result.cleared, false); // No clearing needed for empty data
    });

    it('should handle Redis connection errors', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        connect: async () => { throw new Error('Redis connection failed'); }
      };

      const result = await processor.extractAndClearGPSData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Redis connection failed'));
      assert.strictEqual(result.recordCount, 0);
      assert.deepStrictEqual(result.data, []);
      assert.strictEqual(result.cleared, false);
    });

    it('should handle extraction errors while preserving Redis state', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 5, memoryUsage: 2048 }),
        getListData: async () => { throw new Error('Extraction failed'); }
      };

      const result = await processor.extractAndClearGPSData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Extraction failed'));
      assert.strictEqual(result.recordCount, 0);
      assert.strictEqual(result.cleared, false);
    });
  });

  describe('extractAndClearMobileData', () => {
    it('should extract Mobile data and clear Redis immediately when data exists', async () => {
      // Mock data for Mobile extraction
      const mockMobileData = [
        { id: 1, deviceId: 'device1', signal: -70, timestamp: '2025-01-25T10:00:00Z' },
        { id: 2, deviceId: 'device2', signal: -65, timestamp: '2025-01-25T10:01:00Z' }
      ];

      let clearCalled = false;
      let extractionOrder = [];
      let mobileStatsCallCount = 0;

      processor.redisRepo = {
        ...mockRedisRepo,
        getMobileStats: async () => {
          extractionOrder.push('getMobileStats');
          mobileStatsCallCount++;
          // Return initial stats on first call, final stats (0) on subsequent calls
          if (mobileStatsCallCount === 1) {
            return { totalRecords: 2, memoryUsage: 512 };
          } else {
            return { totalRecords: 0, memoryUsage: 0 };
          }
        },
        getListData: async (key) => {
          extractionOrder.push('getListData');
          assert.strictEqual(key, 'mobile:history:global');
          return mockMobileData;
        },
        clearListData: async (key) => {
          extractionOrder.push('clearListData');
          clearCalled = true;
          assert.strictEqual(key, 'mobile:history:global');
          return true;
        }
      };

      const result = await processor.extractAndClearMobileData();

      // Verify extraction was successful
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 2);
      assert.deepStrictEqual(result.data, mockMobileData);
      assert.strictEqual(result.cleared, true);
      assert.strictEqual(result.key, 'mobile:history:global');
      assert.strictEqual(result.initialRecords, 2);
      assert.strictEqual(result.finalRecords, 0);

      // Verify Redis was cleared immediately after extraction
      assert.strictEqual(clearCalled, true);
      
      // Verify correct order: stats -> extract -> clear -> final stats
      assert.deepStrictEqual(extractionOrder, ['getMobileStats', 'getListData', 'clearListData', 'getMobileStats']);
    });

    it('should handle empty Mobile data gracefully', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getMobileStats: async () => ({ totalRecords: 0, memoryUsage: 0 })
      };

      const result = await processor.extractAndClearMobileData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 0);
      assert.deepStrictEqual(result.data, []);
      assert.strictEqual(result.cleared, false);
    });

    it('should handle Mobile extraction errors', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getMobileStats: async () => ({ totalRecords: 3, memoryUsage: 1024 }),
        getListData: async () => { throw new Error('Mobile extraction failed'); }
      };

      const result = await processor.extractAndClearMobileData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Mobile extraction failed'));
      assert.strictEqual(result.recordCount, 0);
      assert.strictEqual(result.cleared, false);
    });
  });

  describe('extractAllData', () => {
    it('should coordinate both GPS and Mobile extractions atomically', async () => {
      const mockGPSData = [
        { id: 1, lat: 40.7128, lng: -74.0060, timestamp: '2025-01-25T10:00:00Z' }
      ];
      const mockMobileData = [
        { id: 1, deviceId: 'device1', signal: -70, timestamp: '2025-01-25T10:00:00Z' }
      ];

      let operationOrder = [];
      let gpsStatsCallCount = 0;
      let mobileStatsCallCount = 0;

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => {
          operationOrder.push('getGPSStats');
          gpsStatsCallCount++;
          // Return 1 for initial calls, 0 after clearing
          if (gpsStatsCallCount <= 2) {
            return { totalRecords: 1, memoryUsage: 256 };
          } else {
            return { totalRecords: 0, memoryUsage: 0 };
          }
        },
        getMobileStats: async () => {
          operationOrder.push('getMobileStats');
          mobileStatsCallCount++;
          // Return 1 for initial calls, 0 after clearing
          if (mobileStatsCallCount <= 2) {
            return { totalRecords: 1, memoryUsage: 256 };
          } else {
            return { totalRecords: 0, memoryUsage: 0 };
          }
        },
        getListData: async (key) => {
          operationOrder.push(`getListData-${key}`);
          if (key === 'gps:history:global') return mockGPSData;
          if (key === 'mobile:history:global') return mockMobileData;
          return [];
        },
        clearListData: async (key) => {
          operationOrder.push(`clearListData-${key}`);
          return true;
        }
      };

      const result = await processor.extractAllData();

      // Verify overall success
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.totalRecords, 2);
      assert.strictEqual(result.allCleared, true);

      // Verify GPS extraction results
      assert.strictEqual(result.gps.success, true);
      assert.strictEqual(result.gps.recordCount, 1);
      assert.deepStrictEqual(result.gps.data, mockGPSData);
      assert.strictEqual(result.gps.cleared, true);

      // Verify Mobile extraction results
      assert.strictEqual(result.mobile.success, true);
      assert.strictEqual(result.mobile.recordCount, 1);
      assert.deepStrictEqual(result.mobile.data, mockMobileData);
      assert.strictEqual(result.mobile.cleared, true);

      // Verify statistics
      assert.strictEqual(result.initialStats.gps, 1);
      assert.strictEqual(result.initialStats.mobile, 1);
      assert.strictEqual(result.initialStats.total, 2);
      assert.strictEqual(result.finalStats.gps, 0);
      assert.strictEqual(result.finalStats.mobile, 0);
      assert.strictEqual(result.finalStats.total, 0);

      // Verify coordinated execution order
      const expectedOrder = [
        'getGPSStats', 'getMobileStats', // Initial stats
        'getGPSStats', 'getListData-gps:history:global', 'clearListData-gps:history:global', 'getGPSStats', // GPS extraction
        'getMobileStats', 'getListData-mobile:history:global', 'clearListData-mobile:history:global', 'getMobileStats', // Mobile extraction
        'getGPSStats', 'getMobileStats' // Final stats
      ];
      assert.deepStrictEqual(operationOrder, expectedOrder);
    });

    it('should handle empty data for both GPS and Mobile', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 0, memoryUsage: 0 }),
        getMobileStats: async () => ({ totalRecords: 0, memoryUsage: 0 })
      };

      const result = await processor.extractAllData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.totalRecords, 0);
      assert.strictEqual(result.allCleared, true);
      assert.strictEqual(result.gps.recordCount, 0);
      assert.strictEqual(result.mobile.recordCount, 0);
    });

    it('should handle GPS failure gracefully and continue with Mobile', async () => {
      const mockMobileData = [
        { id: 1, deviceId: 'device1', signal: -70, timestamp: '2025-01-25T10:00:00Z' }
      ];

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getMobileStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getListData: async (key) => {
          if (key === 'gps:history:global') {
            throw new Error('GPS extraction failed');
          }
          if (key === 'mobile:history:global') {
            return mockMobileData;
          }
          return [];
        },
        clearListData: async () => true
      };

      const result = await processor.extractAllData();

      // Overall operation should fail due to GPS failure
      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('GPS extraction failed'));
      
      // GPS should have failed
      assert.strictEqual(result.gps.success, false);
      assert.strictEqual(result.gps.recordCount, 0);
      
      // Mobile should not have been attempted due to GPS failure
      assert.strictEqual(result.mobile.success, false);
      assert.strictEqual(result.mobile.recordCount, 0);
    });

    it('should handle Mobile failure after successful GPS extraction', async () => {
      const mockGPSData = [
        { id: 1, lat: 40.7128, lng: -74.0060, timestamp: '2025-01-25T10:00:00Z' }
      ];

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getMobileStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getListData: async (key) => {
          if (key === 'gps:history:global') {
            return mockGPSData;
          }
          if (key === 'mobile:history:global') {
            throw new Error('Mobile extraction failed');
          }
          return [];
        },
        clearListData: async () => true
      };

      const result = await processor.extractAllData();

      // Overall operation should fail due to Mobile failure
      assert.strictEqual(result.success, false);
      
      // GPS should have succeeded
      assert.strictEqual(result.gps.success, true);
      assert.strictEqual(result.gps.recordCount, 1);
      assert.deepStrictEqual(result.gps.data, mockGPSData);
      
      // Mobile should have failed
      assert.strictEqual(result.mobile.success, false);
      assert.strictEqual(result.mobile.recordCount, 0);
    });
  });

  describe('new data arrival during processing', () => {
    it('should allow new GPS data to arrive in clean Redis during processing', async () => {
      const initialGPSData = [
        { id: 1, lat: 40.7128, lng: -74.0060, timestamp: '2025-01-25T10:00:00Z' }
      ];
      
      let redisCleared = false;
      let newDataAdded = false;

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => {
          if (redisCleared && newDataAdded) {
            // After clearing and new data added
            return { totalRecords: 1, memoryUsage: 256 };
          } else if (redisCleared) {
            // After clearing but before new data
            return { totalRecords: 0, memoryUsage: 0 };
          } else {
            // Initial state
            return { totalRecords: 1, memoryUsage: 256 };
          }
        },
        getListData: async (key) => {
          assert.strictEqual(key, 'gps:history:global');
          return initialGPSData;
        },
        clearListData: async (key) => {
          assert.strictEqual(key, 'gps:history:global');
          redisCleared = true;
          
          // Simulate new data arriving immediately after clearing
          setTimeout(() => {
            newDataAdded = true;
          }, 1);
          
          return true;
        },
        addMultipleToList: async (key, data) => {
          // Simulate new data being added to clean Redis
          assert.strictEqual(key, 'gps:history:global');
          assert.strictEqual(redisCleared, true);
          return data.length;
        }
      };

      const result = await processor.extractAndClearGPSData();

      // Verify extraction was successful
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 1);
      assert.deepStrictEqual(result.data, initialGPSData);
      assert.strictEqual(result.cleared, true);

      // Verify Redis was cleared
      assert.strictEqual(redisCleared, true);

      // Simulate adding new data to clean Redis (this would happen in real scenario)
      const newData = [
        { id: 2, lat: 40.7589, lng: -73.9851, timestamp: '2025-01-25T10:05:00Z' }
      ];
      
      await processor.redisRepo.addMultipleToList('gps:history:global', newData);
      newDataAdded = true;

      // Verify new data can be added to clean Redis
      const finalStats = await processor.redisRepo.getGPSStats();
      assert.strictEqual(finalStats.totalRecords, 1); // New data is present
    });

    it('should allow new Mobile data to arrive in clean Redis during processing', async () => {
      const initialMobileData = [
        { id: 1, deviceId: 'device1', signal: -70, timestamp: '2025-01-25T10:00:00Z' }
      ];
      
      let redisCleared = false;

      processor.redisRepo = {
        ...mockRedisRepo,
        getMobileStats: async () => {
          if (redisCleared) {
            return { totalRecords: 0, memoryUsage: 0 };
          } else {
            return { totalRecords: 1, memoryUsage: 256 };
          }
        },
        getListData: async (key) => {
          assert.strictEqual(key, 'mobile:history:global');
          return initialMobileData;
        },
        clearListData: async (key) => {
          assert.strictEqual(key, 'mobile:history:global');
          redisCleared = true;
          return true;
        }
      };

      const result = await processor.extractAndClearMobileData();

      // Verify extraction was successful
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordCount, 1);
      assert.deepStrictEqual(result.data, initialMobileData);
      assert.strictEqual(result.cleared, true);

      // Verify Redis was cleared and is ready for new data
      assert.strictEqual(redisCleared, true);
      assert.strictEqual(result.finalRecords, 0);
    });
  });

  describe('atomic operations verification', () => {
    it('should ensure atomic extraction and clearing for GPS data', async () => {
      const mockData = [
        { id: 1, lat: 40.7128, lng: -74.0060 },
        { id: 2, lat: 40.7589, lng: -73.9851 }
      ];

      let extractionCompleted = false;
      let clearingStarted = false;

      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 2, memoryUsage: 512 }),
        getListData: async (key) => {
          // Simulate extraction taking some time
          await new Promise(resolve => setTimeout(resolve, 10));
          extractionCompleted = true;
          return mockData;
        },
        clearListData: async (key) => {
          // Verify extraction completed before clearing starts
          assert.strictEqual(extractionCompleted, true);
          clearingStarted = true;
          return true;
        }
      };

      const result = await processor.extractAndClearGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(extractionCompleted, true);
      assert.strictEqual(clearingStarted, true);
      assert.strictEqual(result.recordCount, 2);
      assert.strictEqual(result.cleared, true);
    });

    it('should ensure atomic extraction and clearing for Mobile data', async () => {
      const mockData = [
        { id: 1, deviceId: 'device1', signal: -70 }
      ];

      let extractionCompleted = false;
      let clearingStarted = false;

      processor.redisRepo = {
        ...mockRedisRepo,
        getMobileStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getListData: async (key) => {
          await new Promise(resolve => setTimeout(resolve, 10));
          extractionCompleted = true;
          return mockData;
        },
        clearListData: async (key) => {
          assert.strictEqual(extractionCompleted, true);
          clearingStarted = true;
          return true;
        }
      };

      const result = await processor.extractAndClearMobileData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(extractionCompleted, true);
      assert.strictEqual(clearingStarted, true);
      assert.strictEqual(result.recordCount, 1);
      assert.strictEqual(result.cleared, true);
    });
  });

  describe('error handling and recovery', () => {
    it('should handle partial failures in coordinated extraction', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getMobileStats: async () => ({ totalRecords: 1, memoryUsage: 256 }),
        getListData: async (key) => {
          if (key === 'gps:history:global') {
            return [{ id: 1, lat: 40.7128, lng: -74.0060 }];
          }
          throw new Error('Mobile data extraction failed');
        },
        clearListData: async () => true
      };

      const result = await processor.extractAllData();

      // Should fail overall but preserve GPS success info
      assert.strictEqual(result.success, false);
      assert.strictEqual(result.gps.success, true);
      assert.strictEqual(result.mobile.success, false);
    });

    it('should provide detailed error information', async () => {
      processor.redisRepo = {
        ...mockRedisRepo,
        getGPSStats: async () => { throw new Error('Redis connection timeout'); }
      };

      const result = await processor.extractAndClearGPSData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Redis connection timeout'));
      assert.ok(result.totalTime >= 0);
      assert.strictEqual(result.recordCount, 0);
      assert.strictEqual(result.cleared, false);
    });
  });
});