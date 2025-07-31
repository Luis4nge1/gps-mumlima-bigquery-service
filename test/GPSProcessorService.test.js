import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import fs from 'fs/promises';
import path from 'path';

describe('GPSProcessorService', () => {
  let processor;
  let testTmpPath;

  beforeEach(async () => {
    // Configurar variables de entorno para tests
    process.env.GCS_SIMULATION_MODE = 'true';
    process.env.BIGQUERY_SIMULATION_MODE = 'true';
    process.env.GCS_CLEANUP_PROCESSED_FILES = 'true';
    
    // Crear directorio temporal para tests
    testTmpPath = path.join('tmp', 'test-gps-processor');
    await fs.mkdir(testTmpPath, { recursive: true });

    processor = new GPSProcessorService();
    await processor.initialize();
  });

  afterEach(async () => {
    await processor.cleanup();
    
    // Limpiar directorio de tests
    try {
      await fs.rm(testTmpPath, { recursive: true, force: true });
    } catch (error) {
      // Ignorar errores de limpieza
    }
    
    delete process.env.GCS_SIMULATION_MODE;
    delete process.env.BIGQUERY_SIMULATION_MODE;
    delete process.env.GCS_CLEANUP_PROCESSED_FILES;
  });

  describe('initialize', () => {
    it('should initialize all adapters and services', async () => {
      const newProcessor = new GPSProcessorService();
      
      await newProcessor.initialize();
      
      const status = await newProcessor.healthCheck();
      assert.strictEqual(status.healthy, true);
      assert.strictEqual(status.services.gcs, 'healthy');
      assert.strictEqual(status.services.bigQuery, 'healthy');
      assert.strictEqual(status.services.recovery, 'healthy');
      
      await newProcessor.cleanup();
    });
  });

  describe('processGPSData', () => {
    it('should handle empty Redis data', async () => {
      // Mock Redis repository to return empty data
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 0);
      assert.strictEqual(result.message, 'No new data to process');
    });

    it('should process GPS data through complete flow', async () => {
      // Mock Redis data
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 1);
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 1);
      assert.strictEqual(result.results.mobile.recordsProcessed, 0);
    });

    it('should process Mobile data through complete flow', async () => {
      // Mock Mobile data
      const mockMobileData = [
        JSON.stringify({
          userId: 'user_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z',
          name: 'Juan Pérez',
          email: 'juan@example.com'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 1);
      assert.strictEqual(result.results.gps.recordsProcessed, 0);
      assert.ok(result.results.mobile.success);
      assert.strictEqual(result.results.mobile.recordsProcessed, 1);
    });

    it('should process both GPS and Mobile data', async () => {
      // Mock mixed data
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      const mockMobileData = [
        JSON.stringify({
          userId: 'user_001',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-15T10:31:00Z',
          name: 'María García',
          email: 'maria@example.com'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2);
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 1);
      assert.ok(result.results.mobile.success);
      assert.strictEqual(result.results.mobile.recordsProcessed, 1);
    });

    it('should handle processing already in progress', async () => {
      processor.isProcessing = true;

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.error, 'Processing already in progress');
    });

    it('should handle data separation errors', async () => {
      // Mock invalid data that will cause separation to fail
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => ['invalid json data'];

      // Mock DataSeparator to fail
      processor.dataSeparator.separateDataByType = async () => ({
        success: false,
        error: 'Data separation failed'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.error, 'Data separation failed');
    });

    it('should handle GCS upload failures', async () => {
      // Mock valid GPS data
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      // Mock GCS adapter to fail
      processor.gcsAdapter.uploadJSON = async () => ({
        success: false,
        error: 'GCS upload failed'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.error, 'GCS upload failed');
      assert.strictEqual(result.results.gps.stage, 'gcs_upload');
    });

    it('should handle BigQuery processing failures', async () => {
      // Mock valid GPS data
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      // Mock BigQuery processor to fail
      processor.bigQueryProcessor.processGCSFile = async () => ({
        success: false,
        error: 'BigQuery processing failed'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.error, 'BigQuery processing failed');
      assert.strictEqual(result.results.gps.stage, 'bigquery_processing');
    });
  });

  describe('processRecoveryFiles', () => {
    it('should process recovery files successfully', async () => {
      // Mock recovery manager
      processor.recoveryManager.processGCSPendingFiles = async () => ({
        success: true,
        processed: 2,
        failed: 0,
        total: 2
      });

      const result = await processor.processRecoveryFiles();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.processed, 2);
    });

    it('should handle no pending recovery files', async () => {
      // Mock recovery manager with no pending files
      processor.recoveryManager.processGCSPendingFiles = async () => ({
        success: true,
        processed: 0,
        failed: 0,
        total: 0
      });

      const result = await processor.processRecoveryFiles();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.processed, 0);
    });

    it('should handle recovery errors gracefully', async () => {
      // Mock recovery manager to fail
      processor.recoveryManager.processGCSPendingFiles = async () => {
        throw new Error('Recovery failed');
      };

      const result = await processor.processRecoveryFiles();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.processed, 0);
      assert.strictEqual(result.error, 'Recovery failed');
    });
  });

  describe('getDataFromRedis', () => {
    it('should get GPS and Mobile data from Redis', async () => {
      const mockGPSData = ['gps_data_1', 'gps_data_2'];
      const mockMobileData = ['mobile_data_1'];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 2 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };

      const result = await processor.getDataFromRedis();

      assert.strictEqual(result.totalRecords, 3);
      assert.strictEqual(result.allData.length, 3);
      assert.deepStrictEqual(result.allData, [...mockGPSData, ...mockMobileData]);
    });

    it('should handle empty Redis data', async () => {
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });

      const result = await processor.getDataFromRedis();

      assert.strictEqual(result.totalRecords, 0);
      assert.strictEqual(result.allData.length, 0);
    });

    it('should respect max records limit', async () => {
      // Mock large dataset
      const largeDataset = Array.from({ length: 15000 }, (_, i) => `data_${i}`);
      
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 15000 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async (key, limit) => {
        return largeDataset.slice(0, limit);
      };

      const result = await processor.getDataFromRedis();

      // Should be limited to config.gps.batchSize * 10 (default 1000 * 10 = 10000)
      assert.ok(result.totalRecords <= 10000);
    });
  });

  describe('separateDataByType', () => {
    it('should separate mixed data successfully', async () => {
      const mixedData = [
        JSON.stringify({ deviceId: 'device1', lat: -12.0464, lng: -77.0428 }),
        JSON.stringify({ userId: 'user1', lat: -12.0500, lng: -77.0500, name: 'Test', email: 'test@example.com' })
      ];

      const result = await processor.separateDataByType(mixedData);

      assert.strictEqual(result.success, true);
      assert.ok(result.gpsData.length > 0 || result.mobileData.length > 0);
      assert.ok(result.stats);
    });

    it('should handle separation errors', async () => {
      // Mock DataSeparator to fail
      processor.dataSeparator.separateDataByType = async () => ({
        success: false,
        error: 'Separation failed'
      });

      const result = await processor.separateDataByType(['invalid data']);

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.error, 'Separation failed');
    });
  });

  describe('validateDataByType', () => {
    it('should validate GPS data', async () => {
      const gpsData = [
        { deviceId: 'device1', lat: -12.0464, lng: -77.0428, timestamp: '2025-01-15T10:30:00Z' }
      ];

      const result = await processor.validateDataByType('gps', gpsData);

      assert.ok(result.isValid !== undefined);
      assert.ok(Array.isArray(result.validData));
      assert.ok(result.stats);
    });

    it('should validate Mobile data', async () => {
      const mobileData = [
        { 
          userId: 'user1', 
          lat: -12.0464, 
          lng: -77.0428, 
          timestamp: '2025-01-15T10:30:00Z',
          name: 'Test User',
          email: 'test@example.com'
        }
      ];

      const result = await processor.validateDataByType('mobile', mobileData);

      assert.ok(result.isValid !== undefined);
      assert.ok(Array.isArray(result.validData));
      assert.ok(result.stats);
    });

    it('should handle unsupported data type', async () => {
      const result = await processor.validateDataByType('unknown', []);

      assert.strictEqual(result.isValid, false);
      assert.ok(result.errors.length > 0);
      assert.ok(result.errors[0].includes('no soportado'));
    });
  });

  describe('getProcessorStats', () => {
    it('should return comprehensive processor statistics', async () => {
      // Mock repository stats
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 100 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 50 });
      processor.metrics.getMetrics = async () => ({ lastProcessing: new Date().toISOString() });

      const stats = await processor.getProcessorStats();

      assert.ok(stats.redis);
      assert.strictEqual(stats.redis.total, 150);
      assert.ok(stats.gcs);
      assert.ok(stats.bigQuery);
      assert.ok(stats.recovery);
      assert.ok(stats.processor);
      assert.strictEqual(stats.processor.flowType, 'redis_gcs_bigquery');
    });

    it('should handle stats errors gracefully', async () => {
      // Mock error in getting stats
      processor.redisRepo.getGPSStats = async () => {
        throw new Error('Redis error');
      };

      const stats = await processor.getProcessorStats();

      assert.ok(stats.error);
      assert.strictEqual(stats.processor.flowType, 'redis_gcs_bigquery');
    });
  });

  describe('healthCheck', () => {
    it('should return healthy status when all services are healthy', async () => {
      const health = await processor.healthCheck();

      assert.strictEqual(health.healthy, true);
      assert.strictEqual(health.services.redis, 'healthy');
      assert.strictEqual(health.services.gcs, 'healthy');
      assert.strictEqual(health.services.bigQuery, 'healthy');
      assert.strictEqual(health.services.recovery, 'healthy');
      assert.ok(health.details);
      assert.ok(health.timestamp);
    });

    it('should return unhealthy status when Redis fails', async () => {
      // Mock Redis failure
      processor.redisRepo.ping = async () => false;

      const health = await processor.healthCheck();

      assert.strictEqual(health.healthy, false);
      assert.strictEqual(health.services.redis, 'unhealthy');
    });

    it('should handle health check errors', async () => {
      // Mock error in health check
      processor.redisRepo.ping = async () => {
        throw new Error('Health check failed');
      };

      const health = await processor.healthCheck();

      assert.strictEqual(health.healthy, false);
      assert.ok(health.error);
    });
  });

  describe('integration scenarios', () => {
    it('should handle complete end-to-end processing', async () => {
      // Mock complete dataset
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_integration',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      const mockMobileData = [
        JSON.stringify({
          userId: 'user_integration',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-15T10:31:00Z',
          name: 'Integration Test',
          email: 'integration@example.com'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      // Mock recovery manager with no pending files
      processor.recoveryManager.processGCSPendingFiles = async () => ({
        success: true,
        processed: 0,
        failed: 0,
        total: 0
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2);
      assert.ok(result.results.gps.success);
      assert.ok(result.results.mobile.success);
      assert.ok(result.processingTime > 0);
      assert.ok(result.separationStats);
    });

    it('should handle recovery processing before new data', async () => {
      // Mock recovery files
      processor.recoveryManager.processGCSPendingFiles = async () => ({
        success: true,
        processed: 3,
        failed: 0,
        total: 3,
        results: [
          { success: true, recordsProcessed: 100 },
          { success: true, recordsProcessed: 50 },
          { success: true, recordsProcessed: 75 }
        ]
      });

      // Mock empty Redis data
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 0);
      assert.strictEqual(result.message, 'No new data to process');
    });
  });
});