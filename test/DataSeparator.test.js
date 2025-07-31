import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert';
import { DataSeparator } from '../src/services/DataSeparator.js';

describe('DataSeparator', () => {
  let dataSeparator;

  beforeEach(() => {
    dataSeparator = new DataSeparator();
  });

  describe('separateDataByType', () => {
    it('should separate GPS and Mobile data correctly', () => {
      const gpsHistoryData = [
        JSON.stringify({
          deviceId: 'device1',
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z'
        }),
        JSON.stringify({
          deviceId: 'device2',
          latitude: 51.5074,
          longitude: -0.1278,
          timestamp: '2024-01-15T10:40:00Z'
        })
      ];

      const mobileHistoryData = [
        JSON.stringify({
          userId: 'user1',
          latitude: 34.0522,
          longitude: -118.2437,
          timestamp: '2024-01-15T10:35:00Z',
          name: 'John Doe',
          email: 'john@example.com'
        })
      ];

      const result = dataSeparator.separateDataByType(gpsHistoryData, mobileHistoryData);

      assert.strictEqual(result.gps.data.length, 2);
      assert.strictEqual(result.mobile.data.length, 1);
      assert.strictEqual(result.gps.metadata.recordCount, 2);
      assert.strictEqual(result.mobile.metadata.recordCount, 1);
      assert.strictEqual(result.gps.metadata.type, 'gps');
      assert.strictEqual(result.mobile.metadata.type, 'mobile');
      assert.strictEqual(result.gps.metadata.source, 'redis:gps:history:global');
      assert.strictEqual(result.mobile.metadata.source, 'redis:mobile:history:global');
    });

    it('should handle empty data', () => {
      const result = dataSeparator.separateDataByType([], []);

      assert.strictEqual(result.gps.data.length, 0);
      assert.strictEqual(result.mobile.data.length, 0);
      assert.strictEqual(result.gps.metadata.recordCount, 0);
      assert.strictEqual(result.mobile.metadata.recordCount, 0);
    });

    it('should handle only GPS data', () => {
      const gpsHistoryData = [
        JSON.stringify({
          deviceId: 'device1',
          latitude: 40.7128,
          longitude: -74.0060
        })
      ];

      const result = dataSeparator.separateDataByType(gpsHistoryData, []);

      assert.strictEqual(result.gps.data.length, 1);
      assert.strictEqual(result.mobile.data.length, 0);
    });

    it('should handle invalid JSON gracefully', () => {
      const gpsHistoryData = ['invalid json'];
      const mobileHistoryData = [
        JSON.stringify({
          userId: 'user1',
          latitude: 34.0522,
          longitude: -118.2437,
          name: 'John Doe',
          email: 'john@example.com'
        })
      ];

      const result = dataSeparator.separateDataByType(gpsHistoryData, mobileHistoryData);

      assert.strictEqual(result.gps.data.length, 0);
      assert.strictEqual(result.mobile.data.length, 1);
    });
  });

  describe('validateGPSData', () => {
    it('should validate correct GPS data', () => {
      const gpsData = [
        {
          deviceId: 'device1',
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z'
        },
        {
          deviceId: 'device2',
          latitude: 34.0522,
          longitude: -118.2437,
          timestamp: '2024-01-15T10:35:00Z'
        }
      ];

      const result = dataSeparator.validateGPSData(gpsData);

      assert.strictEqual(result.isValid, true);
      assert.strictEqual(result.validData.length, 2);
      assert.strictEqual(result.invalidData.length, 0);
      assert.strictEqual(result.stats.validationRate, '100.00');
    });

    it('should reject GPS data with invalid coordinates', () => {
      const gpsData = [
        {
          deviceId: 'device1',
          latitude: 91, // Invalid latitude
          longitude: -74.0060
        },
        {
          deviceId: 'device2',
          latitude: 34.0522,
          longitude: 181 // Invalid longitude
        }
      ];

      const result = dataSeparator.validateGPSData(gpsData);

      assert.strictEqual(result.isValid, false);
      assert.strictEqual(result.validData.length, 0);
      assert.strictEqual(result.invalidData.length, 2);
      assert.strictEqual(result.stats.validationRate, '0.00');
    });

    it('should handle mixed valid and invalid GPS data', () => {
      const gpsData = [
        {
          deviceId: 'device1',
          latitude: 40.7128,
          longitude: -74.0060
        },
        {
          deviceId: 'device2',
          latitude: 'invalid', // Invalid latitude
          longitude: -118.2437
        }
      ];

      const result = dataSeparator.validateGPSData(gpsData);

      assert.strictEqual(result.isValid, true);
      assert.strictEqual(result.validData.length, 1);
      assert.strictEqual(result.invalidData.length, 1);
      assert.strictEqual(result.stats.validationRate, '50.00');
    });
  });

  describe('validateMobileData', () => {
    it('should validate correct Mobile data', () => {
      const mobileData = [
        {
          userId: 'user1',
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z',
          name: 'John Doe',
          email: 'john@example.com'
        },
        {
          userId: 'user2',
          latitude: 34.0522,
          longitude: -118.2437,
          timestamp: '2024-01-15T10:35:00Z',
          name: 'Jane Smith',
          email: 'jane@example.com'
        }
      ];

      const result = dataSeparator.validateMobileData(mobileData);

      assert.strictEqual(result.isValid, true);
      assert.strictEqual(result.validData.length, 2);
      assert.strictEqual(result.invalidData.length, 0);
      assert.strictEqual(result.stats.validationRate, '100.00');
    });

    it('should reject Mobile data without required fields', () => {
      const mobileData = [
        {
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z'
          // Missing userId, name, email
        }
      ];

      const result = dataSeparator.validateMobileData(mobileData);

      assert.strictEqual(result.isValid, false);
      assert.strictEqual(result.validData.length, 0);
      assert.strictEqual(result.invalidData.length, 1);
      assert.strictEqual(result.stats.validationRate, '0.00');
    });

    it('should validate email format', () => {
      const mobileData = [
        {
          userId: 'user1',
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z',
          name: 'John Doe',
          email: 'invalid-email'
        }
      ];

      const result = dataSeparator.validateMobileData(mobileData);

      assert.strictEqual(result.isValid, false);
      assert.strictEqual(result.validData.length, 0);
      assert.strictEqual(result.invalidData.length, 1);
    });

    it('should validate name length', () => {
      const mobileData = [
        {
          userId: 'user1',
          latitude: 40.7128,
          longitude: -74.0060,
          timestamp: '2024-01-15T10:30:00Z',
          name: 'a'.repeat(101), // Too long
          email: 'john@example.com'
        }
      ];

      const result = dataSeparator.validateMobileData(mobileData);

      assert.strictEqual(result.isValid, false);
      assert.strictEqual(result.validData.length, 0);
      assert.strictEqual(result.invalidData.length, 1);
    });
  });

  describe('formatForGCS', () => {
    it('should format GPS data for GCS correctly', () => {
      const gpsData = [
        {
          deviceId: 'device1',
          lat: 40.7128,
          lng: -74.0060,
          timestamp: '2024-01-15T10:30:00Z'
        }
      ];

      const result = dataSeparator.formatForGCS(gpsData, 'gps');

      assert.strictEqual(result.metadata.type, 'gps');
      assert.strictEqual(result.metadata.recordCount, 1);
      assert.strictEqual(result.metadata.source, 'redis:gps:history:global');
      assert.strictEqual(result.metadata.formatVersion, '1.0');
      assert.strictEqual(result.data.length, 1);
      assert.strictEqual(result.data[0].deviceId, 'device1');
    });

    it('should format Mobile data for GCS correctly', () => {
      const mobileData = [
        {
          userId: 'user1',
          lat: 40.7128,
          lng: -74.0060,
          timestamp: '2024-01-15T10:30:00Z',
          name: 'John Doe',
          email: 'john@example.com'
        }
      ];

      const result = dataSeparator.formatForGCS(mobileData, 'mobile');

      assert.strictEqual(result.metadata.type, 'mobile');
      assert.strictEqual(result.metadata.recordCount, 1);
      assert.strictEqual(result.metadata.source, 'redis:mobile:history:global');
      assert.strictEqual(result.metadata.formatVersion, '1.0');
      assert.strictEqual(result.data.length, 1);
      assert.strictEqual(result.data[0].userId, 'user1');
    });

    it('should handle empty data arrays', () => {
      const result = dataSeparator.formatForGCS([], 'gps');

      assert.strictEqual(result.metadata.type, 'gps');
      assert.strictEqual(result.metadata.recordCount, 0);
      assert.strictEqual(result.data.length, 0);
    });
  });

  describe('validateMobileRecord', () => {
    it('should validate a single Mobile record correctly', () => {
      const record = {
        userId: 'user1',
        latitude: 40.7128,
        longitude: -74.0060,
        timestamp: '2024-01-15T10:30:00Z',
        name: 'John Doe',
        email: 'john@example.com'
      };

      const result = dataSeparator.validateMobileRecord(record);

      assert.strictEqual(result.isValid, true);
      assert.strictEqual(result.errors.length, 0);
      assert.strictEqual(result.cleanedData.userId, 'user1');
      assert.strictEqual(result.cleanedData.lat, 40.7128);
      assert.strictEqual(result.cleanedData.lng, -74.0060);
      assert.strictEqual(result.cleanedData.name, 'John Doe');
      assert.strictEqual(result.cleanedData.email, 'john@example.com');
    });

    it('should reject record without required fields', () => {
      const record = {
        latitude: 40.7128,
        longitude: -74.0060
        // Missing userId, name, email
      };

      const result = dataSeparator.validateMobileRecord(record);

      assert.strictEqual(result.isValid, false);
      assert.ok(result.errors.some(error => error.includes('userId is required')));
    });

    it('should handle string input', () => {
      const record = JSON.stringify({
        userId: 'user1',
        latitude: 40.7128,
        longitude: -74.0060,
        name: 'John Doe',
        email: 'john@example.com'
      });

      const result = dataSeparator.validateMobileRecord(record);

      assert.strictEqual(result.isValid, true);
      assert.strictEqual(result.cleanedData.userId, 'user1');
    });

    it('should handle invalid JSON string', () => {
      const record = 'invalid json';

      const result = dataSeparator.validateMobileRecord(record);

      assert.strictEqual(result.isValid, false);
      assert.ok(result.errors.some(error => error.includes('JSON parse error')));
    });
  });

  describe('utility methods', () => {
    it('should generate unique processing IDs', () => {
      const id1 = dataSeparator.generateProcessingId('gps');
      const id2 = dataSeparator.generateProcessingId('gps');

      assert.ok(id1.startsWith('gps_'));
      assert.ok(id2.startsWith('gps_'));
      assert.notStrictEqual(id1, id2);
    });

    it('should generate unique record IDs', () => {
      const record = {
        timestamp: '2024-01-15T10:30:00Z',
        deviceId: 'device1'
      };

      const id1 = dataSeparator.generateRecordId('gps', record, 0);
      const id2 = dataSeparator.generateRecordId('gps', record, 1);

      assert.ok(id1.startsWith('gps_device1_'));
      assert.ok(id2.startsWith('gps_device1_'));
      assert.notStrictEqual(id1, id2);
    });

    it('should parse Redis values correctly', () => {
      const jsonString = JSON.stringify({ test: 'value' });
      const result1 = dataSeparator.parseRedisValue(jsonString);
      
      assert.deepStrictEqual(result1, { test: 'value' });

      const objectValue = { test: 'value' };
      const result2 = dataSeparator.parseRedisValue(objectValue);
      
      assert.deepStrictEqual(result2, { test: 'value' });

      const invalidJson = 'invalid json';
      const result3 = dataSeparator.parseRedisValue(invalidJson);
      
      assert.strictEqual(result3, null);
    });

    it('should return correct stats', () => {
      const stats = dataSeparator.getStats();

      assert.ok(stats.redisKeys);
      assert.ok(stats.redisKeys.gps);
      assert.ok(stats.redisKeys.mobile);
      assert.ok(stats.validators);
      assert.ok(stats.validators.gps);
      assert.ok(stats.validators.mobile);
      assert.deepStrictEqual(stats.validators.mobile.requiredFields, ['userId', 'lat', 'lng', 'timestamp', 'name', 'email']);
      assert.strictEqual(stats.redisKeys.gps, 'gps:history:global');
      assert.strictEqual(stats.redisKeys.mobile, 'mobile:history:global');
    });
  });

  describe('cleanup', () => {
    it('should cleanup resources', async () => {
      await dataSeparator.cleanup();
      
      // Verificar que el cleanup se ejecute sin errores
      assert.ok(true);
    });
  });
});