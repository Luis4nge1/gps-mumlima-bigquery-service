import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs/promises';
import path from 'path';
import { BackupManager } from '../src/utils/BackupManager.js';
import { FileUtils } from '../src/utils/FileUtils.js';

describe('BackupManager - Enhanced Local Backup Functionality', () => {
  let backupManager;
  let testBackupPath;

  beforeEach(async () => {
    // Set test environment variables
    process.env.BACKUP_STORAGE_PATH = 'tmp/test-atomic-backups/';
    process.env.BACKUP_MAX_RETRIES = '3';
    process.env.BACKUP_RETENTION_HOURS = '24';

    backupManager = new BackupManager();
    testBackupPath = backupManager.backupPath;

    // Ensure test directory exists and is clean
    await FileUtils.ensureDirectoryExists(testBackupPath);
    
    // Clean up any existing test files
    try {
      const files = await fs.readdir(testBackupPath);
      for (const file of files) {
        if (file.startsWith('backup_') && file.endsWith('.json')) {
          await fs.unlink(path.join(testBackupPath, file));
        }
      }
    } catch (error) {
      // Directory might not exist, ignore
    }
  });

  afterEach(async () => {
    // Clean up test files
    try {
      const files = await fs.readdir(testBackupPath);
      for (const file of files) {
        if (file.startsWith('backup_') && file.endsWith('.json')) {
          await fs.unlink(path.join(testBackupPath, file));
        }
      }
    } catch (error) {
      // Ignore cleanup errors
    }

    // Clean up environment variables
    delete process.env.BACKUP_STORAGE_PATH;
    delete process.env.BACKUP_MAX_RETRIES;
    delete process.env.BACKUP_RETENTION_HOURS;
  });

  describe('saveToLocalBackup()', () => {
    it('should save GPS data to local backup successfully', async () => {
      const testData = [
        { id: 1, lat: 10.123, lng: -74.456, timestamp: '2025-01-25T10:00:00Z' },
        { id: 2, lat: 10.124, lng: -74.457, timestamp: '2025-01-25T10:01:00Z' }
      ];

      const result = await backupManager.saveToLocalBackup(testData, 'gps', { source: 'test' });

      assert.strictEqual(result.success, true);
      assert.match(result.backupId, /^backup_gps_/);
      assert.strictEqual(result.recordCount, 2);
      assert.strictEqual(result.type, 'gps');
      // Normalize paths for cross-platform compatibility
      const normalizedTestPath = testBackupPath.replace(/\\/g, '/').replace(/\/$/, '');
      const normalizedResultPath = result.filePath.replace(/\\/g, '/');
      assert.ok(normalizedResultPath.includes(normalizedTestPath));

      // Verify file was created
      const backupData = await FileUtils.readJsonFile(result.filePath);
      assert.strictEqual(backupData.type, 'gps');
      assert.deepStrictEqual(backupData.data, testData);
      assert.strictEqual(backupData.status, 'pending');
      assert.strictEqual(backupData.metadata.recordCount, 2);
      assert.strictEqual(backupData.metadata.retryCount, 0);
    });

    it('should save mobile data to local backup successfully', async () => {
      const testData = [
        { id: 1, deviceId: 'device123', signal: -70, timestamp: '2025-01-25T10:00:00Z' }
      ];

      const result = await backupManager.saveToLocalBackup(testData, 'mobile');

      assert.strictEqual(result.success, true);
      assert.match(result.backupId, /^backup_mobile_/);
      assert.strictEqual(result.type, 'mobile');

      const backupData = await FileUtils.readJsonFile(result.filePath);
      assert.strictEqual(backupData.type, 'mobile');
      assert.deepStrictEqual(backupData.data, testData);
    });

    it('should reject invalid data types', async () => {
      const result = await backupManager.saveToLocalBackup([], 'invalid');

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('El tipo debe ser "gps" o "mobile"'));
    });

    it('should reject non-array data', async () => {
      const result = await backupManager.saveToLocalBackup('invalid', 'gps');

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Los datos deben ser un array válido'));
    });
  });

  describe('getLocalBackupFiles()', () => {
    it('should return empty array when no backup files exist', async () => {
      const backups = await backupManager.getLocalBackupFiles();

      assert.ok(Array.isArray(backups));
      assert.strictEqual(backups.length, 0);
    });

    it('should return pending backup files', async () => {
      // Create test backup files
      const testData1 = [{ id: 1 }];
      const testData2 = [{ id: 2 }];

      await backupManager.saveToLocalBackup(testData1, 'gps');
      await backupManager.saveToLocalBackup(testData2, 'mobile');

      const backups = await backupManager.getLocalBackupFiles();

      assert.strictEqual(backups.length, 2);
      assert.strictEqual(backups[0].status, 'pending');
      assert.strictEqual(backups[1].status, 'pending');
      assert.ok(backups.some(b => b.type === 'gps'));
      assert.ok(backups.some(b => b.type === 'mobile'));
    });
  });

  describe('processLocalBackupFile()', () => {
    it('should successfully process backup file with successful GCS upload', async () => {
      const testData = [{ id: 1, test: true }];
      const saveResult = await backupManager.saveToLocalBackup(testData, 'gps');

      const backupFile = await backupManager.findBackupFile(saveResult.backupId);

      // Mock successful GCS upload function
      const mockGcsUpload = async (data, type) => ({
        success: true,
        gcsFile: 'gps-data/test-file.json',
        fileName: 'test-file.json'
      });

      const result = await backupManager.processLocalBackupFile(backupFile, mockGcsUpload);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.backupId, saveResult.backupId);
      assert.strictEqual(result.recordsProcessed, 1);
      assert.strictEqual(result.gcsFile, 'gps-data/test-file.json');
      assert.strictEqual(result.type, 'gps');

      // Verify backup status was updated to completed
      const updatedBackup = await backupManager.findBackupFile(saveResult.backupId);
      assert.strictEqual(updatedBackup.status, 'completed');
      assert.ok(updatedBackup.processedAt);
    });

    it('should handle GCS upload failure and update retry count', async () => {
      const testData = [{ id: 1 }];
      const saveResult = await backupManager.saveToLocalBackup(testData, 'gps');

      const backupFile = await backupManager.findBackupFile(saveResult.backupId);

      // Mock failed GCS upload function
      const mockGcsUpload = async () => ({
        success: false,
        error: 'GCS connection failed'
      });

      const result = await backupManager.processLocalBackupFile(backupFile, mockGcsUpload);

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.error, 'GCS connection failed');
      assert.strictEqual(result.retryCount, 1);
      assert.strictEqual(result.willRetry, true);

      // Verify backup status and retry count were updated
      const updatedBackup = await backupManager.findBackupFile(saveResult.backupId);

      assert.strictEqual(updatedBackup.status, 'pending');
      assert.strictEqual(updatedBackup.metadata.retryCount, 1);
      assert.strictEqual(updatedBackup.metadata.errors.length, 1);
    });
  });

  describe('deleteLocalBackup()', () => {
    it('should successfully delete completed backup', async () => {
      const testData = [{ id: 1 }];
      const saveResult = await backupManager.saveToLocalBackup(testData, 'gps');

      // Mark backup as completed
      const backupFile = await backupManager.findBackupFile(saveResult.backupId);
      await backupManager.updateBackupMetadata(backupFile.filePath, {
        status: 'completed',
        processedAt: new Date().toISOString()
      });

      const result = await backupManager.deleteLocalBackup(saveResult.backupId);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.backupId, saveResult.backupId);

      // Verify file was deleted
      const deletedBackup = await backupManager.findBackupFile(saveResult.backupId);
      assert.strictEqual(deletedBackup, null);
    });

    it('should reject deletion of non-completed backup', async () => {
      const testData = [{ id: 1 }];
      const saveResult = await backupManager.saveToLocalBackup(testData, 'gps');

      const result = await backupManager.deleteLocalBackup(saveResult.backupId);

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('No se puede eliminar backup'));
      assert.ok(result.error.includes('pending'));
    });
  });

  describe('findBackupFile()', () => {
    it('should find existing backup by ID', async () => {
      const testData = [{ id: 1 }];
      const saveResult = await backupManager.saveToLocalBackup(testData, 'gps');

      const foundBackup = await backupManager.findBackupFile(saveResult.backupId);

      assert.notStrictEqual(foundBackup, null);
      assert.strictEqual(foundBackup.id, saveResult.backupId);
      assert.strictEqual(foundBackup.filePath, saveResult.filePath);
    });

    it('should return null for non-existent backup', async () => {
      const foundBackup = await backupManager.findBackupFile('non-existent-id');

      assert.strictEqual(foundBackup, null);
    });
  });

  describe('generateShortId()', () => {
    it('should generate unique short IDs', () => {
      const id1 = backupManager.generateShortId();
      const id2 = backupManager.generateShortId();

      assert.strictEqual(typeof id1, 'string');
      assert.strictEqual(typeof id2, 'string');
      assert.strictEqual(id1.length, 6);
      assert.strictEqual(id2.length, 6);
      assert.notStrictEqual(id1, id2);
    });
  });
});