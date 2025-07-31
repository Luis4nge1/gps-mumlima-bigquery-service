import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { SchedulerService } from '../src/services/SchedulerService.js';
import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { logger } from '../src/utils/logger.js';

// Mock dependencies
vi.mock('../src/services/GPSProcessorService.js');
vi.mock('../src/utils/BackupManager.js');
vi.mock('../src/utils/logger.js');
vi.mock('../src/utils/FileCleanup.js');

describe('SchedulerService - Backup Processing', () => {
  let schedulerService;
  let mockProcessor;
  let mockBackupManager;

  beforeEach(() => {
    // Reset mocks
    vi.clearAllMocks();
    
    // Mock GPSProcessorService
    mockProcessor = {
      initialize: vi.fn().mockResolvedValue(),
      processGPSData: vi.fn().mockResolvedValue({
        success: true,
        recordsProcessed: 100,
        processingTime: 5000
      }),
      uploadDataToGCS: vi.fn().mockResolvedValue({
        success: true,
        fileName: 'test-file.json',
        gcsFile: 'gcs://bucket/test-file.json'
      }),
      cleanup: vi.fn().mockResolvedValue(),
      redisRepo: {
        redis: {
          // Mock redis client for distributed lock
        }
      }
    };
    
    // Mock BackupManager
    mockBackupManager = {
      getLocalBackupFiles: vi.fn().mockResolvedValue([]),
      processLocalBackupFile: vi.fn().mockResolvedValue({
        success: true,
        backupId: 'test-backup-1',
        recordsProcessed: 50
      }),
      deleteLocalBackup: vi.fn().mockResolvedValue({
        success: true
      }),
      cleanupCompletedBackups: vi.fn().mockResolvedValue({
        success: true,
        cleaned: 2
      }),
      getBackupStats: vi.fn().mockResolvedValue({
        total: 5,
        pending: 2,
        processing: 0,
        completed: 2,
        failed: 1,
        totalRecords: 150
      }),
      getAllBackupFiles: vi.fn().mockResolvedValue([])
    };

    // Mock constructors
    GPSProcessorService.mockImplementation(() => mockProcessor);
    BackupManager.mockImplementation(() => mockBackupManager);

    schedulerService = new SchedulerService();
  });

  afterEach(() => {
    if (schedulerService.isRunning) {
      schedulerService.stop();
    }
  });

  describe('processLocalBackups', () => {
    it('should process pending backup files successfully', async () => {
      // Arrange
      const mockBackupFiles = [
        {
          id: 'backup_gps_20250125_001',
          type: 'gps',
          data: [{ lat: 1, lng: 1 }],
          metadata: { retryCount: 0, maxRetries: 3 }
        },
        {
          id: 'backup_mobile_20250125_002',
          type: 'mobile',
          data: [{ device: 'test' }],
          metadata: { retryCount: 1, maxRetries: 3 }
        }
      ];

      mockBackupManager.getLocalBackupFiles.mockResolvedValue(mockBackupFiles);
      mockBackupManager.processLocalBackupFile.mockResolvedValueOnce({
        success: true,
        backupId: 'backup_gps_20250125_001',
        recordsProcessed: 1
      }).mockResolvedValueOnce({
        success: true,
        backupId: 'backup_mobile_20250125_002',
        recordsProcessed: 1
      });

      // Act
      const result = await schedulerService.processLocalBackups();

      // Assert
      expect(result.success).toBe(true);
      expect(result.processed).toBe(2);
      expect(result.failed).toBe(0);
      expect(result.alerts).toHaveLength(0);
      
      expect(mockBackupManager.getLocalBackupFiles).toHaveBeenCalledOnce();
      expect(mockBackupManager.processLocalBackupFile).toHaveBeenCalledTimes(2);
      expect(mockBackupManager.deleteLocalBackup).toHaveBeenCalledTimes(2);
    });

    it('should handle backup processing failures and generate alerts', async () => {
      // Arrange
      const mockBackupFiles = [
        {
          id: 'backup_gps_20250125_001',
          type: 'gps',
          data: [{ lat: 1, lng: 1 }],
          metadata: { retryCount: 2, maxRetries: 3 }
        }
      ];

      mockBackupManager.getLocalBackupFiles.mockResolvedValue(mockBackupFiles);
      mockBackupManager.processLocalBackupFile.mockResolvedValue({
        success: false,
        backupId: 'backup_gps_20250125_001',
        error: 'GCS upload failed',
        retryCount: 3,
        maxRetries: 3,
        willRetry: false
      });

      // Act
      const result = await schedulerService.processLocalBackups();

      // Assert
      expect(result.success).toBe(false);
      expect(result.processed).toBe(0);
      expect(result.failed).toBe(1);
      expect(result.alerts).toHaveLength(1);
      expect(result.alerts[0]).toContain('backup_gps_20250125_001 falló definitivamente');
      
      expect(mockBackupManager.deleteLocalBackup).not.toHaveBeenCalled();
    });

    it('should return success when no backup files are pending', async () => {
      // Arrange
      mockBackupManager.getLocalBackupFiles.mockResolvedValue([]);

      // Act
      const result = await schedulerService.processLocalBackups();

      // Assert
      expect(result.success).toBe(true);
      expect(result.processed).toBe(0);
      expect(result.failed).toBe(0);
      expect(result.alerts).toHaveLength(0);
    });
  });

  describe('executeBackupCleanup', () => {
    it('should execute backup cleanup successfully', async () => {
      // Arrange
      mockBackupManager.cleanupCompletedBackups.mockResolvedValue({
        success: true,
        cleaned: 3
      });

      // Act
      await schedulerService.executeBackupCleanup();

      // Assert
      expect(mockBackupManager.cleanupCompletedBackups).toHaveBeenCalledOnce();
    });

    it('should handle cleanup errors gracefully', async () => {
      // Arrange
      mockBackupManager.cleanupCompletedBackups.mockResolvedValue({
        success: false,
        error: 'Cleanup failed'
      });

      // Act
      await schedulerService.executeBackupCleanup();

      // Assert
      expect(mockBackupManager.cleanupCompletedBackups).toHaveBeenCalledOnce();
      // Should not throw error
    });
  });

  describe('getStatus', () => {
    it('should include backup information in status', async () => {
      // Act
      const status = await schedulerService.getStatus();

      // Assert
      expect(status.backups).toBeDefined();
      expect(status.backups.cleanupIntervalMinutes).toBeDefined();
      expect(status.backups.stats).toBeDefined();
      expect(status.backups.stats.total).toBe(5);
      expect(status.backups.stats.pending).toBe(2);
    });
  });

  describe('getBackupAlerts', () => {
    it('should return alerts for failed backups', async () => {
      // Arrange
      const mockFailedBackups = [
        {
          id: 'backup_gps_failed_001',
          type: 'gps',
          status: 'failed',
          timestamp: '2025-01-25T10:00:00.000Z',
          metadata: { retryCount: 3, maxRetries: 3, recordCount: 100 },
          error: { message: 'GCS upload failed permanently' }
        }
      ];

      mockBackupManager.getAllBackupFiles.mockResolvedValue(mockFailedBackups);

      // Act
      const alerts = await schedulerService.getBackupAlerts();

      // Assert
      expect(alerts).toHaveLength(1);
      expect(alerts[0].backupId).toBe('backup_gps_failed_001');
      expect(alerts[0].type).toBe('gps');
      expect(alerts[0].retryCount).toBe(3);
      expect(alerts[0].maxRetries).toBe(3);
      expect(alerts[0].lastError).toBe('GCS upload failed permanently');
    });

    it('should return empty array when no failed backups exist', async () => {
      // Arrange
      mockBackupManager.getAllBackupFiles.mockResolvedValue([]);

      // Act
      const alerts = await schedulerService.getBackupAlerts();

      // Assert
      expect(alerts).toHaveLength(0);
    });
  });

  describe('processBackupsManually', () => {
    it('should process backups manually', async () => {
      // Arrange
      mockBackupManager.getLocalBackupFiles.mockResolvedValue([]);

      // Act
      const result = await schedulerService.processBackupsManually();

      // Assert
      expect(result.success).toBe(true);
      expect(result.processed).toBe(0);
    });
  });

  describe('scheduler integration with backups', () => {
    it('should process backups before new data in scheduled job', async () => {
      // Arrange
      const mockBackupFiles = [
        {
          id: 'backup_gps_20250125_001',
          type: 'gps',
          data: [{ lat: 1, lng: 1 }],
          metadata: { retryCount: 0, maxRetries: 3 }
        }
      ];

      mockBackupManager.getLocalBackupFiles.mockResolvedValue(mockBackupFiles);
      mockBackupManager.processLocalBackupFile.mockResolvedValue({
        success: true,
        backupId: 'backup_gps_20250125_001',
        recordsProcessed: 1
      });

      // Mock redis client to be null to avoid distributed lock
      mockProcessor.redisRepo.redis = null;

      // Act
      await schedulerService.executeScheduledJob();

      // Assert
      expect(mockBackupManager.getLocalBackupFiles).toHaveBeenCalled();
      expect(mockBackupManager.processLocalBackupFile).toHaveBeenCalled();
      expect(mockProcessor.processGPSData).toHaveBeenCalled();
    });
  });
});