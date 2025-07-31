import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { GCSRecoveryManager } from '../src/services/GCSRecoveryManager.js';
import fs from 'fs/promises';
import path from 'path';

describe('GCSRecoveryManager', () => {
  let recoveryManager;
  let mockGCSAdapter;
  let mockBigQueryProcessor;
  let testRecoveryPath;

  beforeEach(async () => {
    // Configurar variables de entorno para tests
    process.env.GCS_MAX_RETRY_ATTEMPTS = '3';
    process.env.GCS_CLEANUP_PROCESSED_FILES = 'true';
    
    // Crear mocks
    mockGCSAdapter = {
      bucketName: 'test-bucket',
      uploadJSON: async (data, fileName, metadata) => ({
        success: true,
        gcsUri: `gs://test-bucket/${fileName}`,
        fileName,
        size: JSON.stringify(data).length
      }),
      listFiles: async (options = {}) => {
        if (options.prefix === 'existing-file.json') {
          return [{ name: 'existing-file.json', size: 1000 }];
        }
        return [];
      },
      deleteFile: async (fileName) => ({
        success: true,
        fileName
      }),
      getStatus: async () => ({
        initialized: true,
        simulationMode: true
      })
    };

    mockBigQueryProcessor = {
      processGCSFile: async (gcsUri, dataType, metadata) => ({
        success: true,
        jobId: `test_job_${Date.now()}`,
        recordsProcessed: metadata.recordCount || 100,
        gcsUri,
        dataType
      }),
      getStatus: async () => ({
        initialized: true,
        simulationMode: true
      })
    };

    // Crear directorio temporal para tests
    testRecoveryPath = path.join('tmp', 'test-recovery');
    await fs.mkdir(testRecoveryPath, { recursive: true });

    recoveryManager = new GCSRecoveryManager(mockGCSAdapter, mockBigQueryProcessor);
    
    // Sobrescribir rutas para tests
    recoveryManager.backupPath = testRecoveryPath;
    recoveryManager.gcsRecoveryPath = path.join(testRecoveryPath, 'gcs-recovery');
    
    await recoveryManager.initialize();
  });

  afterEach(async () => {
    await recoveryManager.cleanup();
    
    // Limpiar directorio de tests
    try {
      await fs.rm(testRecoveryPath, { recursive: true, force: true });
    } catch (error) {
      // Ignorar errores de limpieza
    }
    
    delete process.env.GCS_MAX_RETRY_ATTEMPTS;
    delete process.env.GCS_CLEANUP_PROCESSED_FILES;
  });

  describe('initialize', () => {
    it('should initialize GCS recovery manager', async () => {
      const newManager = new GCSRecoveryManager(mockGCSAdapter, mockBigQueryProcessor);
      newManager.gcsRecoveryPath = path.join(testRecoveryPath, 'new-recovery');
      
      await newManager.initialize();
      
      const stats = await fs.stat(newManager.gcsRecoveryPath);
      assert.ok(stats.isDirectory());
    });
  });

  describe('createGCSBackup', () => {
    it('should create GCS backup with metadata', async () => {
      const gcsFileName = 'test-gps-data.json';
      const metadata = {
        dataType: 'gps',
        recordCount: 150,
        source: 'redis:gps:history:global',
        processingId: 'gps_test_123'
      };
      const originalData = [
        { deviceId: 'device1', lat: -12.0464, lng: -77.0428, timestamp: '2025-01-15T10:30:00Z' }
      ];

      const result = await recoveryManager.createGCSBackup(gcsFileName, metadata, originalData);

      assert.strictEqual(result.success, true);
      assert.ok(result.backupId);
      assert.strictEqual(result.gcsFileName, gcsFileName);
      assert.ok(result.filePath);

      // Verificar que el archivo fue creado
      const backupData = JSON.parse(await fs.readFile(result.filePath, 'utf8'));
      assert.strictEqual(backupData.type, 'gcs_recovery');
      assert.strictEqual(backupData.status, 'pending');
      assert.strictEqual(backupData.gcsFileName, gcsFileName);
      assert.strictEqual(backupData.metadata.dataType, 'gps');
      assert.strictEqual(backupData.metadata.recordCount, 150);
      assert.deepStrictEqual(backupData.originalData, originalData);
    });

    it('should handle backup creation errors', async () => {
      // Crear manager con ruta inválida para Windows
      const invalidManager = new GCSRecoveryManager(mockGCSAdapter, mockBigQueryProcessor);
      invalidManager.gcsRecoveryPath = 'Z:\\invalid\\path\\that\\does\\not\\exist';

      const result = await invalidManager.createGCSBackup('test.json', {});

      assert.strictEqual(result.success, false);
      assert.ok(result.error);
    });
  });

  describe('getGCSPendingFiles', () => {
    it('should return pending GCS files', async () => {
      // Crear algunos archivos de recovery
      await recoveryManager.createGCSBackup('file1.json', { dataType: 'gps', recordCount: 100 });
      await recoveryManager.createGCSBackup('file2.json', { dataType: 'mobile', recordCount: 50 });

      const pendingFiles = await recoveryManager.getGCSPendingFiles();

      assert.strictEqual(pendingFiles.length, 2);
      assert.ok(pendingFiles.every(file => file.status === 'pending'));
      assert.ok(pendingFiles.every(file => file.type === 'gcs_recovery'));
    });

    it('should return empty array when no pending files', async () => {
      const pendingFiles = await recoveryManager.getGCSPendingFiles();

      assert.strictEqual(pendingFiles.length, 0);
      assert.ok(Array.isArray(pendingFiles));
    });

    it('should filter out completed and failed files', async () => {
      // Crear archivo pendiente
      const result1 = await recoveryManager.createGCSBackup('pending.json', { dataType: 'gps' });
      
      // Crear archivo completado
      const result2 = await recoveryManager.createGCSBackup('completed.json', { dataType: 'mobile' });
      await recoveryManager.markGCSAsCompleted(result2.backupId, result2.filePath, { success: true });

      const pendingFiles = await recoveryManager.getGCSPendingFiles();

      assert.strictEqual(pendingFiles.length, 1);
      assert.strictEqual(pendingFiles[0].gcsFileName, 'pending.json');
    });
  });

  describe('processGCSPendingFiles', () => {
    it('should process pending files successfully', async () => {
      // Crear archivos pendientes
      await recoveryManager.createGCSBackup('existing-file.json', { 
        dataType: 'gps', 
        recordCount: 100,
        processingId: 'test_123'
      });

      const result = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.processed, 1);
      assert.strictEqual(result.failed, 0);
      assert.strictEqual(result.total, 1);
      assert.strictEqual(result.results.length, 1);
      assert.strictEqual(result.results[0].success, true);
      assert.strictEqual(result.results[0].method, 'gcs_to_bigquery');
    });

    it('should handle recovery from original data when GCS file missing', async () => {
      const originalData = [
        { deviceId: 'device1', lat: -12.0464, lng: -77.0428, timestamp: '2025-01-15T10:30:00Z' }
      ];

      // Crear archivo pendiente con datos originales (archivo no existe en GCS)
      await recoveryManager.createGCSBackup('missing-file.json', { 
        dataType: 'gps', 
        recordCount: 1,
        processingId: 'test_456'
      }, originalData);

      const result = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.processed, 1);
      assert.strictEqual(result.results[0].method, 'original_data_recovery');
    });

    it('should handle BigQuery processing failures', async () => {
      // Mock BigQuery failure
      mockBigQueryProcessor.processGCSFile = async () => ({
        success: false,
        error: 'BigQuery processing failed'
      });

      await recoveryManager.createGCSBackup('existing-file.json', { 
        dataType: 'gps', 
        recordCount: 100 
      });

      const result = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.processed, 0);
      assert.strictEqual(result.failed, 1);
      assert.strictEqual(result.results[0].success, false);
    });

    it('should return success when no pending files', async () => {
      const result = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.processed, 0);
      assert.strictEqual(result.message, 'No pending GCS files');
    });
  });

  describe('recoverFromOriginalData', () => {
    it('should recover from original data successfully', async () => {
      const pendingFile = {
        id: 'test_recovery_123',
        gcsFileName: 'recovery-test.json',
        metadata: {
          dataType: 'gps',
          recordCount: 1,
          processingId: 'recovery_test'
        },
        originalData: [
          { deviceId: 'device1', lat: -12.0464, lng: -77.0428, timestamp: '2025-01-15T10:30:00Z' }
        ]
      };

      const result = await recoveryManager.recoverFromOriginalData(pendingFile);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.method, 'original_data_recovery');
      assert.ok(result.jobId);
      assert.ok(result.recordsProcessed > 0);
    });

    it('should fail when no original data available', async () => {
      const pendingFile = {
        id: 'test_recovery_456',
        gcsFileName: 'no-data.json',
        metadata: { dataType: 'gps' },
        originalData: null
      };

      const result = await recoveryManager.recoverFromOriginalData(pendingFile);

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('No hay datos originales'));
    });

    it('should handle GCS upload failures during recovery', async () => {
      // Mock GCS upload failure
      mockGCSAdapter.uploadJSON = async () => ({
        success: false,
        error: 'GCS upload failed'
      });

      const pendingFile = {
        id: 'test_recovery_789',
        gcsFileName: 'upload-fail.json',
        metadata: { dataType: 'gps' },
        originalData: [{ test: 'data' }]
      };

      const result = await recoveryManager.recoverFromOriginalData(pendingFile);

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Error re-subiendo a GCS'));
    });
  });

  describe('status management', () => {
    it('should mark GCS file as processing', async () => {
      const backupResult = await recoveryManager.createGCSBackup('test.json', { dataType: 'gps' });
      
      const result = await recoveryManager.markGCSAsProcessing(backupResult.backupId, backupResult.filePath);

      assert.strictEqual(result.status, 'processing');
      assert.strictEqual(result.retryCount, 1);
      assert.ok(result.lastRetryAt);
    });

    it('should mark GCS file as completed', async () => {
      const backupResult = await recoveryManager.createGCSBackup('test.json', { dataType: 'gps' });
      const processingResult = { jobId: 'test_job_123', recordsProcessed: 100 };
      
      const result = await recoveryManager.markGCSAsCompleted(backupResult.backupId, backupResult.filePath, processingResult);

      assert.strictEqual(result.status, 'completed');
      assert.ok(result.processedAt);
      assert.deepStrictEqual(result.result, processingResult);
      assert.strictEqual(result.error, null);
    });

    it('should mark GCS file as failed', async () => {
      const backupResult = await recoveryManager.createGCSBackup('test.json', { dataType: 'gps' });
      const error = new Error('Test error');
      
      const result = await recoveryManager.markGCSAsFailed(backupResult.backupId, backupResult.filePath, error);

      assert.strictEqual(result.status, 'pending'); // Primer intento, aún puede reintentar
      assert.ok(result.error);
      assert.strictEqual(result.error.message, 'Test error');
    });

    it('should mark as permanently failed after max retries', async () => {
      const backupResult = await recoveryManager.createGCSBackup('test.json', { dataType: 'gps' });
      
      // Simular múltiples fallos
      for (let i = 0; i < 3; i++) {
        await recoveryManager.markGCSAsProcessing(backupResult.backupId, backupResult.filePath);
        await recoveryManager.markGCSAsFailed(backupResult.backupId, backupResult.filePath, new Error(`Attempt ${i + 1} failed`));
      }

      const backupData = JSON.parse(await fs.readFile(backupResult.filePath, 'utf8'));
      assert.strictEqual(backupData.status, 'failed');
      assert.strictEqual(backupData.retryCount, 3);
    });
  });

  describe('cleanup', () => {
    it('should cleanup processed GCS files', async () => {
      // Crear archivo completado hace más de 24 horas
      const backupResult = await recoveryManager.createGCSBackup('old-completed.json', { dataType: 'gps' });
      await recoveryManager.markGCSAsCompleted(backupResult.backupId, backupResult.filePath, { success: true });
      
      // Modificar timestamp para simular archivo antiguo
      const backupData = JSON.parse(await fs.readFile(backupResult.filePath, 'utf8'));
      backupData.processedAt = new Date(Date.now() - 25 * 60 * 60 * 1000).toISOString(); // 25 horas atrás
      await fs.writeFile(backupResult.filePath, JSON.stringify(backupData));

      const result = await recoveryManager.cleanupProcessedGCSFiles(24 * 60 * 60 * 1000);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.cleaned, 1);

      // Verificar que el archivo fue eliminado
      try {
        await fs.access(backupResult.filePath);
        assert.fail('El archivo debería haber sido eliminado');
      } catch (error) {
        assert.strictEqual(error.code, 'ENOENT');
      }
    });

    it('should not cleanup recent completed files', async () => {
      const backupResult = await recoveryManager.createGCSBackup('recent-completed.json', { dataType: 'gps' });
      await recoveryManager.markGCSAsCompleted(backupResult.backupId, backupResult.filePath, { success: true });

      const result = await recoveryManager.cleanupProcessedGCSFiles(24 * 60 * 60 * 1000);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.cleaned, 0);

      // Verificar que el archivo aún existe
      const stats = await fs.stat(backupResult.filePath);
      assert.ok(stats.isFile());
    });
  });

  describe('getGCSRecoveryStats', () => {
    it('should return comprehensive recovery statistics', async () => {
      // Crear archivos en diferentes estados
      const gpsResult = await recoveryManager.createGCSBackup('gps-test.json', { dataType: 'gps', recordCount: 100 });
      const mobileResult = await recoveryManager.createGCSBackup('mobile-test.json', { dataType: 'mobile', recordCount: 50 });
      
      await recoveryManager.markGCSAsCompleted(gpsResult.backupId, gpsResult.filePath, { success: true });
      await recoveryManager.markGCSAsFailed(mobileResult.backupId, mobileResult.filePath, new Error('Test error'));

      const stats = await recoveryManager.getGCSRecoveryStats();

      assert.strictEqual(stats.total, 2);
      assert.strictEqual(stats.pending, 1); // mobile failed but can retry
      assert.strictEqual(stats.completed, 1);
      assert.strictEqual(stats.failed, 0);
      assert.strictEqual(stats.totalRecords, 150);
      
      assert.strictEqual(stats.byDataType.gps.total, 1);
      assert.strictEqual(stats.byDataType.gps.completed, 1);
      assert.strictEqual(stats.byDataType.mobile.total, 1);
      assert.strictEqual(stats.byDataType.mobile.pending, 1);
    });

    it('should return empty stats when no recovery files', async () => {
      const stats = await recoveryManager.getGCSRecoveryStats();

      assert.strictEqual(stats.total, 0);
      assert.strictEqual(stats.pending, 0);
      assert.strictEqual(stats.completed, 0);
      assert.strictEqual(stats.failed, 0);
    });
  });

  describe('getStatus', () => {
    it('should return comprehensive status', async () => {
      const status = await recoveryManager.getStatus();

      assert.strictEqual(status.initialized, true);
      assert.ok(status.backupPath);
      assert.ok(status.gcsRecoveryPath);
      assert.strictEqual(status.maxRetryAttempts, 3);
      assert.strictEqual(status.cleanupProcessedFiles, true);
      assert.ok(status.backupStats);
      assert.ok(status.gcsRecoveryStats);
      assert.ok(status.adapters.gcs);
      assert.ok(status.adapters.bigQuery);
    });
  });

  describe('integration scenarios', () => {
    it('should handle complete recovery workflow', async () => {
      const originalData = [
        { deviceId: 'device1', lat: -12.0464, lng: -77.0428, timestamp: '2025-01-15T10:30:00Z' },
        { deviceId: 'device2', lat: -12.0500, lng: -77.0500, timestamp: '2025-01-15T10:31:00Z' }
      ];

      // Crear backup GCS
      const backupResult = await recoveryManager.createGCSBackup('integration-test.json', {
        dataType: 'gps',
        recordCount: 2,
        processingId: 'integration_test_123'
      }, originalData);

      assert.strictEqual(backupResult.success, true);

      // Procesar archivos pendientes
      const processResult = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(processResult.success, true);
      assert.strictEqual(processResult.processed, 1);
      assert.strictEqual(processResult.results[0].success, true);

      // Verificar estadísticas
      const stats = await recoveryManager.getGCSRecoveryStats();
      assert.strictEqual(stats.completed, 1);
      assert.strictEqual(stats.pending, 0);
    });

    it('should handle multiple data types in recovery', async () => {
      // Mock para que los archivos existan en GCS
      mockGCSAdapter.listFiles = async (options = {}) => {
        if (options.prefix === 'gps-multi.json') {
          return [{ name: 'gps-multi.json', size: 1000 }];
        }
        if (options.prefix === 'mobile-multi.json') {
          return [{ name: 'mobile-multi.json', size: 500 }];
        }
        return [];
      };

      // Crear backups para GPS y Mobile
      await recoveryManager.createGCSBackup('gps-multi.json', { dataType: 'gps', recordCount: 100 });
      await recoveryManager.createGCSBackup('mobile-multi.json', { dataType: 'mobile', recordCount: 50 });

      const processResult = await recoveryManager.processGCSPendingFiles();

      assert.strictEqual(processResult.success, true);
      assert.strictEqual(processResult.processed, 2);

      const stats = await recoveryManager.getGCSRecoveryStats();
      assert.strictEqual(stats.byDataType.gps.completed, 1);
      assert.strictEqual(stats.byDataType.mobile.completed, 1);
    });
  });
});