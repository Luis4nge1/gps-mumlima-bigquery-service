import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs/promises';
import path from 'path';
import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { GCSRecoveryManager } from '../src/services/GCSRecoveryManager.js';
import { DataSeparator } from '../src/services/DataSeparator.js';
import { RedisRepository } from '../src/repositories/RedisRepository.js';

/**
 * Tests de integración end-to-end para el flujo completo Redis → GCS → BigQuery
 * Cubre los requerimientos 1.1, 2.1, 3.1 del spec
 */
describe('End-to-End Integration Tests', () => {
  let processor;
  let gcsAdapter;
  let bigQueryProcessor;
  let recoveryManager;
  let dataSeparator;
  let redisRepo;
  let testTmpPath;

  beforeEach(async () => {
    // Configurar variables de entorno para tests
    process.env.GCS_SIMULATION_MODE = 'true';
    process.env.BIGQUERY_SIMULATION_MODE = 'true';
    process.env.GCS_CLEANUP_PROCESSED_FILES = 'true';
    process.env.GCS_MAX_RETRY_ATTEMPTS = '3';
    process.env.GCS_BUCKET_NAME = 'test-integration-bucket';
    process.env.BIGQUERY_PROJECT_ID = 'test-integration-project';
    process.env.BIGQUERY_DATASET_ID = 'integration_test_data';
    
    // Crear directorio temporal para tests
    testTmpPath = path.join('tmp', 'integration-test');
    await fs.mkdir(testTmpPath, { recursive: true });

    // Inicializar componentes
    processor = new GPSProcessorService();
    gcsAdapter = new GCSAdapter();
    bigQueryProcessor = new BigQueryBatchProcessor();
    dataSeparator = new DataSeparator();
    redisRepo = new RedisRepository();
    
    // Configurar paths de simulación específicos para tests
    gcsAdapter.localStoragePath = path.join(testTmpPath, 'gcs-simulation/');
    
    await processor.initialize();
    
    recoveryManager = new GCSRecoveryManager(gcsAdapter, bigQueryProcessor);
    await recoveryManager.initialize();
  });

  afterEach(async () => {
    await processor.cleanup();
    
    // Limpiar directorio de tests
    try {
      await fs.rm(testTmpPath, { recursive: true, force: true });
    } catch (error) {
      // Ignorar errores de limpieza
    }
    
    // Limpiar variables de entorno
    delete process.env.GCS_SIMULATION_MODE;
    delete process.env.BIGQUERY_SIMULATION_MODE;
    delete process.env.GCS_CLEANUP_PROCESSED_FILES;
    delete process.env.GCS_MAX_RETRY_ATTEMPTS;
    delete process.env.GCS_BUCKET_NAME;
    delete process.env.BIGQUERY_PROJECT_ID;
    delete process.env.BIGQUERY_DATASET_ID;
  });

  describe('Complete Redis → GCS → BigQuery Flow', () => {
    it('should process GPS data through complete end-to-end flow', async () => {
      // Preparar datos GPS de prueba
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_e2e_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z',
          speed: 45.5,
          heading: 180,
          altitude: 150,
          accuracy: 5
        }),
        JSON.stringify({
          deviceId: 'device_e2e_002',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-15T10:31:00Z',
          speed: 50.0,
          heading: 185,
          altitude: 155,
          accuracy: 3
        })
      ];

      // Mock Redis repository para simular datos
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 2 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      // Ejecutar procesamiento completo
      const result = await processor.processGPSData();

      // Verificar resultado general
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2);
      assert.ok(result.processingTime > 0);

      // Verificar resultado GPS específico
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 2);
      assert.strictEqual(result.results.gps.stage, 'completed');
      assert.ok(result.results.gps.gcsFileName);
      assert.ok(result.results.gps.bigQueryJobId);

      // Verificar que no se procesaron datos Mobile
      assert.strictEqual(result.results.mobile.recordsProcessed, 0);

      // Verificar que se creó archivo en GCS (simulado)
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.ok(gcsFiles.length > 0);
      assert.ok(gcsFiles.some(file => file.metadata.recordCount === 2));

      // Verificar estadísticas de separación
      assert.ok(result.separationStats);
      assert.strictEqual(result.separationStats.gpsRecords, 2);
      assert.strictEqual(result.separationStats.mobileRecords, 0);
    });

    it('should process Mobile data through complete end-to-end flow', async () => {
      // Preparar datos Mobile de prueba
      const mockMobileData = [
        JSON.stringify({
          userId: 'user_e2e_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z',
          name: 'Juan Pérez E2E',
          email: 'juan.e2e@example.com',
          appVersion: '1.2.3',
          deviceType: 'android'
        }),
        JSON.stringify({
          userId: 'user_e2e_002',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-15T10:31:00Z',
          name: 'María García E2E',
          email: 'maria.e2e@example.com',
          appVersion: '1.2.4',
          deviceType: 'ios'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 2 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      // Ejecutar procesamiento completo
      const result = await processor.processGPSData();

      // Verificar resultado general
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2);

      // Verificar resultado Mobile específico
      assert.ok(result.results.mobile.success);
      assert.strictEqual(result.results.mobile.recordsProcessed, 2);
      assert.strictEqual(result.results.mobile.stage, 'completed');
      assert.ok(result.results.mobile.gcsFileName);
      assert.ok(result.results.mobile.bigQueryJobId);

      // Verificar que no se procesaron datos GPS
      assert.strictEqual(result.results.gps.recordsProcessed, 0);

      // Verificar que se creó archivo en GCS (simulado)
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'mobile' });
      assert.ok(gcsFiles.length > 0);
      assert.ok(gcsFiles.some(file => file.metadata.recordCount === 2));
    });

    it('should process mixed GPS and Mobile data simultaneously', async () => {
      // Preparar datos mixtos
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'device_mixed_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      const mockMobileData = [
        JSON.stringify({
          userId: 'user_mixed_001',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-15T10:31:00Z',
          name: 'Mixed Test User',
          email: 'mixed@example.com'
        }),
        JSON.stringify({
          userId: 'user_mixed_002',
          lat: -12.0600,
          lng: -77.0600,
          timestamp: '2025-01-15T10:32:00Z',
          name: 'Another Mixed User',
          email: 'another.mixed@example.com'
        })
      ];

      // Mock Redis repository
      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 2 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return mockGPSData;
        if (key === 'mobile:history:global') return mockMobileData;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      // Ejecutar procesamiento completo
      const result = await processor.processGPSData();

      // Verificar resultado general
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 3);

      // Verificar ambos tipos de datos se procesaron
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 1);
      assert.ok(result.results.mobile.success);
      assert.strictEqual(result.results.mobile.recordsProcessed, 2);

      // Verificar que se crearon archivos separados en GCS
      const gpsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      const mobileFiles = await gcsAdapter.listFiles({ dataType: 'mobile' });
      
      assert.ok(gpsFiles.length > 0);
      assert.ok(mobileFiles.length > 0);
      assert.ok(gpsFiles.some(file => file.metadata.recordCount === 1));
      assert.ok(mobileFiles.some(file => file.metadata.recordCount === 2));

      // Verificar estadísticas de separación
      assert.strictEqual(result.separationStats.gpsRecords, 1);
      assert.strictEqual(result.separationStats.mobileRecords, 2);
      assert.strictEqual(result.separationStats.totalRecords, 3);
    });
  });

  describe('Recovery from GCS Files', () => {
    it('should recover and process pending GCS files', async () => {
      // Crear archivos GCS pendientes simulados
      const pendingGPSData = {
        metadata: {
          type: 'gps',
          processingId: 'recovery_gps_001',
          recordCount: 3,
          timestamp: '2025-01-15T09:00:00Z'
        },
        data: [
          { deviceId: 'recovery_device_001', lat: -12.0464, lng: -77.0428 },
          { deviceId: 'recovery_device_002', lat: -12.0500, lng: -77.0500 },
          { deviceId: 'recovery_device_003', lat: -12.0600, lng: -77.0600 }
        ]
      };

      const pendingMobileData = {
        metadata: {
          type: 'mobile',
          processingId: 'recovery_mobile_001',
          recordCount: 2,
          timestamp: '2025-01-15T09:01:00Z'
        },
        data: [
          { userId: 'recovery_user_001', lat: -12.0464, lng: -77.0428, name: 'Recovery User 1', email: 'recovery1@example.com' },
          { userId: 'recovery_user_002', lat: -12.0500, lng: -77.0500, name: 'Recovery User 2', email: 'recovery2@example.com' }
        ]
      };

      // Subir archivos pendientes a GCS
      const gpsFileName = await gcsAdapter.uploadJSON(pendingGPSData, 'recovery-gps-test.json');
      const mobileFileName = await gcsAdapter.uploadJSON(pendingMobileData, 'recovery-mobile-test.json');

      // Crear registros de recovery
      await recoveryManager.createGCSBackup(gpsFileName.fileName, pendingGPSData.metadata);
      await recoveryManager.createGCSBackup(mobileFileName.fileName, pendingMobileData.metadata);

      // Ejecutar recovery
      const recoveryResult = await recoveryManager.processGCSPendingFiles();

      // Verificar resultado del recovery
      assert.strictEqual(recoveryResult.success, true);
      assert.strictEqual(recoveryResult.processed, 2);
      assert.strictEqual(recoveryResult.failed, 0);
      assert.strictEqual(recoveryResult.total, 2);

      // Verificar que los archivos se procesaron correctamente
      assert.ok(recoveryResult.results.length === 2);
      assert.ok(recoveryResult.results.every(result => result.success === true));

      // Verificar que se registraron las métricas correctas
      const totalRecordsRecovered = recoveryResult.results.reduce((sum, result) => sum + result.recordsProcessed, 0);
      assert.strictEqual(totalRecordsRecovered, 5); // 3 GPS + 2 Mobile
    });

    it('should handle recovery failures gracefully', async () => {
      // Crear archivo GCS con datos inválidos
      const invalidData = {
        metadata: {
          type: 'invalid_type', // Tipo no soportado
          processingId: 'invalid_recovery_001',
          recordCount: 1
        },
        data: [{ invalid: 'data' }]
      };

      const fileName = await gcsAdapter.uploadJSON(invalidData, 'invalid-recovery-test.json');
      await recoveryManager.createGCSBackup(fileName.fileName, invalidData.metadata);

      // Mock BigQuery processor para fallar
      const originalProcessGCSFile = bigQueryProcessor.processGCSFile;
      bigQueryProcessor.processGCSFile = async () => ({
        success: false,
        error: 'Invalid data type for recovery test'
      });

      // Ejecutar recovery
      const recoveryResult = await recoveryManager.processGCSPendingFiles();

      // Verificar que se manejó el error correctamente
      assert.strictEqual(recoveryResult.success, true); // El recovery continúa aunque falle un archivo
      assert.strictEqual(recoveryResult.processed, 0);
      assert.strictEqual(recoveryResult.failed, 1);
      assert.strictEqual(recoveryResult.total, 1);

      // Restaurar método original
      bigQueryProcessor.processGCSFile = originalProcessGCSFile;
    });

    it('should retry failed recovery files up to max attempts', async () => {
      // Crear archivo para recovery con múltiples fallos
      const retryData = {
        metadata: {
          type: 'gps',
          processingId: 'retry_test_001',
          recordCount: 1
        },
        data: [{ deviceId: 'retry_device', lat: -12.0464, lng: -77.0428 }]
      };

      const fileName = await gcsAdapter.uploadJSON(retryData, 'retry-test.json');
      await recoveryManager.createGCSBackup(fileName.fileName, retryData.metadata);

      // Mock BigQuery processor para fallar las primeras 2 veces
      let attemptCount = 0;
      const originalProcessGCSFile = bigQueryProcessor.processGCSFile;
      bigQueryProcessor.processGCSFile = async () => {
        attemptCount++;
        if (attemptCount <= 2) {
          return { success: false, error: `Attempt ${attemptCount} failed` };
        }
        return { success: true, recordsProcessed: 1, jobId: 'retry_job_success' };
      };

      // Ejecutar recovery múltiples veces para simular reintentos
      let finalResult;
      for (let i = 0; i < 3; i++) {
        finalResult = await recoveryManager.processGCSPendingFiles();
        if (finalResult.processed > 0) break;
      }

      // Verificar que finalmente tuvo éxito
      assert.strictEqual(finalResult.success, true);
      assert.strictEqual(finalResult.processed, 1);
      assert.strictEqual(attemptCount, 3); // Falló 2 veces, éxito en la 3ra

      // Restaurar método original
      bigQueryProcessor.processGCSFile = originalProcessGCSFile;
    });
  });

  describe('Error Handling at Each Stage', () => {
    it('should handle data separation errors', async () => {
      // Mock datos inválidos que causarán error en separación
      const invalidData = ['invalid json data', 'another invalid entry'];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 2 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => invalidData;

      // Mock DataSeparator para fallar
      processor.dataSeparator.separateDataByType = async () => ({
        success: false,
        error: 'Data separation failed due to invalid JSON'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Data separation failed'));
      assert.strictEqual(result.recordsProcessed, 0);
    });

    it('should handle GCS upload failures', async () => {
      // Preparar datos válidos
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'gcs_fail_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      // Mock GCS adapter para fallar
      processor.gcsAdapter.uploadJSON = async () => ({
        success: false,
        error: 'GCS upload failed - network error'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.stage, 'gcs_upload');
      assert.ok(result.results.gps.error.includes('GCS upload failed'));
    });

    it('should handle BigQuery processing failures', async () => {
      // Preparar datos válidos
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'bq_fail_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      // Mock BigQuery processor para fallar
      processor.bigQueryProcessor.processGCSFile = async () => ({
        success: false,
        error: 'BigQuery job failed - quota exceeded'
      });

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.stage, 'bigquery_processing');
      assert.ok(result.results.gps.error.includes('BigQuery job failed'));

      // Verificar que se creó backup para recovery
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.ok(gcsFiles.length > 0); // El archivo debe estar en GCS para recovery
    });

    it('should handle concurrent processing attempts', async () => {
      // Simular procesamiento ya en curso
      processor.isProcessing = true;

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.strictEqual(result.error, 'Processing already in progress');
      assert.strictEqual(result.recordsProcessed, 0);
    });

    it('should handle Redis connection failures', async () => {
      // Mock Redis repository para fallar
      processor.redisRepo.getGPSStats = async () => {
        throw new Error('Redis connection failed');
      };

      const result = await processor.processGPSData();

      assert.strictEqual(result.success, false);
      assert.ok(result.error.includes('Redis connection failed'));
    });
  });

  describe('Performance Tests for Different Batch Sizes', () => {
    it('should handle small batch sizes efficiently', async () => {
      // Crear dataset pequeño (10 registros)
      const smallGPSDataset = Array.from({ length: 10 }, (_, i) => 
        JSON.stringify({
          deviceId: `small_device_${i}`,
          lat: -12.0464 + (i * 0.001),
          lng: -77.0428 + (i * 0.001),
          timestamp: new Date(Date.now() + i * 1000).toISOString()
        })
      );

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 10 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => smallGPSDataset;
      processor.redisRepo.clearListData = async () => {};

      const startTime = Date.now();
      const result = await processor.processGPSData();
      const processingTime = Date.now() - startTime;

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 10);
      assert.ok(processingTime < 5000); // Debe completarse en menos de 5 segundos
      assert.ok(result.processingTime > 0);
      
      // Verificar eficiencia de memoria (aproximada)
      const memoryUsage = process.memoryUsage();
      assert.ok(memoryUsage.heapUsed < 100 * 1024 * 1024); // Menos de 100MB
    });

    it('should handle medium batch sizes efficiently', async () => {
      // Crear dataset mediano (500 registros)
      const mediumGPSDataset = Array.from({ length: 500 }, (_, i) => 
        JSON.stringify({
          deviceId: `medium_device_${i}`,
          lat: -12.0464 + (i * 0.0001),
          lng: -77.0428 + (i * 0.0001),
          timestamp: new Date(Date.now() + i * 1000).toISOString(),
          speed: 30 + (i % 50),
          heading: i % 360
        })
      );

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 500 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mediumGPSDataset;
      processor.redisRepo.clearListData = async () => {};

      const startTime = Date.now();
      const result = await processor.processGPSData();
      const processingTime = Date.now() - startTime;

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 500);
      assert.ok(processingTime < 15000); // Debe completarse en menos de 15 segundos
      
      // Verificar que el archivo GCS se creó correctamente
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      const targetFile = gcsFiles.find(file => file.metadata.recordCount === 500);
      assert.ok(targetFile);
      assert.ok(targetFile.size > 1000); // Archivo debe tener tamaño considerable
    });

    it('should handle large batch sizes with memory management', async () => {
      // Crear dataset grande (2000 registros)
      const largeGPSDataset = Array.from({ length: 2000 }, (_, i) => 
        JSON.stringify({
          deviceId: `large_device_${i}`,
          lat: -12.0464 + (i * 0.00001),
          lng: -77.0428 + (i * 0.00001),
          timestamp: new Date(Date.now() + i * 1000).toISOString(),
          speed: 20 + (i % 80),
          heading: i % 360,
          altitude: 100 + (i % 200),
          accuracy: 1 + (i % 10)
        })
      );

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 2000 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => largeGPSDataset;
      processor.redisRepo.clearListData = async () => {};

      const initialMemory = process.memoryUsage();
      const startTime = Date.now();
      
      const result = await processor.processGPSData();
      
      const processingTime = Date.now() - startTime;
      const finalMemory = process.memoryUsage();
      const memoryIncrease = finalMemory.heapUsed - initialMemory.heapUsed;

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2000);
      assert.ok(processingTime < 30000); // Debe completarse en menos de 30 segundos
      
      // Verificar que el incremento de memoria sea razonable (menos de 50MB)
      assert.ok(memoryIncrease < 50 * 1024 * 1024);
      
      // Verificar estadísticas de procesamiento
      assert.ok(result.separationStats.totalRecords === 2000);
      assert.ok(result.results.gps.processingTime > 0);
    });

    it('should handle mixed large datasets efficiently', async () => {
      // Crear datasets grandes mixtos
      const largeGPSDataset = Array.from({ length: 1000 }, (_, i) => 
        JSON.stringify({
          deviceId: `mixed_gps_${i}`,
          lat: -12.0464 + (i * 0.00001),
          lng: -77.0428 + (i * 0.00001),
          timestamp: new Date(Date.now() + i * 1000).toISOString()
        })
      );

      const largeMobileDataset = Array.from({ length: 800 }, (_, i) => 
        JSON.stringify({
          userId: `mixed_user_${i}`,
          lat: -12.0500 + (i * 0.00001),
          lng: -77.0500 + (i * 0.00001),
          timestamp: new Date(Date.now() + i * 1000).toISOString(),
          name: `Mixed User ${i}`,
          email: `mixed.user.${i}@example.com`,
          appVersion: '1.2.3',
          deviceType: i % 2 === 0 ? 'android' : 'ios'
        })
      );

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1000 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 800 });
      processor.redisRepo.getListData = async (key) => {
        if (key === 'gps:history:global') return largeGPSDataset;
        if (key === 'mobile:history:global') return largeMobileDataset;
        return [];
      };
      processor.redisRepo.clearListData = async () => {};

      const startTime = Date.now();
      const result = await processor.processGPSData();
      const processingTime = Date.now() - startTime;

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 1800);
      assert.ok(processingTime < 45000); // Debe completarse en menos de 45 segundos

      // Verificar que ambos tipos se procesaron correctamente
      assert.strictEqual(result.results.gps.recordsProcessed, 1000);
      assert.strictEqual(result.results.mobile.recordsProcessed, 800);

      // Verificar que se crearon archivos separados
      const gpsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      const mobileFiles = await gcsAdapter.listFiles({ dataType: 'mobile' });
      
      assert.ok(gpsFiles.some(file => file.metadata.recordCount === 1000));
      assert.ok(mobileFiles.some(file => file.metadata.recordCount === 800));
    });
  });

  describe('Concurrency and Distributed Locks Tests', () => {
    it('should prevent concurrent processing of same data', async () => {
      // Preparar datos de prueba
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'concurrent_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-15T10:30:00Z'
        })
      ];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      // Simular procesamiento lento para probar concurrencia
      const originalProcessGCSFile = processor.bigQueryProcessor.processGCSFile;
      processor.bigQueryProcessor.processGCSFile = async (...args) => {
        await new Promise(resolve => setTimeout(resolve, 1000)); // 1 segundo de delay
        return originalProcessGCSFile.call(processor.bigQueryProcessor, ...args);
      };

      // Ejecutar dos procesos concurrentes
      const promise1 = processor.processGPSData();
      const promise2 = processor.processGPSData();

      const [result1, result2] = await Promise.all([promise1, promise2]);

      // Uno debe tener éxito, el otro debe fallar por concurrencia
      const successCount = [result1, result2].filter(r => r.success).length;
      const concurrencyFailCount = [result1, result2].filter(r => 
        !r.success && r.error === 'Processing already in progress'
      ).length;

      assert.strictEqual(successCount, 1);
      assert.strictEqual(concurrencyFailCount, 1);

      // Restaurar método original
      processor.bigQueryProcessor.processGCSFile = originalProcessGCSFile;
    });

    it('should handle multiple recovery processes concurrently', async () => {
      // Crear múltiples archivos de recovery
      const recoveryFiles = [];
      for (let i = 0; i < 5; i++) {
        const data = {
          metadata: {
            type: 'gps',
            processingId: `concurrent_recovery_${i}`,
            recordCount: 10 + i
          },
          data: Array.from({ length: 10 + i }, (_, j) => ({
            deviceId: `concurrent_device_${i}_${j}`,
            lat: -12.0464 + (j * 0.001),
            lng: -77.0428 + (j * 0.001)
          }))
        };

        const fileName = await gcsAdapter.uploadJSON(data, `concurrent-recovery-${i}.json`);
        await recoveryManager.createGCSBackup(fileName.fileName, data.metadata);
        recoveryFiles.push(fileName.fileName);
      }

      // Ejecutar múltiples procesos de recovery concurrentes
      const recoveryPromises = Array.from({ length: 3 }, () => 
        recoveryManager.processGCSPendingFiles()
      );

      const results = await Promise.all(recoveryPromises);

      // Verificar que al menos uno tuvo éxito y procesó archivos
      const totalProcessed = results.reduce((sum, result) => sum + result.processed, 0);
      assert.ok(totalProcessed >= 5); // Todos los archivos deben procesarse eventualmente

      // Verificar que no hubo duplicación de procesamiento
      const totalRecordsProcessed = results.reduce((sum, result) => 
        sum + result.results.reduce((subSum, res) => subSum + (res.recordsProcessed || 0), 0), 0
      );
      assert.ok(totalRecordsProcessed >= 65); // 10+11+12+13+14 = 60, más margen de error
    });

    it('should handle concurrent GCS uploads efficiently', async () => {
      // Crear múltiples datasets para upload concurrente
      const datasets = Array.from({ length: 5 }, (_, i) => ({
        type: i % 2 === 0 ? 'gps' : 'mobile',
        data: {
          metadata: {
            type: i % 2 === 0 ? 'gps' : 'mobile',
            processingId: `concurrent_upload_${i}`,
            recordCount: 20
          },
          data: Array.from({ length: 20 }, (_, j) => ({
            id: `concurrent_${i}_${j}`,
            lat: -12.0464 + (j * 0.001),
            lng: -77.0428 + (j * 0.001)
          }))
        }
      }));

      // Ejecutar uploads concurrentes
      const uploadPromises = datasets.map((dataset, i) => 
        gcsAdapter.uploadJSON(dataset.data, `concurrent-upload-${i}.json`)
      );

      const startTime = Date.now();
      const uploadResults = await Promise.all(uploadPromises);
      const totalTime = Date.now() - startTime;

      // Verificar que todos los uploads tuvieron éxito
      assert.ok(uploadResults.every(result => result.success));
      assert.strictEqual(uploadResults.length, 5);

      // Verificar que el procesamiento concurrente fue eficiente
      assert.ok(totalTime < 10000); // Menos de 10 segundos para 5 uploads

      // Verificar que todos los archivos se crearon
      const allFiles = await gcsAdapter.listFiles();
      const concurrentFiles = allFiles.filter(file => 
        file.name.includes('concurrent-upload')
      );
      assert.strictEqual(concurrentFiles.length, 5);
    });

    it('should handle concurrent BigQuery batch processing', async () => {
      // Crear múltiples archivos GCS para procesamiento concurrente
      const batchFiles = [];
      for (let i = 0; i < 4; i++) {
        const data = {
          metadata: {
            type: i % 2 === 0 ? 'gps' : 'mobile',
            processingId: `concurrent_batch_${i}`,
            recordCount: 15
          },
          data: Array.from({ length: 15 }, (_, j) => ({
            id: `batch_${i}_${j}`,
            lat: -12.0464 + (j * 0.001),
            lng: -77.0428 + (j * 0.001)
          }))
        };

        const fileName = await gcsAdapter.uploadJSON(data, `concurrent-batch-${i}.json`);
        batchFiles.push({
          gcsUri: `gs://test-integration-bucket/${fileName.fileName}`,
          dataType: data.metadata.type,
          metadata: data.metadata
        });
      }

      // Ejecutar procesamiento batch concurrente
      const startTime = Date.now();
      const batchResult = await bigQueryProcessor.processBatch(batchFiles, { 
        maxConcurrency: 2,
        continueOnError: true 
      });
      const totalTime = Date.now() - startTime;

      // Verificar resultado del batch
      assert.strictEqual(batchResult.success, true);
      assert.strictEqual(batchResult.totalFiles, 4);
      assert.strictEqual(batchResult.successfulFiles, 4);
      assert.strictEqual(batchResult.failedFiles, 0);

      // Verificar eficiencia del procesamiento concurrente
      assert.ok(totalTime < 15000); // Menos de 15 segundos
      assert.ok(batchResult.totalRecords === 60); // 4 archivos * 15 registros

      // Verificar que se generaron jobs únicos
      const jobIds = batchResult.results.map(result => result.jobId);
      const uniqueJobIds = new Set(jobIds);
      assert.strictEqual(uniqueJobIds.size, 4); // Todos los jobs deben ser únicos
    });

    it('should handle resource contention gracefully', async () => {
      // Simular alta carga con múltiples operaciones simultáneas
      const operations = [];

      // Agregar operaciones de procesamiento
      for (let i = 0; i < 3; i++) {
        const mockData = [JSON.stringify({
          deviceId: `contention_device_${i}`,
          lat: -12.0464,
          lng: -77.0428,
          timestamp: new Date().toISOString()
        })];

        const processorCopy = new GPSProcessorService();
        await processorCopy.initialize();
        
        processorCopy.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
        processorCopy.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
        processorCopy.redisRepo.getListData = async () => mockData;
        processorCopy.redisRepo.clearListData = async () => {};

        operations.push(processorCopy.processGPSData());
      }

      // Agregar operaciones de recovery
      for (let i = 0; i < 2; i++) {
        operations.push(recoveryManager.processGCSPendingFiles());
      }

      // Agregar operaciones de estadísticas
      operations.push(gcsAdapter.getBucketStats());
      operations.push(bigQueryProcessor.getTableStats());

      // Ejecutar todas las operaciones concurrentemente
      const startTime = Date.now();
      const results = await Promise.allSettled(operations);
      const totalTime = Date.now() - startTime;

      // Verificar que la mayoría de operaciones completaron exitosamente
      const successfulOps = results.filter(result => 
        result.status === 'fulfilled' && 
        (result.value.success !== false || result.value.success === undefined)
      ).length;

      assert.ok(successfulOps >= operations.length * 0.7); // Al menos 70% exitosas
      assert.ok(totalTime < 30000); // Completar en menos de 30 segundos

      // Verificar que no hubo deadlocks o bloqueos permanentes
      const rejectedOps = results.filter(result => result.status === 'rejected').length;
      assert.ok(rejectedOps < operations.length * 0.3); // Menos del 30% rechazadas
    });
  });

  describe('Integration Health Checks and Monitoring', () => {
    it('should provide comprehensive health status across all components', async () => {
      const health = await processor.healthCheck();

      assert.strictEqual(health.healthy, true);
      assert.ok(health.services);
      assert.strictEqual(health.services.redis, 'healthy');
      assert.strictEqual(health.services.gcs, 'healthy');
      assert.strictEqual(health.services.bigQuery, 'healthy');
      assert.strictEqual(health.services.recovery, 'healthy');
      assert.ok(health.timestamp);
      assert.ok(health.details);
    });

    it('should provide detailed processor statistics', async () => {
      // Procesar algunos datos primero
      const mockData = [JSON.stringify({
        deviceId: 'stats_device',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: new Date().toISOString()
      })];

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 1 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockData;
      processor.redisRepo.clearListData = async () => {};

      await processor.processGPSData();

      // Obtener estadísticas
      const stats = await processor.getProcessorStats();

      assert.ok(stats.redis);
      assert.ok(stats.gcs);
      assert.ok(stats.bigQuery);
      assert.ok(stats.recovery);
      assert.ok(stats.processor);
      assert.strictEqual(stats.processor.flowType, 'redis_gcs_bigquery');
      assert.ok(stats.processor.lastProcessing);
    });

    it('should track metrics throughout the integration flow', async () => {
      // Procesar datos con seguimiento de métricas
      const mockGPSData = Array.from({ length: 50 }, (_, i) => 
        JSON.stringify({
          deviceId: `metrics_device_${i}`,
          lat: -12.0464 + (i * 0.001),
          lng: -77.0428 + (i * 0.001),
          timestamp: new Date(Date.now() + i * 1000).toISOString()
        })
      );

      processor.redisRepo.getGPSStats = async () => ({ totalRecords: 50 });
      processor.redisRepo.getMobileStats = async () => ({ totalRecords: 0 });
      processor.redisRepo.getListData = async () => mockGPSData;
      processor.redisRepo.clearListData = async () => {};

      const result = await processor.processGPSData();

      // Verificar que se registraron métricas
      assert.strictEqual(result.success, true);
      assert.ok(result.processingTime > 0);
      assert.ok(result.separationStats);
      assert.ok(result.results.gps.processingTime > 0);

      // Verificar métricas de GCS
      const gcsStats = await gcsAdapter.getBucketStats();
      assert.ok(gcsStats.totalFiles > 0);
      assert.ok(gcsStats.totalSize > 0);

      // Verificar métricas de BigQuery
      const bqStats = await bigQueryProcessor.getTableStats();
      assert.ok(bqStats.gps);
      assert.ok(bqStats.mobile);
    });
  });
});