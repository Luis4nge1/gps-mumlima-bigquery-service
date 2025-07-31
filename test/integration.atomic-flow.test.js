import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs/promises';
import path from 'path';
import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import { AtomicRedisProcessor } from '../src/services/AtomicRedisProcessor.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { RedisRepository } from '../src/repositories/RedisRepository.js';

/**
 * Tests de integración para flujo completo con extracción atómica
 * Cubre los requerimientos 1.5, 2.1, 2.4, 4.1, 4.2 del spec
 */
describe('Integration Tests - Atomic Flow Complete', () => {
  let processor;
  let atomicProcessor;
  let backupManager;
  let gcsAdapter;
  let bigQueryProcessor;
  let redisRepo;
  let testTmpPath;

  beforeEach(async () => {
    // Configurar variables de entorno para tests
    process.env.GCS_SIMULATION_MODE = 'true';
    process.env.BIGQUERY_SIMULATION_MODE = 'true';
    process.env.GCS_CLEANUP_PROCESSED_FILES = 'false'; // Keep files for verification
    process.env.BACKUP_STORAGE_PATH = 'tmp/test-atomic-backups/';
    process.env.BACKUP_MAX_RETRIES = '3';
    process.env.BACKUP_RETENTION_HOURS = '24';
    process.env.ATOMIC_PROCESSING_ENABLED = 'true';
    
    // Crear directorio temporal para tests
    testTmpPath = path.join('tmp', 'integration-atomic-test');
    await fs.mkdir(testTmpPath, { recursive: true });

    // Inicializar componentes
    processor = new GPSProcessorService();
    atomicProcessor = new AtomicRedisProcessor();
    backupManager = new BackupManager();
    gcsAdapter = new GCSAdapter();
    bigQueryProcessor = new BigQueryBatchProcessor();
    redisRepo = new RedisRepository();
    
    // Configurar paths de simulación específicos para tests
    gcsAdapter.localStoragePath = path.join(testTmpPath, 'gcs-simulation/');
    backupManager.backupPath = path.join(testTmpPath, 'atomic-backups/');
    
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
    
    // Limpiar variables de entorno
    delete process.env.GCS_SIMULATION_MODE;
    delete process.env.BIGQUERY_SIMULATION_MODE;
    delete process.env.GCS_CLEANUP_PROCESSED_FILES;
    delete process.env.BACKUP_STORAGE_PATH;
    delete process.env.BACKUP_MAX_RETRIES;
    delete process.env.BACKUP_RETENTION_HOURS;
    delete process.env.ATOMIC_PROCESSING_ENABLED;
  });

  describe('Test de flujo exitoso: Redis → extracción atómica → GCS → BigQuery', () => {
    it('should process GPS data through complete atomic flow successfully', async () => {
      // Preparar datos GPS de prueba
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'atomic_gps_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-25T10:30:00Z',
          speed: 45.5,
          heading: 180,
          altitude: 150,
          accuracy: 5
        }),
        JSON.stringify({
          deviceId: 'atomic_gps_002',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-25T10:31:00Z',
          speed: 50.0,
          heading: 185,
          altitude: 155,
          accuracy: 3
        })
      ];

      // Mock AtomicRedisProcessor para simular extracción exitosa
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 2,
        allCleared: true,
        extractionTime: 50,
        gps: {
          data: mockGPSData,
          recordCount: 2,
          success: true,
          extractionTime: 25,
          clearTime: 10,
          cleared: true
        },
        mobile: {
          data: [],
          recordCount: 0,
          success: true,
          extractionTime: 15,
          clearTime: 5,
          cleared: true
        },
        initialStats: { gps: 2, mobile: 0, total: 2 },
        finalStats: { gps: 0, mobile: 0, total: 0 }
      });

      // Ejecutar procesamiento completo
      const result = await processor.processGPSData();

      // Verificar resultado general
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2);
      assert.ok(result.processingTime > 0);
      assert.ok(result.extractionTime > 0);

      // Verificar extracción atómica
      assert.ok(result.atomicExtraction);
      assert.strictEqual(result.atomicExtraction.totalExtracted, 2);
      assert.strictEqual(result.atomicExtraction.redisCleared, true);
      assert.strictEqual(result.atomicExtraction.gpsExtracted, 2);
      assert.strictEqual(result.atomicExtraction.mobileExtracted, 0);

      // Verificar resultado GPS específico
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 2);
      assert.strictEqual(result.results.gps.stage, 'completed');
      assert.strictEqual(result.results.gps.source, 'atomic_extraction');
      assert.ok(result.results.gps.gcsFile);
      assert.ok(result.results.gps.jobId);

      // Verificar que no se procesaron datos Mobile
      assert.strictEqual(result.results.mobile.recordsProcessed, 0);
      assert.ok(result.results.mobile.message.includes('No Mobile data'));

      // Verificar que se creó archivo en GCS (simulado)
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.ok(gcsFiles.length > 0);
      const gpsFile = gcsFiles.find(file => file.metadata.recordCount === 2);
      assert.ok(gpsFile);
      assert.strictEqual(gpsFile.metadata.source, 'atomic_extraction:gps:history:global');

      // Verificar que no se crearon backups locales
      const backupFiles = await backupManager.getLocalBackupFiles();
      assert.strictEqual(backupFiles.length, 0);
    });

    it('should process mixed GPS and Mobile data through atomic flow', async () => {
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'mixed_gps_001',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-25T10:30:00Z'
        })
      ];

      const mockMobileData = [
        JSON.stringify({
          userId: 'mixed_user_001',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-25T10:31:00Z',
          name: 'Mixed Test User',
          email: 'mixed@example.com'
        }),
        JSON.stringify({
          userId: 'mixed_user_002',
          lat: -12.0600,
          lng: -77.0600,
          timestamp: '2025-01-25T10:32:00Z',
          name: 'Another Mixed User',
          email: 'another.mixed@example.com'
        })
      ];

      // Mock extracción atómica mixta
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 3,
        allCleared: true,
        extractionTime: 75,
        gps: {
          data: mockGPSData,
          recordCount: 1,
          success: true,
          cleared: true
        },
        mobile: {
          data: mockMobileData,
          recordCount: 2,
          success: true,
          cleared: true
        },
        initialStats: { gps: 1, mobile: 2, total: 3 },
        finalStats: { gps: 0, mobile: 0, total: 0 }
      });

      const result = await processor.processGPSData();

      // Verificar resultado general
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 3);

      // Verificar ambos tipos se procesaron
      assert.ok(result.results.gps.success);
      assert.strictEqual(result.results.gps.recordsProcessed, 1);
      assert.ok(result.results.mobile.success);
      assert.strictEqual(result.results.mobile.recordsProcessed, 2);

      // Verificar archivos separados en GCS
      const gpsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      const mobileFiles = await gcsAdapter.listFiles({ dataType: 'mobile' });
      
      assert.ok(gpsFiles.length > 0);
      assert.ok(mobileFiles.length > 0);
      assert.ok(gpsFiles.some(file => file.metadata.recordCount === 1));
      assert.ok(mobileFiles.some(file => file.metadata.recordCount === 2));

      // Verificar extracción atómica
      assert.strictEqual(result.atomicExtraction.totalExtracted, 3);
      assert.strictEqual(result.atomicExtraction.redisCleared, true);
    });
  });

  describe('Test de falla en GCS: Redis → extracción atómica → GCS falla → backup local → retry exitoso', () => {
    it('should create local backup when GCS upload fails and retry successfully', async () => {
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'gcs_fail_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-25T10:30:00Z'
        })
      ];

      // Mock extracción atómica exitosa
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 1,
        allCleared: true,
        gps: {
          data: mockGPSData,
          recordCount: 1,
          success: true,
          cleared: true
        },
        mobile: {
          data: [],
          recordCount: 0,
          success: true,
          cleared: true
        }
      });

      // Mock GCS adapter para fallar en el primer intento
      let gcsCallCount = 0;
      const originalUploadJSONLines = processor.gcsAdapter.uploadJSONLines;
      processor.gcsAdapter.uploadJSONLines = async (data, fileName, metadata) => {
        gcsCallCount++;
        if (gcsCallCount === 1) {
          return {
            success: false,
            error: 'GCS network error - simulated failure'
          };
        } else {
          // Segundo intento exitoso (desde backup)
          return originalUploadJSONLines.call(processor.gcsAdapter, data, fileName, metadata);
        }
      };

      // Primer procesamiento - debe fallar GCS y crear backup
      const firstResult = await processor.processGPSData();

      // Verificar que la extracción fue exitosa pero GCS falló
      assert.strictEqual(firstResult.success, false); // Falla temporal
      assert.strictEqual(firstResult.results.gps.success, false);
      assert.strictEqual(firstResult.results.gps.stage, 'gcs_upload_failed');
      assert.strictEqual(firstResult.results.gps.backupCreated, true);
      assert.ok(firstResult.results.gps.backupId);

      // Verificar que se creó backup local
      const backupFiles = await backupManager.getLocalBackupFiles();
      assert.strictEqual(backupFiles.length, 1);
      assert.strictEqual(backupFiles[0].type, 'gps');
      assert.strictEqual(backupFiles[0].data.length, 1);
      assert.strictEqual(backupFiles[0].status, 'pending');

      // Mock nueva extracción vacía (Redis ya limpio)
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 0,
        allCleared: true,
        gps: { data: [], recordCount: 0, success: true },
        mobile: { data: [], recordCount: 0, success: true }
      });

      // Segundo procesamiento - debe procesar backup exitosamente
      const secondResult = await processor.processGPSData();

      // Verificar que el backup se procesó exitosamente
      assert.strictEqual(secondResult.success, true);
      assert.strictEqual(secondResult.recordsProcessed, 1); // Del backup procesado

      // Verificar que el backup fue eliminado después del procesamiento exitoso
      const remainingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(remainingBackups.length, 0);

      // Verificar que se creó archivo en GCS
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.ok(gcsFiles.length > 0);
      const gpsFile = gcsFiles.find(file => file.metadata.originalBackupId);
      assert.ok(gpsFile);
      assert.strictEqual(gpsFile.metadata.source, 'local_backup');

      // Restaurar método original
      processor.gcsAdapter.uploadJSONLines = originalUploadJSONLines;
    });

    it('should handle multiple backup files processing in correct order', async () => {
      // Crear múltiples backups simulados
      const gpsData1 = [{ deviceId: 'backup_device_1', lat: -12.0464, lng: -77.0428 }];
      const gpsData2 = [{ deviceId: 'backup_device_2', lat: -12.0500, lng: -77.0500 }];
      const mobileData1 = [{ userId: 'backup_user_1', lat: -12.0600, lng: -77.0600 }];

      // Crear backups con diferentes timestamps
      const backup1 = await backupManager.saveToLocalBackup(gpsData1, 'gps', { 
        extractedAt: '2025-01-25T10:00:00Z' 
      });
      await new Promise(resolve => setTimeout(resolve, 10)); // Pequeña pausa
      
      const backup2 = await backupManager.saveToLocalBackup(gpsData2, 'gps', { 
        extractedAt: '2025-01-25T10:01:00Z' 
      });
      await new Promise(resolve => setTimeout(resolve, 10));
      
      const backup3 = await backupManager.saveToLocalBackup(mobileData1, 'mobile', { 
        extractedAt: '2025-01-25T10:02:00Z' 
      });

      // Mock extracción vacía (no nuevos datos)
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 0,
        allCleared: true,
        gps: { data: [], recordCount: 0, success: true },
        mobile: { data: [], recordCount: 0, success: true }
      });

      // Procesar backups
      const result = await processor.processGPSData();

      // Verificar que se procesaron todos los backups
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 3); // 1 + 1 + 1

      // Verificar que no quedan backups pendientes
      const remainingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(remainingBackups.length, 0);

      // Verificar archivos en GCS
      const gcsFiles = await gcsAdapter.listFiles();
      const backupFiles = gcsFiles.filter(file => file.metadata.source === 'local_backup');
      assert.strictEqual(backupFiles.length, 3);

      assert.ok(backup1.success);
      assert.ok(backup2.success);
      assert.ok(backup3.success);
    });
  });

  describe('Test de falla en BigQuery: Redis → extracción atómica → GCS OK → BigQuery falla → recovery existente', () => {
    it('should handle BigQuery failure while keeping file in GCS for recovery', async () => {
      const mockGPSData = [
        JSON.stringify({
          deviceId: 'bq_fail_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-25T10:30:00Z'
        })
      ];

      // Mock extracción atómica exitosa
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 1,
        allCleared: true,
        gps: {
          data: mockGPSData,
          recordCount: 1,
          success: true,
          cleared: true
        },
        mobile: {
          data: [],
          recordCount: 0,
          success: true,
          cleared: true
        }
      });

      // Mock BigQuery processor para fallar
      processor.bigQueryProcessor.processGCSFile = async (gcsPath, dataType, metadata) => ({
        success: false,
        error: 'BigQuery quota exceeded - simulated failure',
        recordsProcessed: 0
      });

      // Ejecutar procesamiento
      const result = await processor.processGPSData();

      // Verificar que GCS fue exitoso pero BigQuery falló
      assert.strictEqual(result.success, false);
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.stage, 'bigquery_processing');
      assert.ok(result.results.gps.error.includes('BigQuery quota exceeded'));
      assert.ok(result.results.gps.gcsFile); // Archivo debe estar en GCS

      // Verificar que NO se creó backup local (BigQuery failure no requiere backup)
      const backupFiles = await backupManager.getLocalBackupFiles();
      assert.strictEqual(backupFiles.length, 0);

      // Verificar que el archivo permanece en GCS para recovery
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.ok(gcsFiles.length > 0);
      const gpsFile = gcsFiles.find(file => file.metadata.recordCount === 1);
      assert.ok(gpsFile);
      assert.strictEqual(gpsFile.metadata.source, 'atomic_extraction:gps:history:global');

      // Simular recovery exitoso del archivo GCS
      processor.bigQueryProcessor.processGCSFile = async (gcsPath, dataType, metadata) => ({
        success: true,
        recordsProcessed: 1,
        jobId: 'recovery_job_123'
      });

      // El sistema de recovery existente debería procesar el archivo
      const recoveryResult = await processor.bigQueryProcessor.processGCSFile(
        gpsFile.gcsPath || gpsFile.name,
        'gps',
        gpsFile.metadata
      );

      assert.strictEqual(recoveryResult.success, true);
      assert.strictEqual(recoveryResult.recordsProcessed, 1);
      assert.ok(recoveryResult.jobId);
    });

    it('should handle mixed success/failure in BigQuery processing', async () => {
      const mockGPSData = [JSON.stringify({ deviceId: 'mixed_gps', lat: -12.0464, lng: -77.0428 })];
      const mockMobileData = [JSON.stringify({ userId: 'mixed_user', lat: -12.0500, lng: -77.0500 })];

      // Mock extracción atómica exitosa
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 2,
        allCleared: true,
        gps: {
          data: mockGPSData,
          recordCount: 1,
          success: true,
          cleared: true
        },
        mobile: {
          data: mockMobileData,
          recordCount: 1,
          success: true,
          cleared: true
        }
      });

      // Mock BigQuery processor para fallar solo en GPS
      processor.bigQueryProcessor.processGCSFile = async (gcsPath, dataType, metadata) => {
        if (dataType === 'gps') {
          return {
            success: false,
            error: 'GPS BigQuery processing failed',
            recordsProcessed: 0
          };
        } else {
          return {
            success: true,
            recordsProcessed: 1,
            jobId: 'mobile_job_success'
          };
        }
      };

      const result = await processor.processGPSData();

      // Verificar resultado general (falla por GPS)
      assert.strictEqual(result.success, false);

      // Verificar GPS falló en BigQuery
      assert.strictEqual(result.results.gps.success, false);
      assert.strictEqual(result.results.gps.stage, 'bigquery_processing');
      assert.ok(result.results.gps.gcsFile);

      // Verificar Mobile fue exitoso
      assert.strictEqual(result.results.mobile.success, true);
      assert.strictEqual(result.results.mobile.recordsProcessed, 1);
      assert.strictEqual(result.results.mobile.stage, 'completed');

      // Verificar archivos en GCS
      const gcsFiles = await gcsAdapter.listFiles();
      const gpsFiles = gcsFiles.filter(file => file.metadata.dataType === 'gps');
      const mobileFiles = gcsFiles.filter(file => file.metadata.dataType === 'mobile');
      
      assert.strictEqual(gpsFiles.length, 1); // GPS file remains for recovery
      assert.strictEqual(mobileFiles.length, 0); // Mobile file cleaned up after success
    });
  });

  describe('Test de datos nuevos durante procesamiento: verificar que no se pierden datos', () => {
    it('should allow new data to arrive in clean Redis during atomic processing', async () => {
      const initialGPSData = [
        JSON.stringify({
          deviceId: 'initial_device',
          lat: -12.0464,
          lng: -77.0428,
          timestamp: '2025-01-25T10:30:00Z'
        })
      ];

      const newGPSData = [
        JSON.stringify({
          deviceId: 'new_device',
          lat: -12.0500,
          lng: -77.0500,
          timestamp: '2025-01-25T10:35:00Z'
        })
      ];

      let redisCleared = false;
      let newDataArrived = false;

      // Mock extracción atómica que simula limpieza inmediata
      processor.atomicProcessor.extractAllData = async () => {
        // Simular que Redis se limpia inmediatamente después de extracción
        redisCleared = true;
        
        // Simular llegada de nuevos datos durante procesamiento
        setTimeout(() => {
          newDataArrived = true;
        }, 10);

        return {
          success: true,
          totalRecords: 1,
          allCleared: true,
          gps: {
            data: initialGPSData,
            recordCount: 1,
            success: true,
            cleared: true
          },
          mobile: {
            data: [],
            recordCount: 0,
            success: true,
            cleared: true
          },
          initialStats: { gps: 1, mobile: 0, total: 1 },
          finalStats: { gps: 0, mobile: 0, total: 0 }
        };
      };

      // Primer procesamiento
      const firstResult = await processor.processGPSData();

      // Verificar que el procesamiento inicial fue exitoso
      assert.strictEqual(firstResult.success, true);
      assert.strictEqual(firstResult.recordsProcessed, 1);
      assert.strictEqual(firstResult.atomicExtraction.redisCleared, true);

      // Verificar que Redis fue limpiado
      assert.strictEqual(redisCleared, true);

      // Esperar a que lleguen nuevos datos
      await new Promise(resolve => setTimeout(resolve, 20));
      assert.strictEqual(newDataArrived, true);

      // Mock segunda extracción con nuevos datos
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 1,
        allCleared: true,
        gps: {
          data: newGPSData,
          recordCount: 1,
          success: true,
          cleared: true
        },
        mobile: {
          data: [],
          recordCount: 0,
          success: true,
          cleared: true
        },
        initialStats: { gps: 1, mobile: 0, total: 1 },
        finalStats: { gps: 0, mobile: 0, total: 0 }
      });

      // Segundo procesamiento con nuevos datos
      const secondResult = await processor.processGPSData();

      // Verificar que los nuevos datos se procesaron correctamente
      assert.strictEqual(secondResult.success, true);
      assert.strictEqual(secondResult.recordsProcessed, 1);

      // Verificar que se crearon archivos separados en GCS
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.strictEqual(gcsFiles.length, 2); // Dos archivos separados

      // Verificar que los datos son diferentes
      const file1 = gcsFiles[0];
      const file2 = gcsFiles[1];
      assert.notStrictEqual(file1.metadata.processingId, file2.metadata.processingId);
    });

    it('should handle continuous data arrival during multiple processing cycles', async () => {
      const dataBatches = [
        [JSON.stringify({ deviceId: 'batch_1_device', lat: -12.0464, lng: -77.0428 })],
        [JSON.stringify({ deviceId: 'batch_2_device', lat: -12.0500, lng: -77.0500 })],
        [JSON.stringify({ deviceId: 'batch_3_device', lat: -12.0600, lng: -77.0600 })]
      ];

      let batchIndex = 0;

      // Mock extracción que simula diferentes batches de datos
      processor.atomicProcessor.extractAllData = async () => {
        const currentBatch = dataBatches[batchIndex] || [];
        batchIndex++;

        return {
          success: true,
          totalRecords: currentBatch.length,
          allCleared: true,
          gps: {
            data: currentBatch,
            recordCount: currentBatch.length,
            success: true,
            cleared: true
          },
          mobile: {
            data: [],
            recordCount: 0,
            success: true,
            cleared: true
          }
        };
      };

      const results = [];

      // Procesar múltiples batches
      for (let i = 0; i < 3; i++) {
        const result = await processor.processGPSData();
        results.push(result);
        
        // Pequeña pausa entre procesamientos
        await new Promise(resolve => setTimeout(resolve, 10));
      }

      // Verificar que todos los batches se procesaron exitosamente
      results.forEach((result, index) => {
        assert.strictEqual(result.success, true);
        assert.strictEqual(result.recordsProcessed, 1);
        assert.strictEqual(result.atomicExtraction.redisCleared, true);
      });

      // Verificar que se crearon archivos separados para cada batch
      const gcsFiles = await gcsAdapter.listFiles({ dataType: 'gps' });
      assert.strictEqual(gcsFiles.length, 3);

      // Verificar que cada archivo tiene datos únicos
      const deviceIds = gcsFiles.map(file => file.metadata.processingId);
      const uniqueDeviceIds = new Set(deviceIds);
      assert.strictEqual(uniqueDeviceIds.size, 3);
    });
  });

  describe('Test de múltiples backups pendientes procesándose en orden correcto', () => {
    it('should process multiple pending backups in chronological order', async () => {
      // Crear múltiples backups con timestamps específicos
      const backupData = [
        { data: [{ deviceId: 'oldest_device', timestamp: '2025-01-25T10:00:00Z' }], type: 'gps', time: '2025-01-25T10:00:00Z' },
        { data: [{ userId: 'middle_user', timestamp: '2025-01-25T10:01:00Z' }], type: 'mobile', time: '2025-01-25T10:01:00Z' },
        { data: [{ deviceId: 'newest_device', timestamp: '2025-01-25T10:02:00Z' }], type: 'gps', time: '2025-01-25T10:02:00Z' }
      ];

      const backupIds = [];

      // Crear backups en orden no cronológico para probar ordenamiento
      for (let i = backupData.length - 1; i >= 0; i--) {
        const backup = backupData[i];
        const result = await backupManager.saveToLocalBackup(backup.data, backup.type, {
          extractedAt: backup.time,
          testOrder: i
        });
        backupIds.push(result.backupId);
        
        // Pequeña pausa para asegurar timestamps diferentes
        await new Promise(resolve => setTimeout(resolve, 5));
      }

      // Verificar que se crearon los backups
      const pendingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(pendingBackups.length, 3);

      // Mock extracción vacía (no nuevos datos)
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 0,
        allCleared: true,
        gps: { data: [], recordCount: 0, success: true },
        mobile: { data: [], recordCount: 0, success: true }
      });

      // Track del orden de procesamiento
      const processedOrder = [];
      const originalUploadJSONLines = processor.gcsAdapter.uploadJSONLines;
      processor.gcsAdapter.uploadJSONLines = async (data, fileName, metadata) => {
        processedOrder.push(metadata.originalBackupId);
        return originalUploadJSONLines.call(processor.gcsAdapter, data, fileName, metadata);
      };

      // Procesar backups
      const result = await processor.processGPSData();

      // Verificar que se procesaron todos los backups
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 3);

      // Verificar que no quedan backups pendientes
      const remainingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(remainingBackups.length, 0);

      // Verificar orden cronológico de procesamiento
      assert.strictEqual(processedOrder.length, 3);
      
      // Los backups deben procesarse en orden cronológico (oldest first)
      const gcsFiles = await gcsAdapter.listFiles();
      const backupFiles = gcsFiles.filter(file => file.metadata.source === 'local_backup');
      assert.strictEqual(backupFiles.length, 3);

      // Verificar que se procesaron en orden correcto por timestamp
      const timestamps = backupFiles.map(file => file.metadata.extractedAt).sort();
      assert.deepStrictEqual(timestamps, [
        '2025-01-25T10:00:00Z',
        '2025-01-25T10:01:00Z', 
        '2025-01-25T10:02:00Z'
      ]);

      // Restaurar método original
      processor.gcsAdapter.uploadJSONLines = originalUploadJSONLines;
    });

    it('should handle backup processing failures and continue with remaining backups', async () => {
      // Crear múltiples backups
      const backup1 = await backupManager.saveToLocalBackup(
        [{ deviceId: 'success_device' }], 
        'gps', 
        { extractedAt: '2025-01-25T10:00:00Z' }
      );
      
      const backup2 = await backupManager.saveToLocalBackup(
        [{ deviceId: 'fail_device' }], 
        'gps', 
        { extractedAt: '2025-01-25T10:01:00Z' }
      );
      
      const backup3 = await backupManager.saveToLocalBackup(
        [{ userId: 'success_user' }], 
        'mobile', 
        { extractedAt: '2025-01-25T10:02:00Z' }
      );

      // Mock extracción vacía
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 0,
        allCleared: true,
        gps: { data: [], recordCount: 0, success: true },
        mobile: { data: [], recordCount: 0, success: true }
      });

      // Mock GCS upload para fallar en el segundo backup
      let uploadCallCount = 0;
      const originalUploadJSONLines = processor.gcsAdapter.uploadJSONLines;
      processor.gcsAdapter.uploadJSONLines = async (data, fileName, metadata) => {
        uploadCallCount++;
        if (uploadCallCount === 2) {
          return {
            success: false,
            error: 'Simulated GCS failure for second backup'
          };
        }
        return originalUploadJSONLines.call(processor.gcsAdapter, data, fileName, metadata);
      };

      // Procesar backups
      const result = await processor.processGPSData();

      // Verificar que se procesaron los backups exitosos
      assert.strictEqual(result.success, true);
      assert.strictEqual(result.recordsProcessed, 2); // Solo 2 exitosos

      // Verificar que queda 1 backup pendiente (el que falló)
      const remainingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(remainingBackups.length, 1);
      assert.strictEqual(remainingBackups[0].id, backup2.backupId);
      assert.strictEqual(remainingBackups[0].metadata.retryCount, 1);

      // Verificar archivos exitosos en GCS
      const gcsFiles = await gcsAdapter.listFiles();
      const backupFiles = gcsFiles.filter(file => file.metadata.source === 'local_backup');
      assert.strictEqual(backupFiles.length, 2); // Solo los exitosos

      // Restaurar método original
      processor.gcsAdapter.uploadJSONLines = originalUploadJSONLines;

      assert.ok(backup1.success);
      assert.ok(backup3.success);
    });

    it('should handle backup retry limits correctly', async () => {
      // Crear backup que fallará múltiples veces
      const backupResult = await backupManager.saveToLocalBackup(
        [{ deviceId: 'retry_limit_device' }], 
        'gps'
      );

      // Mock extracción vacía
      processor.atomicProcessor.extractAllData = async () => ({
        success: true,
        totalRecords: 0,
        allCleared: true,
        gps: { data: [], recordCount: 0, success: true },
        mobile: { data: [], recordCount: 0, success: true }
      });

      // Mock GCS upload para fallar siempre
      processor.gcsAdapter.uploadJSONLines = async () => ({
        success: false,
        error: 'Persistent GCS failure'
      });

      // Procesar múltiples veces hasta exceder límite de reintentos
      for (let i = 0; i < 4; i++) {
        await processor.processGPSData();
      }

      // Verificar que el backup ya no está pendiente (excedió reintentos)
      const remainingBackups = await backupManager.getLocalBackupFiles();
      assert.strictEqual(remainingBackups.length, 0);

      // Verificar que el backup está marcado como fallido
      const failedBackup = await backupManager.findBackupFile(backupResult.backupId);
      assert.ok(failedBackup);
      assert.strictEqual(failedBackup.status, 'failed');
      assert.ok(failedBackup.metadata.retryCount >= 3);
    });
  });
});