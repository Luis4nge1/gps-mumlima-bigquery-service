import { RedisRepository } from '../repositories/RedisRepository.js';
import { DataSeparator } from '../services/DataSeparator.js';
import { GCSAdapter } from '../adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../services/BigQueryBatchProcessor.js';
import { GCSRecoveryManager } from '../services/GCSRecoveryManager.js';
import { AtomicRedisProcessor } from '../services/AtomicRedisProcessor.js';
import { BackupManager } from '../utils/BackupManager.js';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { GPSValidator } from '../validators/GPSValidator.js';
import { metrics } from '../utils/metrics.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Servicio principal para procesamiento de datos GPS
 * Actualizado para usar el nuevo flujo: Redis → GCS → BigQuery
 */
export class GPSProcessorService {
  constructor() {
    this.redisRepo = new RedisRepository();
    this.dataSeparator = new DataSeparator();
    this.gcsAdapter = new GCSAdapter();
    this.bigQueryProcessor = new BigQueryBatchProcessor();
    this.recoveryManager = new GCSRecoveryManager(this.gcsAdapter, this.bigQueryProcessor);
    this.atomicProcessor = new AtomicRedisProcessor();
    this.backupManager = new BackupManager();
    this.validator = new GPSValidator();
    this.metrics = metrics;
    this.isProcessing = false;
  }

  /**
   * Inicializa todos los adaptadores y servicios
   */
  async initialize() {
    try {
      logger.info('🔧 Inicializando GPS Processor Service...');

      await Promise.all([
        this.redisRepo.connect(), // Conectar Redis explícitamente
        this.gcsAdapter.initialize(),
        this.bigQueryProcessor.initialize(),
        this.recoveryManager.initialize(),
        this.atomicProcessor.initialize()
      ]);

      logger.info('✅ GPS Processor Service inicializado exitosamente');
    } catch (error) {
      logger.error('❌ Error inicializando GPS Processor Service:', error.message);
      throw error;
    }
  }

  /**
   * Procesa todos los datos GPS y Mobile disponibles usando extracción atómica
   * Flujo: Extracción atómica → Limpieza inmediata → Procesamiento → Backup local si falla GCS
   */
  async processGPSData() {
    if (this.isProcessing) {
      logger.warn('⚠️ Procesamiento ya en curso, saltando ejecución');
      return { success: false, error: 'Processing already in progress' };
    }

    this.isProcessing = true;
    const startTime = Date.now();

    try {
      logger.info('🔄 Iniciando procesamiento atómico de datos GPS y Mobile...');

      // Paso 1: Procesar backups locales pendientes primero
      await this.processLocalBackups();

      // Paso 2: Procesar archivos pendientes de recovery de GCS
      await this.processRecoveryFiles();

      // Paso 3: Extracción de datos (atómica o legacy según feature flag)
      let extractionResult;

      if (this.atomicProcessor.isAtomicProcessingEnabled()) {
        logger.info('🔄 Usando extracción atómica (feature flag habilitado)');
        extractionResult = await this.atomicProcessor.extractAllData();
      } else {
        logger.warn('⚠️ Usando extracción legacy (feature flag deshabilitado) - RIESGO DE PÉRDIDA DE DATOS');
        extractionResult = await this.legacyExtractAllData();
      }

      if (!extractionResult.success) {
        const mode = this.atomicProcessor.isAtomicProcessingEnabled() ? 'atómica' : 'legacy';
        logger.error(`❌ Error en extracción ${mode}:`, extractionResult.error);
        return {
          success: false,
          error: extractionResult.error,
          recordsProcessed: 0,
          extractionMode: mode
        };
      }

      if (extractionResult.totalRecords === 0) {
        logger.info('📍 No hay datos nuevos para procesar');
        return {
          success: true,
          recordsProcessed: 0,
          message: 'No new data to process'
        };
      }

      logger.info(`✅ Extracción atómica completada: ${extractionResult.totalRecords} registros extraídos y Redis limpiado`);

      // Paso 4: Procesar GPS y Mobile en paralelo para máximo rendimiento
      logger.info('🚀 Iniciando procesamiento paralelo GPS y Mobile...');

      const [gpsResult, mobileResult] = await Promise.all([
        // Procesar datos GPS extraídos
        extractionResult.gps.recordCount > 0
          ? this.processExtractedDataType('gps', extractionResult.gps.data)
          : Promise.resolve({ success: true, recordsProcessed: 0, message: 'No GPS data' }),

        // Procesar datos Mobile extraídos
        extractionResult.mobile.recordCount > 0
          ? this.processExtractedDataType('mobile', extractionResult.mobile.data)
          : Promise.resolve({ success: true, recordsProcessed: 0, message: 'No Mobile data' })
      ]);

      const results = {
        gps: gpsResult,
        mobile: mobileResult
      };

      logger.info(`✅ Procesamiento paralelo completado: GPS(${gpsResult.recordsProcessed}) + Mobile(${mobileResult.recordsProcessed})`)

      // Determinar éxito general (para métricas)
      const overallSuccess = results.gps.success && results.mobile.success;

      // En modo legacy, limpiar Redis DESPUÉS del procesamiento (con riesgo de pérdida)
      if (extractionResult.legacyMode) {
        await this.legacyClearRedisAfterProcessing();
      }

      // Recopilar métricas
      const processingTime = Date.now() - startTime;
      const totalRecordsProcessed = results.gps.recordsProcessed + results.mobile.recordsProcessed;
      const isAtomicMode = this.atomicProcessor.isAtomicProcessingEnabled();

      await this.metrics.recordProcessing({
        recordsProcessed: totalRecordsProcessed,
        processingTime,
        success: overallSuccess,
        gpsRecords: results.gps.recordsProcessed,
        mobileRecords: results.mobile.recordsProcessed,
        atomicExtraction: isAtomicMode,
        extractionMode: isAtomicMode ? 'atomic' : 'legacy',
        extractionTime: extractionResult.extractionTime,
        riskOfDataLoss: extractionResult.riskOfDataLoss || false
      });

      const mode = isAtomicMode ? 'atómico' : 'legacy';
      logger.info(`✅ Procesamiento ${mode} completado: ${totalRecordsProcessed} registros en ${processingTime}ms`);
      logger.info(`   📊 GPS: ${results.gps.recordsProcessed} registros`);
      logger.info(`   📊 Mobile: ${results.mobile.recordsProcessed} registros`);
      logger.info(`   ⚡ Extracción ${mode}: ${extractionResult.extractionTime}ms`);

      if (!isAtomicMode) {
        logger.warn(`   ⚠️ Modo legacy usado - posible pérdida de datos`);
      }

      return {
        success: overallSuccess,
        recordsProcessed: totalRecordsProcessed,
        processingTime,
        extractionTime: extractionResult.extractionTime,
        extractionMode: isAtomicMode ? 'atomic' : 'legacy',
        atomicProcessingEnabled: isAtomicMode,
        riskOfDataLoss: extractionResult.riskOfDataLoss || false,
        results: {
          gps: results.gps,
          mobile: results.mobile
        },
        extraction: {
          mode: isAtomicMode ? 'atomic' : 'legacy',
          totalExtracted: extractionResult.totalRecords,
          redisCleared: extractionResult.allCleared,
          gpsExtracted: extractionResult.gps.recordCount,
          mobileExtracted: extractionResult.mobile.recordCount,
          clearedImmediately: isAtomicMode
        }
      };

    } catch (error) {
      logger.error('❌ Error en procesamiento atómico GPS:', error.message);

      const isAtomicMode = this.atomicProcessor.isAtomicProcessingEnabled();

      await this.metrics.recordProcessing({
        recordsProcessed: 0,
        processingTime: Date.now() - startTime,
        success: false,
        error: error.message,
        atomicExtraction: isAtomicMode,
        extractionMode: isAtomicMode ? 'atomic' : 'legacy'
      });

      return {
        success: false,
        error: error.message,
        recordsProcessed: 0
      };

    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Extracción legacy (sin limpieza inmediata) - SOLO cuando atomic processing está deshabilitado
   * ⚠️ ADVERTENCIA: Este método tiene riesgo de pérdida de datos
   */
  async legacyExtractAllData() {
    const startTime = Date.now();

    try {
      logger.warn('⚠️ USANDO EXTRACCIÓN LEGACY - RIESGO DE PÉRDIDA DE DATOS');
      logger.warn('   Para usar extracción segura, habilitar ATOMIC_PROCESSING_ENABLED=true');

      // Obtener datos sin limpiar inmediatamente (método legacy)
      const gpsData = await this.redisRepo.getListData(config.gps.listKey);
      const mobileData = await this.redisRepo.getListData('mobile:history:global');

      const extractionTime = Date.now() - startTime;

      // En el método legacy, la limpieza se hace DESPUÉS del procesamiento
      // Esto crea una ventana donde se pueden perder datos nuevos

      logger.warn(`⚠️ Extracción legacy completada: ${gpsData.length + mobileData.length} registros en ${extractionTime}ms`);
      logger.warn('   Redis NO limpiado - datos nuevos pueden perderse durante procesamiento');

      return {
        success: true,
        gps: {
          data: gpsData,
          recordCount: gpsData.length,
          success: true,
          extractionTime: extractionTime,
          cleared: false // No se limpia en legacy
        },
        mobile: {
          data: mobileData,
          recordCount: mobileData.length,
          success: true,
          extractionTime: extractionTime,
          cleared: false // No se limpia en legacy
        },
        totalRecords: gpsData.length + mobileData.length,
        extractionTime: extractionTime,
        allCleared: false, // Redis NO se limpia en legacy
        legacyMode: true,
        riskOfDataLoss: true
      };

    } catch (error) {
      const extractionTime = Date.now() - startTime;
      logger.error(`❌ Error en extracción legacy (${extractionTime}ms):`, error.message);

      return {
        success: false,
        error: error.message,
        gps: { data: [], recordCount: 0, success: false },
        mobile: { data: [], recordCount: 0, success: false },
        totalRecords: 0,
        extractionTime: extractionTime,
        legacyMode: true
      };
    }
  }

  /**
   * Limpia Redis después del procesamiento (solo en modo legacy)
   * ⚠️ ADVERTENCIA: Datos nuevos que llegaron durante el procesamiento se PERDERÁN
   */
  async legacyClearRedisAfterProcessing() {
    try {
      logger.warn('⚠️ Limpiando Redis DESPUÉS del procesamiento (modo legacy)');
      logger.warn('   Datos nuevos que llegaron durante procesamiento se PERDERÁN');

      const [gpsCleared, mobileCleared] = await Promise.all([
        this.redisRepo.clearListData(config.gps.listKey),
        this.redisRepo.clearListData('mobile:history:global')
      ]);

      if (gpsCleared && mobileCleared) {
        logger.warn('⚠️ Redis limpiado - posible pérdida de datos nuevos');
      } else {
        logger.error('❌ Error limpiando Redis en modo legacy');
      }

      return gpsCleared && mobileCleared;

    } catch (error) {
      logger.error('❌ Error en limpieza legacy de Redis:', error.message);
      return false;
    }
  }

  /**
   * Procesa backups locales pendientes antes de procesar nuevos datos
   */
  async processLocalBackups() {
    try {
      logger.info('🔄 Procesando backups locales pendientes...');

      const pendingBackups = await this.backupManager.getLocalBackupFiles();

      if (pendingBackups.length === 0) {
        logger.debug('📋 No hay backups locales pendientes');
        return { success: true, processed: 0 };
      }

      logger.info(`📦 Encontrados ${pendingBackups.length} backups locales pendientes`);

      let processedCount = 0;
      let failedCount = 0;

      for (const backup of pendingBackups) {
        try {
          // Crear función de upload específica para el tipo de datos
          const uploadFunction = async (data, type) => {
            return await this.uploadDataToGCS(data, type, {
              source: 'local_backup',
              originalBackupId: backup.id
            });
          };

          const result = await this.backupManager.processLocalBackupFile(backup, uploadFunction);

          if (result.success) {
            processedCount++;
            logger.info(`✅ Backup local procesado: ${result.backupId} (${result.recordsProcessed} registros)`);

            // Eliminar backup procesado exitosamente
            await this.backupManager.deleteLocalBackup(result.backupId);
          } else {
            failedCount++;
            if (!result.willRetry) {
              logger.error(`❌ Backup local falló definitivamente: ${result.backupId}`);
            }
          }

        } catch (error) {
          failedCount++;
          logger.error(`❌ Error procesando backup local ${backup.id}:`, error.message);
        }
      }

      logger.info(`✅ Procesamiento de backups locales completado: ${processedCount} exitosos, ${failedCount} fallidos`);

      return {
        success: failedCount === 0,
        processed: processedCount,
        failed: failedCount,
        total: pendingBackups.length
      };

    } catch (error) {
      logger.error('❌ Error procesando backups locales:', error.message);
      return { success: false, processed: 0, error: error.message };
    }
  }

  /**
   * Procesa archivos pendientes de recovery antes de procesar nuevos datos
   */
  async processRecoveryFiles() {
    try {
      logger.info('🔄 Procesando archivos pendientes de recovery...');

      const recoveryResult = await this.recoveryManager.processGCSPendingFiles();

      if (recoveryResult.processed > 0) {
        logger.info(`✅ Recovery completado: ${recoveryResult.processed} archivos procesados`);
      } else {
        logger.debug('📋 No hay archivos pendientes de recovery');
      }

      return recoveryResult;
    } catch (error) {
      logger.error('❌ Error en recovery de archivos pendientes:', error.message);
      // No fallar el procesamiento principal por errores de recovery
      return { success: false, processed: 0, error: error.message };
    }
  }

  /**
   * Sube datos a GCS con manejo de errores y backup local
   * @param {Array} data - Datos a subir
   * @param {string} type - Tipo de datos ('gps' o 'mobile')
   * @param {Object} metadata - Metadata adicional
   * @returns {Object} Resultado de la subida
   */
  async uploadDataToGCS(data, type, metadata = {}) {
    try {
      // Validar datos
      const validationResult = await this.validateDataByType(type, data);
      if (!validationResult.isValid || validationResult.validData.length === 0) {
        logger.warn(`⚠️ No hay datos ${type} válidos para subir a GCS`);
        return {
          success: true,
          recordsProcessed: 0,
          message: `No valid ${type} data`,
          validationStats: validationResult.stats
        };
      }

      // Formatear datos para GCS
      const formattedData = this.dataSeparator.formatForGCS(validationResult.validData, type);
      if (!formattedData || !formattedData.data) {
        throw new Error('Invalid format result');
      }

      // Generar nombre de archivo y metadata
      const processingId = this.dataSeparator.generateProcessingId(type);
      const fileName = this.gcsAdapter.generateFileName(type, processingId);
      const gcsMetadata = {
        dataType: type,
        recordCount: formattedData.data.length,
        source: metadata.source || `redis:${type}:history:global`,
        processingId,
        originalSize: JSON.stringify(data).length,
        validationStats: validationResult.stats,
        ...metadata
      };

      // Convertir array a NEWLINE_DELIMITED_JSON para BigQuery
      const jsonLines = formattedData.data.map(record => JSON.stringify(record)).join('\n');

      // Subir a GCS
      const gcsResult = await this.gcsAdapter.uploadJSONLines(jsonLines, fileName, gcsMetadata);

      if (!gcsResult.success) {
        throw new Error(gcsResult.error);
      }

      logger.info(`✅ Datos ${type} subidos a GCS: ${gcsResult.fileName}`);

      return {
        success: true,
        fileName: gcsResult.fileName,
        gcsFile: gcsResult.fileName,
        recordsProcessed: formattedData.data.length,
        gcsPath: gcsResult.gcsPath || gcsResult.gcsUri,
        validationStats: validationResult.stats
      };

    } catch (error) {
      logger.error(`❌ Error subiendo ${type} a GCS:`, error.message);
      return {
        success: false,
        error: error.message,
        recordsProcessed: 0
      };
    }
  }

  /**
   * Procesa un tipo específico de datos extraídos atómicamente
   * Flujo: Validar → GCS → BigQuery → Backup local si falla GCS
   * @param {string} dataType - Tipo de datos ('gps' o 'mobile')
   * @param {Array} data - Datos extraídos de Redis
   * @returns {Object} Resultado del procesamiento
   */
  async processExtractedDataType(dataType, data) {
    try {
      logger.info(`🔄 Procesando ${data.length} registros ${dataType.toUpperCase()} extraídos atómicamente...`);

      // Paso 1: Intentar subir a GCS
      const gcsResult = await this.uploadDataToGCS(data, dataType, {
        source: `atomic_extraction:${dataType}:history:global`,
        extractedAt: new Date().toISOString()
      });

      if (gcsResult.success) {
        // Paso 2: Procesar hacia BigQuery
        const bigQueryResult = await this.bigQueryProcessor.processGCSFile(
          gcsResult.gcsPath,
          dataType,
          {
            dataType,
            recordCount: gcsResult.recordsProcessed,
            source: `atomic_extraction:${dataType}:history:global`,
            processingId: gcsResult.fileName.split('_').pop().split('.')[0],
            validationStats: gcsResult.validationStats
          }
        );

        if (bigQueryResult.success) {
          logger.info(`✅ Datos ${dataType} procesados exitosamente: GCS → BigQuery`);

          // Limpiar archivo GCS si está configurado
          if (process.env.GCS_CLEANUP_PROCESSED_FILES !== 'false') {
            try {
              await this.gcsAdapter.deleteFile(gcsResult.fileName);
              logger.debug(`🗑️ Archivo GCS ${dataType} limpiado: ${gcsResult.fileName}`);
            } catch (cleanupError) {
              logger.warn(`⚠️ Error limpiando archivo GCS ${dataType}:`, cleanupError.message);
            }
          }

          return {
            success: true,
            recordsProcessed: bigQueryResult.recordsProcessed,
            jobId: bigQueryResult.jobId,
            gcsFile: gcsResult.fileName,
            validationStats: gcsResult.validationStats,
            stage: 'completed',
            source: 'atomic_extraction'
          };
        } else {
          logger.error(`❌ Error procesando ${dataType} en BigQuery:`, bigQueryResult.error);

          // Crear archivo de recovery metadata para reintento posterior
          logger.info(`📝 Creando metadata de recovery para ${dataType}: ${gcsResult.fileName}`);

          const recoveryResult = await this.recoveryManager.createGCSBackup(
            gcsResult.fileName,
            {
              dataType,
              recordCount: gcsResult.recordsProcessed,
              source: `atomic_extraction:${dataType}:history:global`,
              processingId: gcsResult.fileName.split('_').pop().split('.')[0],
              validationStats: gcsResult.validationStats,
              bigQueryError: bigQueryResult.error,
              gcsUri: gcsResult.gcsPath
            },
            data // Datos originales para fallback
          );

          if (recoveryResult.success) {
            logger.info(`✅ Recovery metadata creado: ${recoveryResult.backupId}`);
          } else {
            logger.error(`❌ Error creando recovery metadata: ${recoveryResult.error}`);
          }

          // El archivo queda en GCS para recovery posterior
          return {
            success: false,
            error: bigQueryResult.error,
            recordsProcessed: 0,
            stage: 'bigquery_processing',
            gcsFile: gcsResult.fileName,
            source: 'atomic_extraction',
            recoveryCreated: recoveryResult.success,
            recoveryId: recoveryResult.backupId
          };
        }
      } else {
        // Paso 3: Si falla GCS, crear backup local
        logger.warn(`⚠️ Falla en subida a GCS para ${dataType}, creando backup local...`);

        const backupResult = await this.backupManager.saveToLocalBackup(data, dataType, {
          extractedAt: new Date().toISOString(),
          source: `atomic_extraction:${dataType}:history:global`,
          gcsError: gcsResult.error
        });

        if (backupResult.success) {
          logger.info(`💾 Backup local creado para ${dataType}: ${backupResult.backupId}`);

          return {
            success: false, // Falla temporal, se reintentará desde backup
            error: gcsResult.error,
            recordsProcessed: 0,
            stage: 'gcs_upload_failed',
            backupCreated: true,
            backupId: backupResult.backupId,
            source: 'atomic_extraction'
          };
        } else {
          logger.error(`❌ Error creando backup local para ${dataType}:`, backupResult.error);

          return {
            success: false,
            error: `GCS upload failed and backup creation failed: ${gcsResult.error} | ${backupResult.error}`,
            recordsProcessed: 0,
            stage: 'backup_creation_failed',
            backupCreated: false,
            source: 'atomic_extraction'
          };
        }
      }

    } catch (error) {
      logger.error(`❌ Error procesando datos ${dataType} extraídos:`, error.message);
      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        stage: 'error',
        source: 'atomic_extraction'
      };
    }
  }



  /**
   * Valida datos según su tipo
   */
  async validateDataByType(dataType, data) {
    try {
      if (dataType === 'gps') {
        return await this.dataSeparator.validateGPSData(data);
      } else if (dataType === 'mobile') {
        return await this.dataSeparator.validateMobileData(data);
      } else {
        throw new Error(`Tipo de datos no soportado: ${dataType}`);
      }
    } catch (error) {
      logger.error(`❌ Error validando datos ${dataType}:`, error.message);
      return {
        isValid: false,
        validData: [],
        invalidData: [],
        errors: [error.message],
        stats: { total: 0, valid: 0, invalid: 0, validationRate: 0 }
      };
    }
  }



  /**
   * Obtiene estadísticas del procesador con extracción atómica
   */
  async getProcessorStats() {
    try {
      const gpsStats = await this.redisRepo.getGPSStats();
      const mobileStats = await this.redisRepo.getMobileStats();

      const [gcsStatus, bigQueryStatus, recoveryStatus, atomicStats, backupStats] = await Promise.all([
        this.gcsAdapter.getStatus(),
        this.bigQueryProcessor.getStatus(),
        this.recoveryManager.getStatus(),
        this.atomicProcessor.getStats(),
        this.backupManager.getBackupStats()
      ]);

      const metrics = await this.metrics.getMetrics();

      return {
        redis: {
          gps: gpsStats,
          mobile: mobileStats,
          total: gpsStats.totalRecords + mobileStats.totalRecords
        },
        gcs: gcsStatus,
        bigQuery: bigQueryStatus,
        recovery: {
          initialized: recoveryStatus.initialized,
          pendingFiles: recoveryStatus.gcsRecoveryStats?.pending || 0,
          completedFiles: recoveryStatus.gcsRecoveryStats?.completed || 0
        },
        atomicProcessor: {
          initialized: atomicStats.initialized,
          redis: (atomicStats.redis && atomicStats.redis.gps) ? atomicStats.redis : { gps: { totalRecords: 0 }, mobile: { totalRecords: 0 }, total: 0 }
        },
        localBackups: {
          total: backupStats?.total || 0,
          pending: backupStats?.pending || 0,
          processing: backupStats?.processing || 0,
          completed: backupStats?.completed || 0,
          failed: backupStats?.failed || 0,
          totalRecords: backupStats?.totalRecords || 0,
          oldestPending: backupStats?.oldestPending || null
        },
        metrics: metrics,
        processor: {
          isProcessing: this.isProcessing,
          lastProcessed: metrics.lastProcessing || null,
          flowType: 'atomic_extraction_gcs_bigquery',
          atomicExtractionEnabled: true
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas del procesador:', error.message);
      return {
        error: error.message,
        processor: {
          isProcessing: this.isProcessing,
          flowType: 'atomic_extraction_gcs_bigquery',
          atomicExtractionEnabled: true
        }
      };
    }
  }

  /**
   * Verifica la salud del servicio con extracción atómica
   */
  async healthCheck() {
    try {
      const [redisHealth, gcsStatus, bigQueryStatus, recoveryStatus, atomicHealth] = await Promise.all([
        this.redisRepo.ping(),
        this.gcsAdapter.getStatus(),
        this.bigQueryProcessor.getStatus(),
        this.recoveryManager.getStatus(),
        this.atomicProcessor.healthCheck()
      ]);

      const services = {
        redis: redisHealth ? 'healthy' : 'unhealthy',
        gcs: gcsStatus.initialized ? 'healthy' : 'unhealthy',
        bigQuery: bigQueryStatus.initialized ? 'healthy' : 'unhealthy',
        recovery: recoveryStatus.initialized ? 'healthy' : 'unhealthy',
        atomicProcessor: atomicHealth.healthy ? 'healthy' : 'unhealthy'
      };

      const isHealthy = Object.values(services).every(status => status === 'healthy');

      // Obtener estadísticas de backups para el health check
      let backupStats = null;
      try {
        backupStats = await this.backupManager.getBackupStats();
      } catch (backupError) {
        logger.warn('⚠️ Error obteniendo estadísticas de backup para health check:', backupError.message);
      }

      return {
        healthy: isHealthy,
        services,
        details: {
          gcs: {
            simulationMode: gcsStatus.simulationMode || false,
            bucketName: gcsStatus.bucketName
          },
          bigQuery: {
            simulationMode: bigQueryStatus.simulationMode || false,
            projectId: bigQueryStatus.projectId,
            datasetId: bigQueryStatus.datasetId
          },
          recovery: {
            pendingFiles: recoveryStatus.gcsRecoveryStats?.pending || 0
          },
          atomicProcessor: {
            initialized: atomicHealth.initialized,
            redis: atomicHealth.redis
          },
          localBackups: {
            pending: backupStats?.pending || 0,
            failed: backupStats?.failed || 0,
            oldestPending: backupStats?.oldestPending || null
          }
        },
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en health check:', error.message);
      return {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Actualiza la metadata de un archivo en GCS
   * @param {string} fileName - Nombre del archivo
   * @param {Object} newMetadata - Nueva metadata a agregar
   */
  async updateFileMetadata(fileName, newMetadata) {
    try {
      if (this.gcsAdapter.simulationMode) {
        // En modo simulación, actualizar el archivo de metadata local
        const metadataPath = path.join(this.gcsAdapter.localStoragePath, `${fileName}.metadata.json`);

        let existingMetadata = {};
        try {
          const content = await fs.readFile(metadataPath, 'utf8');
          existingMetadata = JSON.parse(content);
        } catch (readError) {
          // Si no existe, crear nueva estructura
          existingMetadata = { metadata: {} };
        }

        // Actualizar metadata
        existingMetadata.metadata = {
          ...existingMetadata.metadata,
          ...newMetadata
        };

        await fs.writeFile(metadataPath, JSON.stringify(existingMetadata, null, 2), 'utf8');
        logger.debug(`📝 Metadata simulada actualizada: ${fileName}`);

      } else {
        // En modo real, actualizar metadata en GCS
        const file = this.gcsAdapter.bucket.file(fileName);
        const [metadata] = await file.getMetadata();

        const updatedMetadata = {
          ...metadata,
          metadata: {
            ...metadata.metadata,
            ...newMetadata
          }
        };

        await file.setMetadata(updatedMetadata);
        logger.debug(`📝 Metadata GCS actualizada: ${fileName}`);
      }

    } catch (error) {
      logger.error(`❌ Error actualizando metadata de ${fileName}:`, error.message);
      throw error;
    }
  }

  /**
   * Limpia recursos del procesador con extracción atómica
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del procesador GPS...');

      await Promise.all([
        this.redisRepo.disconnect(),
        this.gcsAdapter.cleanup(),
        this.bigQueryProcessor.cleanup(),
        this.recoveryManager.cleanup(),
        this.atomicProcessor.cleanup(),
        this.metrics.flush()
      ]);

      logger.info('✅ Recursos del procesador GPS limpiados exitosamente');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del procesador GPS:', error.message);
    }
  }
}