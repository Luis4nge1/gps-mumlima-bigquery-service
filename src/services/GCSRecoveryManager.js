import { BackupManager } from '../utils/BackupManager.js';
import { logger } from '../utils/logger.js';
import { FileUtils } from '../utils/FileUtils.js';
import { config } from '../config/env.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Gestor de recovery específico para el flujo GCS-BigQuery
 * Extiende BackupManager para manejar archivos GCS pendientes y recovery
 */
export class GCSRecoveryManager extends BackupManager {
  constructor(gcsAdapter, bigQueryProcessor) {
    super();
    this.gcsAdapter = gcsAdapter;
    this.bigQueryProcessor = bigQueryProcessor;
    this.gcsRecoveryPath = path.join(this.backupPath, 'gcs-recovery');
    this.maxRetryAttempts = parseInt(process.env.GCS_MAX_RETRY_ATTEMPTS) || 3;
    this.cleanupProcessedFiles = process.env.GCS_CLEANUP_PROCESSED_FILES !== 'false';
  }

  /**
   * Inicializa el gestor de recovery GCS
   */
  async initialize() {
    try {
      await FileUtils.ensureDirectoryExists(this.gcsRecoveryPath);
      logger.info('✅ GCS Recovery Manager inicializado');
    } catch (error) {
      logger.error('❌ Error inicializando GCS Recovery Manager:', error.message);
      throw error;
    }
  }

  /**
   * Crea un backup con referencia GCS para recovery posterior
   * @param {string} gcsFileName - Nombre del archivo en GCS
   * @param {Object} metadata - Metadata del archivo
   * @param {Object} originalData - Datos originales de Redis (opcional)
   */
  async createGCSBackup(gcsFileName, metadata = {}, originalData = null) {
    try {
      const timestamp = new Date().toISOString();
      const backupId = `gcs_recovery_${timestamp.replace(/[:.]/g, '-')}_${Math.random().toString(36).slice(2, 8)}`;

      const gcsBackupData = {
        id: backupId,
        type: 'gcs_recovery',
        timestamp,
        status: 'pending', // pending, processing, completed, failed
        retryCount: 0,
        maxRetries: this.maxRetryAttempts,
        gcsFileName,
        metadata: {
          dataType: metadata.dataType || 'unknown',
          recordCount: metadata.recordCount || 0,
          source: metadata.source || 'redis',
          processingId: metadata.processingId,
          gcsUri: `gs://${this.gcsAdapter.bucketName}/${gcsFileName}`,
          originalSize: metadata.originalSize || 0,
          checksum: metadata.checksum,
          ...metadata
        },
        originalData, // Datos originales de Redis para fallback
        error: null,
        processedAt: null,
        createdAt: timestamp,
        lastRetryAt: null
      };

      // Guardar archivo de recovery
      const recoveryFile = path.join(this.gcsRecoveryPath, `${backupId}.json`);
      await FileUtils.writeJsonFile(recoveryFile, gcsBackupData);

      logger.info(`💾 GCS Recovery backup creado: ${backupId} para archivo ${gcsFileName}`);

      return {
        success: true,
        backupId,
        filePath: recoveryFile,
        gcsFileName
      };

    } catch (error) {
      logger.error('❌ Error creando GCS backup:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Obtiene archivos GCS pendientes de procesamiento (método público)
   */
  async getPendingGCSFiles() {
    return await this.getGCSPendingFiles();
  }

  /**
   * Obtiene archivos GCS pendientes de procesamiento (método interno)
   */
  async getGCSPendingFiles() {
    try {
      await FileUtils.ensureDirectoryExists(this.gcsRecoveryPath);

      const files = await fs.readdir(this.gcsRecoveryPath);
      const recoveryFiles = files.filter(file =>
        file.startsWith('gcs_recovery_') && file.endsWith('.json')
      );

      const pendingFiles = [];

      for (const file of recoveryFiles) {
        try {
          const filePath = path.join(this.gcsRecoveryPath, file);
          const recoveryData = await FileUtils.readJsonFile(filePath);

          if (recoveryData.status === 'pending' && recoveryData.retryCount < recoveryData.maxRetries) {
            pendingFiles.push({
              ...recoveryData,
              filePath
            });
          }
        } catch (error) {
          logger.warn(`⚠️ Error leyendo recovery file ${file}:`, error.message);
        }
      }

      // Ordenar por timestamp (más antiguos primero)
      pendingFiles.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      return pendingFiles;

    } catch (error) {
      logger.error('❌ Error obteniendo archivos GCS pendientes:', error.message);
      return [];
    }
  }

  /**
   * Procesa archivos GCS pendientes hacia BigQuery
   */
  async processGCSPendingFiles() {
    try {
      // Paso 1: Procesar archivos de recovery metadata
      const pendingFiles = await this.getGCSPendingFiles();

      // Paso 2: Buscar archivos huérfanos en GCS (archivos reales sin metadata)
      const orphanFiles = await this.findOrphanGCSFiles();

      const totalFiles = pendingFiles.length + orphanFiles.length;

      if (totalFiles === 0) {
        logger.debug('📋 No hay archivos GCS pendientes para procesar');
        return {
          success: true,
          processed: 0,
          message: 'No pending GCS files'
        };
      }

      logger.info(`🔄 Encontrados ${pendingFiles.length} archivos de recovery + ${orphanFiles.length} archivos huérfanos en GCS`);

      // Procesar archivos de recovery primero
      const recoveryResult = await this.processRecoveryFiles(pendingFiles);

      // Procesar archivos huérfanos después
      const orphanResult = await this.processOrphanFiles(orphanFiles);

      return {
        success: recoveryResult.success && orphanResult.success,
        processed: recoveryResult.processed + orphanResult.processed,
        failed: recoveryResult.failed + orphanResult.failed,
        total: totalFiles,
        results: [...recoveryResult.results, ...orphanResult.results],
        recovery: recoveryResult,
        orphans: orphanResult
      };

    } catch (error) {
      logger.error('❌ Error procesando archivos GCS pendientes:', error.message);
      return {
        success: false,
        error: error.message,
        processed: 0
      };
    }
  }

  /**
   * Busca archivos huérfanos en GCS (archivos sin metadata de recovery)
   */
  async findOrphanGCSFiles() {
    try {
      logger.debug('🔍 Buscando archivos huérfanos en GCS...');

      // Obtener archivos GPS y Mobile de GCS
      const [gpsFiles, mobileFiles] = await Promise.all([
        this.gcsAdapter.listFiles({ prefix: 'gps-data/' }),
        this.gcsAdapter.listFiles({ prefix: 'mobile-data/' })
      ]);

      const allFiles = [...gpsFiles, ...mobileFiles];

      if (allFiles.length === 0) {
        logger.debug('📋 No hay archivos en GCS para verificar');
        return [];
      }

      logger.debug(`📁 Encontrados ${allFiles.length} archivos en GCS (${gpsFiles.length} GPS + ${mobileFiles.length} Mobile)`);

      // Obtener archivos de recovery existentes para evitar duplicados
      const existingRecoveryFiles = await this.getGCSPendingFiles();
      const existingFileNames = existingRecoveryFiles.map(f => f.gcsFileName);

      // Filtrar archivos que no tienen metadata de recovery
      const orphanFiles = allFiles.filter(file => !existingFileNames.includes(file.name));

      logger.info(`🔍 Encontrados ${orphanFiles.length} archivos huérfanos en GCS (sin metadata de recovery)`);

      return orphanFiles.map(file => ({
        name: file.name,
        gcsPath: file.gcsPath,
        size: file.size,
        created: file.created,
        metadata: file.metadata || {},
        dataType: file.name.includes('gps-data/') ? 'gps' : 'mobile',
        isOrphan: true
      }));

    } catch (error) {
      logger.error('❌ Error buscando archivos huérfanos en GCS:', error.message);
      return [];
    }
  }

  /**
   * Procesa archivos de recovery metadata
   */
  async processRecoveryFiles(pendingFiles) {
    if (pendingFiles.length === 0) {
      return { success: true, processed: 0, failed: 0, results: [] };
    }

    try {
      logger.info(`🔄 Procesando ${pendingFiles.length} archivos GCS pendientes...`);

      let processedCount = 0;
      let failedCount = 0;
      const results = [];

      for (const pendingFile of pendingFiles) {
        try {
          // Marcar como procesando
          await this.markGCSAsProcessing(pendingFile.id, pendingFile.filePath);

          // Verificar que el archivo existe en GCS
          const gcsFiles = await this.gcsAdapter.listFiles({ prefix: pendingFile.gcsFileName });
          const fileExists = gcsFiles.some(file => file.name === pendingFile.gcsFileName);

          if (!fileExists) {
            // Si el archivo no existe en GCS, intentar recovery desde datos originales
            const recoveryResult = await this.recoverFromOriginalData(pendingFile);

            if (recoveryResult.success) {
              await this.markGCSAsCompleted(pendingFile.id, pendingFile.filePath, recoveryResult);
              processedCount++;
              results.push({
                backupId: pendingFile.id,
                success: true,
                method: 'original_data_recovery',
                recordsProcessed: recoveryResult.recordsProcessed || 0
              });
            } else {
              await this.markGCSAsFailed(pendingFile.id, pendingFile.filePath, 'Archivo GCS no encontrado y recovery desde datos originales falló');
              failedCount++;
              results.push({
                backupId: pendingFile.id,
                success: false,
                error: 'GCS file not found and original data recovery failed'
              });
            }
            continue;
          }

          // Procesar archivo desde GCS hacia BigQuery
          const gcsUri = pendingFile.metadata.gcsUri;
          const dataType = pendingFile.metadata.dataType;

          const result = await this.bigQueryProcessor.processGCSFile(gcsUri, dataType, pendingFile.metadata);

          if (result.success) {
            await this.markGCSAsCompleted(pendingFile.id, pendingFile.filePath, result);

            // Limpiar archivo GCS si está configurado
            if (this.cleanupProcessedFiles) {
              try {
                await this.gcsAdapter.deleteFile(pendingFile.gcsFileName);
                logger.debug(`🗑️ Archivo GCS limpiado: ${pendingFile.gcsFileName}`);
              } catch (cleanupError) {
                logger.warn(`⚠️ Error limpiando archivo GCS ${pendingFile.gcsFileName}:`, cleanupError.message);
              }
            }

            processedCount++;
            results.push({
              backupId: pendingFile.id,
              success: true,
              method: 'gcs_to_bigquery',
              recordsProcessed: result.recordsProcessed || 0,
              jobId: result.jobId
            });

          } else {
            await this.markGCSAsFailed(pendingFile.id, pendingFile.filePath, result.error || 'Unknown BigQuery processing error');
            failedCount++;

            results.push({
              backupId: pendingFile.id,
              success: false,
              error: result.error
            });
          }

          // Pequeña pausa entre procesamientos
          if (pendingFiles.length > 1) {
            await new Promise(resolve => setTimeout(resolve, 2000));
          }

        } catch (error) {
          await this.markGCSAsFailed(pendingFile.id, pendingFile.filePath, error);
          failedCount++;

          results.push({
            backupId: pendingFile.id,
            success: false,
            error: error.message
          });

          logger.error(`❌ Error procesando archivo GCS pendiente ${pendingFile.id}:`, error.message);
        }
      }

      logger.info(`✅ Procesamiento de recovery completado: ${processedCount} exitosos, ${failedCount} fallidos`);

      return {
        success: failedCount === 0,
        processed: processedCount,
        failed: failedCount,
        total: pendingFiles.length,
        results
      };

    } catch (error) {
      logger.error('❌ Error procesando archivos de recovery:', error.message);
      return {
        success: false,
        error: error.message,
        processed: 0,
        failed: 0,
        results: []
      };
    }
  }

  /**
   * Procesa archivos huérfanos de GCS hacia BigQuery
   */
  async processOrphanFiles(orphanFiles) {
    if (orphanFiles.length === 0) {
      return { success: true, processed: 0, failed: 0, results: [] };
    }

    try {
      logger.info(`🔄 Procesando ${orphanFiles.length} archivos huérfanos de GCS...`);

      let processedCount = 0;
      let failedCount = 0;
      const results = [];

      for (const orphanFile of orphanFiles) {
        try {
          logger.info(`📄 Procesando archivo huérfano: ${orphanFile.name}`);

          // Procesar archivo directamente desde GCS hacia BigQuery
          const result = await this.bigQueryProcessor.processGCSFile(
            orphanFile.gcsPath,
            orphanFile.dataType,
            {
              dataType: orphanFile.dataType,
              recordCount: orphanFile.metadata?.recordCount || 0,
              source: `orphan_recovery:${orphanFile.dataType}`,
              processingId: `orphan_${Date.now()}`,
              isOrphan: true,
              originalFile: orphanFile.name
            }
          );

          if (result.success) {
            // Limpiar archivo GCS si está configurado
            if (this.cleanupProcessedFiles) {
              try {
                await this.gcsAdapter.deleteFile(orphanFile.name);
                logger.info(`🗑️ Archivo huérfano procesado y eliminado: ${orphanFile.name}`);
              } catch (cleanupError) {
                logger.warn(`⚠️ Error eliminando archivo huérfano ${orphanFile.name}:`, cleanupError.message);
              }
            } else {
              logger.info(`✅ Archivo huérfano procesado (conservado en GCS): ${orphanFile.name}`);
            }

            processedCount++;
            results.push({
              fileName: orphanFile.name,
              success: true,
              method: 'orphan_gcs_to_bigquery',
              recordsProcessed: result.recordsProcessed || 0,
              jobId: result.jobId,
              dataType: orphanFile.dataType
            });

          } else {
            logger.error(`❌ Error procesando archivo huérfano ${orphanFile.name}:`, result.error);
            failedCount++;

            results.push({
              fileName: orphanFile.name,
              success: false,
              error: result.error,
              dataType: orphanFile.dataType
            });
          }

          // Pequeña pausa entre procesamientos
          if (orphanFiles.length > 1) {
            await new Promise(resolve => setTimeout(resolve, 1000));
          }

        } catch (error) {
          failedCount++;

          results.push({
            fileName: orphanFile.name,
            success: false,
            error: error.message,
            dataType: orphanFile.dataType
          });

          logger.error(`❌ Error procesando archivo huérfano ${orphanFile.name}:`, error.message);
        }
      }

      logger.info(`✅ Procesamiento de huérfanos completado: ${processedCount} exitosos, ${failedCount} fallidos`);

      return {
        success: failedCount === 0,
        processed: processedCount,
        failed: failedCount,
        total: orphanFiles.length,
        results
      };

    } catch (error) {
      logger.error('❌ Error procesando archivos huérfanos:', error.message);
      return {
        success: false,
        error: error.message,
        processed: 0,
        failed: 0,
        results: []
      };
    }
  }

  /**
   * Intenta recovery desde datos originales cuando el archivo GCS no existe
   */
  async recoverFromOriginalData(pendingFile) {
    try {
      if (!pendingFile.originalData || !Array.isArray(pendingFile.originalData)) {
        return {
          success: false,
          error: 'No hay datos originales disponibles para recovery'
        };
      }

      logger.info(`🔄 Intentando recovery desde datos originales para ${pendingFile.id}`);

      // Re-subir datos a GCS
      const uploadResult = await this.gcsAdapter.uploadJSON(
        pendingFile.originalData,
        pendingFile.gcsFileName,
        pendingFile.metadata
      );

      if (!uploadResult.success) {
        return {
          success: false,
          error: `Error re-subiendo a GCS: ${uploadResult.error}`
        };
      }

      // Procesar hacia BigQuery
      const gcsUri = uploadResult.gcsUri;
      const dataType = pendingFile.metadata.dataType;

      const bigQueryResult = await this.bigQueryProcessor.processGCSFile(gcsUri, dataType, pendingFile.metadata);

      if (bigQueryResult.success) {
        // Limpiar archivo GCS si está configurado
        if (this.cleanupProcessedFiles) {
          try {
            await this.gcsAdapter.deleteFile(pendingFile.gcsFileName);
          } catch (cleanupError) {
            logger.warn(`⚠️ Error limpiando archivo GCS durante recovery:`, cleanupError.message);
          }
        }

        return {
          success: true,
          recordsProcessed: bigQueryResult.recordsProcessed,
          jobId: bigQueryResult.jobId,
          method: 'original_data_recovery'
        };
      } else {
        return {
          success: false,
          error: `Error procesando hacia BigQuery: ${bigQueryResult.error}`
        };
      }

    } catch (error) {
      logger.error(`❌ Error en recovery desde datos originales:`, error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Marca un archivo GCS como en procesamiento
   */
  async markGCSAsProcessing(backupId, filePath) {
    try {
      const recoveryData = await FileUtils.readJsonFile(filePath);

      recoveryData.status = 'processing';
      recoveryData.retryCount++;
      recoveryData.lastRetryAt = new Date().toISOString();

      await FileUtils.writeJsonFile(filePath, recoveryData);

      logger.debug(`🔄 GCS Recovery ${backupId} marcado como procesando (intento ${recoveryData.retryCount})`);

      return recoveryData;

    } catch (error) {
      logger.error(`❌ Error marcando GCS recovery ${backupId} como procesando:`, error.message);
      throw error;
    }
  }

  /**
   * Marca un archivo GCS como completado exitosamente
   */
  async markGCSAsCompleted(backupId, filePath, result = {}) {
    try {
      const recoveryData = await FileUtils.readJsonFile(filePath);

      recoveryData.status = 'completed';
      recoveryData.processedAt = new Date().toISOString();
      recoveryData.result = result;
      recoveryData.error = null;

      await FileUtils.writeJsonFile(filePath, recoveryData);

      logger.info(`✅ GCS Recovery ${backupId} procesado exitosamente`);

      return recoveryData;

    } catch (error) {
      logger.error(`❌ Error marcando GCS recovery ${backupId} como completado:`, error.message);
      throw error;
    }
  }

  /**
   * Marca un archivo GCS como fallido
   */
  async markGCSAsFailed(backupId, filePath, error) {
    try {
      const recoveryData = await FileUtils.readJsonFile(filePath);

      recoveryData.status = recoveryData.retryCount >= recoveryData.maxRetries ? 'failed' : 'pending';
      recoveryData.error = {
        message: error.message || error,
        timestamp: new Date().toISOString(),
        retryCount: recoveryData.retryCount
      };

      await FileUtils.writeJsonFile(filePath, recoveryData);

      if (recoveryData.status === 'failed') {
        logger.error(`❌ GCS Recovery ${backupId} falló definitivamente después de ${recoveryData.retryCount} intentos`);
      } else {
        logger.warn(`⚠️ GCS Recovery ${backupId} falló, se reintentará (${recoveryData.retryCount}/${recoveryData.maxRetries})`);
      }

      return recoveryData;

    } catch (err) {
      logger.error(`❌ Error marcando GCS recovery ${backupId} como fallido:`, err.message);
      throw err;
    }
  }

  /**
   * Limpia archivos GCS procesados exitosamente
   */
  async cleanupProcessedGCSFiles(maxAge = 24 * 60 * 60 * 1000) { // 24 horas por defecto
    try {
      const files = await fs.readdir(this.gcsRecoveryPath);
      const recoveryFiles = files.filter(file =>
        file.startsWith('gcs_recovery_') && file.endsWith('.json')
      );

      let cleanedCount = 0;
      const now = Date.now();

      for (const file of recoveryFiles) {
        try {
          const filePath = path.join(this.gcsRecoveryPath, file);
          const recoveryData = await FileUtils.readJsonFile(filePath);

          // Solo limpiar archivos completados antiguos
          if (recoveryData.status === 'completed' && recoveryData.processedAt) {
            const processedTime = new Date(recoveryData.processedAt).getTime();

            if (now - processedTime > maxAge) {
              await fs.unlink(filePath);
              cleanedCount++;
              logger.debug(`🗑️ GCS Recovery completado eliminado: ${recoveryData.id}`);
            }
          }

        } catch (error) {
          logger.warn(`⚠️ Error limpiando GCS recovery ${file}:`, error.message);
        }
      }

      if (cleanedCount > 0) {
        logger.info(`🧹 ${cleanedCount} archivos GCS recovery completados eliminados`);
      }

      return {
        success: true,
        cleaned: cleanedCount
      };

    } catch (error) {
      logger.error('❌ Error limpiando archivos GCS recovery:', error.message);
      return {
        success: false,
        error: error.message,
        cleaned: 0
      };
    }
  }

  /**
   * Obtiene estadísticas de recovery GCS
   */
  async getRecoveryStats() {
    return await this.getGCSRecoveryStats();
  }

  /**
   * Obtiene estadísticas de recovery GCS (método interno)
   */
  async getGCSRecoveryStats() {
    try {
      await FileUtils.ensureDirectoryExists(this.gcsRecoveryPath);

      const files = await fs.readdir(this.gcsRecoveryPath);
      const recoveryFiles = files.filter(file =>
        file.startsWith('gcs_recovery_') && file.endsWith('.json')
      );

      const stats = {
        total: 0,
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0,
        totalRecords: 0,
        byDataType: {
          gps: { total: 0, pending: 0, completed: 0, failed: 0 },
          mobile: { total: 0, pending: 0, completed: 0, failed: 0 }
        },
        oldestPending: null,
        newestCompleted: null
      };

      for (const file of recoveryFiles) {
        try {
          const filePath = path.join(this.gcsRecoveryPath, file);
          const recoveryData = await FileUtils.readJsonFile(filePath);

          stats.total++;
          stats[recoveryData.status]++;
          stats.totalRecords += recoveryData.metadata?.recordCount || 0;

          // Estadísticas por tipo de datos
          const dataType = recoveryData.metadata?.dataType || 'unknown';
          if (stats.byDataType[dataType]) {
            stats.byDataType[dataType].total++;
            stats.byDataType[dataType][recoveryData.status]++;
          }

          if (recoveryData.status === 'pending' && (!stats.oldestPending || recoveryData.timestamp < stats.oldestPending)) {
            stats.oldestPending = recoveryData.timestamp;
          }

          if (recoveryData.status === 'completed' && (!stats.newestCompleted || recoveryData.timestamp > stats.newestCompleted)) {
            stats.newestCompleted = recoveryData.processedAt;
          }

        } catch (error) {
          logger.warn(`⚠️ Error leyendo estadísticas de GCS recovery ${file}:`, error.message);
        }
      }

      return stats;

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas de GCS recovery:', error.message);
      return null;
    }
  }

  /**
   * Obtiene el estado general del recovery manager
   */
  async getStatus() {
    try {
      const [backupStats, gcsStats] = await Promise.all([
        this.getBackupStats(),
        this.getGCSRecoveryStats()
      ]);

      return {
        initialized: true,
        backupPath: this.backupPath,
        gcsRecoveryPath: this.gcsRecoveryPath,
        maxRetryAttempts: this.maxRetryAttempts,
        cleanupProcessedFiles: this.cleanupProcessedFiles,
        backupStats,
        gcsRecoveryStats: gcsStats,
        adapters: {
          gcs: this.gcsAdapter ? await this.gcsAdapter.getStatus() : null,
          bigQuery: this.bigQueryProcessor ? await this.bigQueryProcessor.getStatus() : null
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado del GCS Recovery Manager:', error.message);
      return {
        initialized: false,
        error: error.message
      };
    }
  }

  /**
   * Limpia recursos
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del GCS Recovery Manager...');

      // Limpiar archivos completados antiguos
      await this.cleanupCompletedBackups();
      await this.cleanupProcessedGCSFiles();

      logger.info('✅ Recursos del GCS Recovery Manager limpiados');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del GCS Recovery Manager:', error.message);
    }
  }
}