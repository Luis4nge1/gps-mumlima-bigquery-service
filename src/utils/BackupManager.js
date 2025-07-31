import fs from 'fs/promises';
import path from 'path';
import { logger } from './logger.js';
import { config } from '../config/env.js';
import { FileUtils } from './FileUtils.js';
import { MetricsCollector } from './MetricsCollector.js';

/**
 * Gestor de backups con sistema de recuperación automática
 * Mejorado para manejo de archivos locales con soporte para GPS y Mobile data
 */
export class BackupManager {
  constructor() {
    this.backupPath = config.backup.storagePath || 'tmp/atomic-backups/';
    this.maxRetries = config.backup.maxRetries || 3;
    this.retentionHours = config.backup.retentionHours || 24;
    this.retryDelay = 5000; // 5 segundos
    this.metrics = MetricsCollector.getInstance();
  }

  /**
   * Crea un backup con metadata de estado
   */
  async createBackup(data, metadata = {}) {
    try {
      const timestamp = new Date().toISOString();
      const backupId = `gps_backup_${timestamp.replace(/[:.]/g, '-')}`;
      
      // ✅ CRÍTICO: Validar integridad de datos antes de backup
      const dataValidation = this.validateBackupData(data);
      if (!dataValidation.isValid) {
        throw new Error(`Datos inválidos para backup: ${dataValidation.errors.join(', ')}`);
      }
      
      const backupData = {
        id: backupId,
        timestamp,
        status: 'pending', // pending, processing, completed, failed
        retryCount: 0,
        maxRetries: this.maxRetries,
        metadata: {
          recordCount: Array.isArray(data) ? data.length : 0,
          source: 'gps_processor',
          checksum: this.calculateChecksum(data),
          dataSize: JSON.stringify(data).length,
          ...metadata
        },
        data: data,
        error: null,
        processedAt: null,
        createdAt: timestamp
      };

      // Guardar archivo de backup
      const backupFile = path.join(this.backupPath, `${backupId}.json`);
      await FileUtils.writeJsonFile(backupFile, backupData);

      logger.info(`💾 Backup creado: ${backupId} (${backupData.metadata.recordCount} registros)`);
      
      // Registrar métricas de backup creado
      await this.metrics.recordBackupOperation('created', {
        id: backupId,
        type: 'gps', // Legacy backup type
        recordCount: backupData.metadata.recordCount
      });
      
      return {
        success: true,
        backupId,
        filePath: backupFile,
        recordCount: backupData.metadata.recordCount
      };

    } catch (error) {
      logger.error('❌ Error creando backup:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Obtiene backups pendientes de procesamiento
   */
  async getPendingBackups() {
    try {
      await FileUtils.ensureDirectoryExists(this.backupPath);
      
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        file.startsWith('gps_backup_') && file.endsWith('.json')
      );

      const pendingBackups = [];

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          if (backupData.status === 'pending' && backupData.retryCount < backupData.maxRetries) {
            pendingBackups.push({
              ...backupData,
              filePath
            });
          }
        } catch (error) {
          logger.warn(`⚠️ Error leyendo backup ${file}:`, error.message);
        }
      }

      // Ordenar por timestamp (más antiguos primero)
      pendingBackups.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      return pendingBackups;

    } catch (error) {
      logger.error('❌ Error obteniendo backups pendientes:', error.message);
      return [];
    }
  }

  /**
   * Marca un backup como en procesamiento
   */
  async markAsProcessing(backupId, filePath) {
    try {
      const backupData = await FileUtils.readJsonFile(filePath);
      
      backupData.status = 'processing';
      backupData.retryCount++;
      backupData.lastRetryAt = new Date().toISOString();
      
      await FileUtils.writeJsonFile(filePath, backupData);
      
      logger.debug(`🔄 Backup ${backupId} marcado como procesando (intento ${backupData.retryCount})`);
      
      // Registrar métricas de backup en procesamiento
      await this.metrics.recordBackupOperation('processing', {
        id: backupId,
        type: backupData.type || 'gps',
        retryCount: backupData.retryCount
      });
      
      return backupData;

    } catch (error) {
      logger.error(`❌ Error marcando backup ${backupId} como procesando:`, error.message);
      throw error;
    }
  }

  /**
   * Marca un backup como completado exitosamente
   */
  async markAsCompleted(backupId, filePath, result = {}) {
    try {
      const backupData = await FileUtils.readJsonFile(filePath);
      
      backupData.status = 'completed';
      backupData.processedAt = new Date().toISOString();
      backupData.result = result;
      backupData.error = null;
      
      await FileUtils.writeJsonFile(filePath, backupData);
      
      logger.info(`✅ Backup ${backupId} procesado exitosamente`);
      
      // Registrar métricas de backup completado
      await this.metrics.recordBackupOperation('completed', {
        id: backupId,
        type: backupData.type || 'gps',
        recordCount: backupData.metadata?.recordCount || 0,
        retryTime: backupData.lastRetryAt ? 
          new Date() - new Date(backupData.lastRetryAt) : 0
      });
      
      return backupData;

    } catch (error) {
      logger.error(`❌ Error marcando backup ${backupId} como completado:`, error.message);
      throw error;
    }
  }

  /**
   * Marca un backup como fallido
   */
  async markAsFailed(backupId, filePath, error) {
    try {
      const backupData = await FileUtils.readJsonFile(filePath);
      
      backupData.status = backupData.retryCount >= backupData.maxRetries ? 'failed' : 'pending';
      backupData.error = {
        message: error.message || error,
        timestamp: new Date().toISOString(),
        retryCount: backupData.retryCount
      };
      
      await FileUtils.writeJsonFile(filePath, backupData);
      
      if (backupData.status === 'failed') {
        logger.error(`❌ Backup ${backupId} falló definitivamente después de ${backupData.retryCount} intentos`);
      } else {
        logger.warn(`⚠️ Backup ${backupId} falló, se reintentará (${backupData.retryCount}/${backupData.maxRetries})`);
      }
      
      // Registrar métricas de backup fallido
      await this.metrics.recordBackupOperation('failed', {
        id: backupId,
        type: backupData.type || 'gps',
        retryCount: backupData.retryCount,
        maxRetries: backupData.maxRetries
      }, false, error.message || error);
      
      return backupData;

    } catch (err) {
      logger.error(`❌ Error marcando backup ${backupId} como fallido:`, err.message);
      throw err;
    }
  }

  /**
   * Procesa todos los backups pendientes
   */
  async processAllPendingBackups(processor) {
    try {
      const pendingBackups = await this.getPendingBackups();
      
      if (pendingBackups.length === 0) {
        logger.debug('📋 No hay backups pendientes para procesar');
        return {
          success: true,
          processed: 0,
          message: 'No pending backups'
        };
      }

      logger.info(`🔄 Procesando ${pendingBackups.length} backups pendientes...`);

      let processedCount = 0;
      let failedCount = 0;
      const results = [];

      for (const backup of pendingBackups) {
        try {
          // Marcar como procesando
          await this.markAsProcessing(backup.id, backup.filePath);
          
          // Procesar datos del backup
          const result = await processor(backup.data);
          
          if (result.success) {
            await this.markAsCompleted(backup.id, backup.filePath, result);
            processedCount++;
            
            results.push({
              backupId: backup.id,
              success: true,
              recordsProcessed: result.recordsProcessed || 0
            });
            
          } else {
            await this.markAsFailed(backup.id, backup.filePath, result.error || 'Unknown error');
            failedCount++;
            
            results.push({
              backupId: backup.id,
              success: false,
              error: result.error
            });
          }

          // Pequeña pausa entre procesamientos
          if (pendingBackups.length > 1) {
            await new Promise(resolve => setTimeout(resolve, 1000));
          }

        } catch (error) {
          await this.markAsFailed(backup.id, backup.filePath, error);
          failedCount++;
          
          results.push({
            backupId: backup.id,
            success: false,
            error: error.message
          });
          
          logger.error(`❌ Error procesando backup ${backup.id}:`, error.message);
        }
      }

      logger.info(`✅ Procesamiento de backups completado: ${processedCount} exitosos, ${failedCount} fallidos`);

      return {
        success: failedCount === 0,
        processed: processedCount,
        failed: failedCount,
        total: pendingBackups.length,
        results
      };

    } catch (error) {
      logger.error('❌ Error procesando backups pendientes:', error.message);
      return {
        success: false,
        error: error.message,
        processed: 0
      };
    }
  }

  /**
   * Obtiene estadísticas de backups
   */
  async getBackupStats() {
    try {
      await FileUtils.ensureDirectoryExists(this.backupPath);
      
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        file.startsWith('gps_backup_') && file.endsWith('.json')
      );

      const stats = {
        total: 0,
        pending: 0,
        processing: 0,
        completed: 0,
        failed: 0,
        totalRecords: 0,
        oldestPending: null,
        newestCompleted: null
      };

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          stats.total++;
          stats[backupData.status]++;
          stats.totalRecords += backupData.metadata?.recordCount || 0;
          
          if (backupData.status === 'pending' && (!stats.oldestPending || backupData.timestamp < stats.oldestPending)) {
            stats.oldestPending = backupData.timestamp;
          }
          
          if (backupData.status === 'completed' && (!stats.newestCompleted || backupData.timestamp > stats.newestCompleted)) {
            stats.newestCompleted = backupData.processedAt;
          }
          
        } catch (error) {
          logger.warn(`⚠️ Error leyendo estadísticas de backup ${file}:`, error.message);
        }
      }

      return stats;

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas de backups:', error.message);
      return null;
    }
  }

  /**
   * Valida datos antes de crear backup
   */
  validateBackupData(data) {
    const errors = [];
    
    if (!data) {
      errors.push('Datos nulos o undefined');
    } else if (!Array.isArray(data)) {
      errors.push('Datos deben ser un array');
    } else if (data.length === 0) {
      errors.push('Array de datos vacío');
    } else {
      // Validar estructura de algunos registros
      const sampleSize = Math.min(5, data.length);
      for (let i = 0; i < sampleSize; i++) {
        const record = data[i];
        if (!record || typeof record !== 'object') {
          errors.push(`Registro ${i} inválido`);
        }
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors
    };
  }

  /**
   * Calcula checksum simple para validación de integridad
   */
  calculateChecksum(data) {
    try {
      const dataString = JSON.stringify(data);
      let hash = 0;
      
      for (let i = 0; i < dataString.length; i++) {
        const char = dataString.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convertir a 32bit integer
      }
      
      return Math.abs(hash).toString(16);
    } catch (error) {
      return 'checksum_error';
    }
  }

  /**
   * Guarda datos extraídos en archivos locales cuando falla GCS upload
   * @param {Array} data - Datos extraídos de Redis
   * @param {string} type - Tipo de datos: 'gps' o 'mobile'
   * @param {Object} metadata - Metadata adicional
   * @returns {Object} Resultado de la operación
   */
  async saveToLocalBackup(data, type, metadata = {}) {
    const startTime = Date.now();
    
    try {
      logger.debug(`🔄 Iniciando creación de backup local para ${type}...`, {
        recordCount: Array.isArray(data) ? data.length : 0,
        metadata: metadata
      });

      // Validar parámetros
      if (!data || !Array.isArray(data)) {
        const error = 'Los datos deben ser un array válido';
        logger.error(`❌ Validación fallida en saveToLocalBackup: ${error}`, {
          dataType: typeof data,
          isArray: Array.isArray(data)
        });
        throw new Error(error);
      }
      
      if (!type || !['gps', 'mobile'].includes(type)) {
        const error = 'El tipo debe ser "gps" o "mobile"';
        logger.error(`❌ Validación fallida en saveToLocalBackup: ${error}`, {
          providedType: type,
          validTypes: ['gps', 'mobile']
        });
        throw new Error(error);
      }

      // Asegurar que el directorio existe
      await FileUtils.ensureDirectoryExists(this.backupPath);
      logger.debug(`📁 Directorio de backup verificado: ${this.backupPath}`);

      const timestamp = new Date().toISOString();
      const backupId = `backup_${type}_${timestamp.replace(/[:.]/g, '-')}_${this.generateShortId()}`;
      
      logger.debug(`🆔 Generado ID de backup: ${backupId}`);
      
      // Validar integridad de datos
      const dataValidation = this.validateBackupData(data);
      if (!dataValidation.isValid) {
        const error = `Datos inválidos para backup: ${dataValidation.errors.join(', ')}`;
        logger.error(`❌ Validación de datos fallida para backup ${backupId}:`, {
          errors: dataValidation.errors,
          sampleData: data.slice(0, 2) // Solo primeros 2 registros para debug
        });
        throw new Error(error);
      }
      
      const dataSize = JSON.stringify(data).length;
      const checksum = this.calculateChecksum(data);
      
      logger.debug(`📊 Datos validados para backup ${backupId}:`, {
        recordCount: data.length,
        dataSize: dataSize,
        checksum: checksum
      });
      
      const backupData = {
        id: backupId,
        type: type,
        timestamp: timestamp,
        data: data,
        metadata: {
          recordCount: data.length,
          extractedAt: timestamp,
          retryCount: 0,
          maxRetries: this.maxRetries,
          lastAttempt: null,
          errors: [],
          dataSize: dataSize,
          checksum: checksum,
          originalFailureReason: metadata.originalFailureReason || 'GCS upload failed',
          ...metadata
        },
        status: 'pending'
      };

      // Guardar archivo de backup
      const backupFile = path.join(this.backupPath, `${backupId}.json`);
      
      logger.debug(`💾 Guardando backup en archivo: ${backupFile}`);
      await FileUtils.writeJsonFile(backupFile, backupData);

      const duration = Date.now() - startTime;
      logger.info(`✅ Backup local creado exitosamente: ${backupId}`, {
        type: type,
        recordCount: data.length,
        dataSize: dataSize,
        filePath: backupFile,
        duration: `${duration}ms`,
        reason: metadata.originalFailureReason || 'GCS upload failed'
      });
      
      // Registrar métricas de backup local creado
      await this.metrics.recordBackupOperation('created', {
        id: backupId,
        type: type,
        recordCount: data.length,
        duration: duration
      });
      
      return {
        success: true,
        backupId,
        filePath: backupFile,
        recordCount: data.length,
        type: type,
        duration: duration
      };

    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(`❌ Error guardando backup local para ${type}:`, {
        error: error.message,
        stack: error.stack,
        duration: `${duration}ms`,
        recordCount: Array.isArray(data) ? data.length : 'unknown',
        metadata: metadata
      });
      
      return {
        success: false,
        error: error.message,
        duration: duration
      };
    }
  }

  /**
   * Lista archivos de backup pendientes de procesamiento
   * @returns {Array} Lista de archivos de backup pendientes
   */
  async getLocalBackupFiles() {
    const startTime = Date.now();
    
    try {
      logger.debug(`🔍 Iniciando búsqueda de backups locales pendientes en: ${this.backupPath}`);
      
      await FileUtils.ensureDirectoryExists(this.backupPath);
      
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        file.startsWith('backup_') && file.endsWith('.json')
      );

      logger.debug(`📁 Encontrados ${backupFiles.length} archivos de backup en directorio`, {
        directory: this.backupPath,
        totalFiles: files.length,
        backupFiles: backupFiles.length
      });

      const pendingBackups = [];
      const skippedBackups = {
        completed: 0,
        failed: 0,
        processing: 0,
        exceededRetries: 0,
        corrupted: 0
      };

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          logger.debug(`📄 Procesando archivo de backup: ${file}`, {
            id: backupData.id,
            type: backupData.type,
            status: backupData.status,
            retryCount: backupData.metadata?.retryCount || 0,
            maxRetries: backupData.metadata?.maxRetries || 0,
            recordCount: backupData.metadata?.recordCount || 0
          });
          
          // Solo incluir backups pendientes que no hayan excedido el máximo de reintentos
          if (backupData.status === 'pending' && 
              backupData.metadata.retryCount < backupData.metadata.maxRetries) {
            
            const backupAge = Date.now() - new Date(backupData.timestamp).getTime();
            
            pendingBackups.push({
              ...backupData,
              filePath,
              age: backupAge
            });
            
            logger.debug(`✅ Backup agregado a lista pendiente: ${backupData.id}`, {
              age: `${Math.round(backupAge / 1000)}s`,
              retryCount: backupData.metadata.retryCount,
              recordCount: backupData.metadata.recordCount
            });
          } else {
            // Contar backups omitidos por categoría
            if (backupData.status === 'completed') {
              skippedBackups.completed++;
            } else if (backupData.status === 'failed') {
              skippedBackups.failed++;
            } else if (backupData.status === 'processing') {
              skippedBackups.processing++;
            } else if (backupData.metadata.retryCount >= backupData.metadata.maxRetries) {
              skippedBackups.exceededRetries++;
            }
            
            logger.debug(`⏭️ Backup omitido: ${backupData.id}`, {
              status: backupData.status,
              retryCount: backupData.metadata?.retryCount || 0,
              maxRetries: backupData.metadata?.maxRetries || 0,
              reason: backupData.status !== 'pending' ? 'status' : 'exceeded_retries'
            });
          }
        } catch (error) {
          skippedBackups.corrupted++;
          logger.warn(`⚠️ Error leyendo backup ${file}:`, {
            error: error.message,
            filePath: path.join(this.backupPath, file)
          });
        }
      }

      // Ordenar por timestamp (más antiguos primero)
      pendingBackups.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      const duration = Date.now() - startTime;
      const oldestBackup = pendingBackups.length > 0 ? pendingBackups[0] : null;
      const newestBackup = pendingBackups.length > 0 ? pendingBackups[pendingBackups.length - 1] : null;

      logger.info(`📋 Búsqueda de backups completada: ${pendingBackups.length} pendientes`, {
        totalFiles: backupFiles.length,
        pendingBackups: pendingBackups.length,
        skipped: skippedBackups,
        duration: `${duration}ms`,
        oldestPending: oldestBackup ? {
          id: oldestBackup.id,
          age: `${Math.round(oldestBackup.age / 1000)}s`,
          type: oldestBackup.type
        } : null,
        newestPending: newestBackup ? {
          id: newestBackup.id,
          age: `${Math.round(newestBackup.age / 1000)}s`,
          type: newestBackup.type
        } : null
      });
      
      return pendingBackups;

    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error('❌ Error obteniendo archivos de backup locales:', {
        error: error.message,
        stack: error.stack,
        directory: this.backupPath,
        duration: `${duration}ms`
      });
      return [];
    }
  }

  /**
   * Reintenta subir archivos de backup a GCS
   * @param {Object} backupFile - Archivo de backup a procesar
   * @param {Function} gcsUploadFunction - Función para subir a GCS
   * @returns {Object} Resultado del procesamiento
   */
  async processLocalBackupFile(backupFile, gcsUploadFunction) {
    const startTime = Date.now();
    
    try {
      if (!backupFile || !backupFile.filePath) {
        const error = 'Archivo de backup inválido';
        logger.error(`❌ Error en processLocalBackupFile: ${error}`, {
          backupFile: backupFile ? 'exists' : 'null',
          hasFilePath: backupFile?.filePath ? 'yes' : 'no'
        });
        throw new Error(error);
      }

      if (typeof gcsUploadFunction !== 'function') {
        const error = 'Función de upload a GCS requerida';
        logger.error(`❌ Error en processLocalBackupFile: ${error}`, {
          functionType: typeof gcsUploadFunction
        });
        throw new Error(error);
      }

      const backupId = backupFile.id;
      const currentRetry = backupFile.metadata.retryCount + 1;
      const backupAge = Date.now() - new Date(backupFile.timestamp).getTime();
      
      logger.info(`🔄 Iniciando procesamiento de backup local: ${backupId}`, {
        type: backupFile.type,
        attempt: currentRetry,
        maxRetries: backupFile.metadata.maxRetries,
        recordCount: backupFile.data?.length || 0,
        backupAge: `${Math.round(backupAge / 1000)}s`,
        filePath: backupFile.filePath,
        previousErrors: backupFile.metadata.errors?.length || 0
      });

      // Actualizar metadata antes del intento
      logger.debug(`📝 Actualizando metadata para backup ${backupId} antes del intento ${currentRetry}`);
      
      const updatedBackup = await this.updateBackupMetadata(backupFile.filePath, {
        status: 'processing',
        metadata: {
          retryCount: currentRetry,
          lastAttempt: new Date().toISOString()
        }
      });

      // Registrar métricas de retry
      await this.metrics.recordBackupOperation('retry', {
        id: backupId,
        type: backupFile.type,
        retryCount: currentRetry,
        backupAge: backupAge
      });

      try {
        logger.debug(`☁️ Iniciando upload a GCS para backup ${backupId}`, {
          dataSize: JSON.stringify(backupFile.data).length,
          recordCount: backupFile.data.length
        });

        // Intentar subir a GCS
        const uploadStartTime = Date.now();
        const uploadResult = await gcsUploadFunction(backupFile.data, backupFile.type);
        const uploadDuration = Date.now() - uploadStartTime;
        
        if (uploadResult.success) {
          // Marcar como completado
          const completedAt = new Date().toISOString();
          await this.updateBackupMetadata(backupFile.filePath, {
            status: 'completed',
            processedAt: completedAt,
            gcsFile: uploadResult.gcsFile || uploadResult.fileName,
            result: uploadResult,
            metadata: {
              uploadDuration: uploadDuration,
              totalProcessingTime: Date.now() - startTime
            }
          });

          const totalDuration = Date.now() - startTime;
          logger.info(`✅ Backup procesado exitosamente: ${backupId}`, {
            type: backupFile.type,
            recordCount: backupFile.data.length,
            gcsFile: uploadResult.gcsFile || uploadResult.fileName,
            attempt: currentRetry,
            uploadDuration: `${uploadDuration}ms`,
            totalDuration: `${totalDuration}ms`,
            backupAge: `${Math.round(backupAge / 1000)}s`,
            retryTime: updatedBackup.metadata.lastAttempt ? 
              new Date() - new Date(updatedBackup.metadata.lastAttempt) : 0
          });
          
          // Registrar métricas de backup completado
          await this.metrics.recordBackupOperation('completed', {
            id: backupId,
            type: backupFile.type,
            recordCount: backupFile.data.length,
            retryTime: updatedBackup.metadata.lastAttempt ? 
              new Date() - new Date(updatedBackup.metadata.lastAttempt) : 0,
            uploadDuration: uploadDuration,
            totalDuration: totalDuration
          });
          
          return {
            success: true,
            backupId,
            recordsProcessed: backupFile.data.length,
            gcsFile: uploadResult.gcsFile || uploadResult.fileName,
            type: backupFile.type,
            attempt: currentRetry,
            uploadDuration: uploadDuration,
            totalDuration: totalDuration
          };
        } else {
          throw new Error(uploadResult.error || 'Error desconocido en upload a GCS');
        }

      } catch (uploadError) {
        const uploadDuration = Date.now() - startTime;
        
        // Manejar falla en el upload
        const errorInfo = {
          message: uploadError.message,
          timestamp: new Date().toISOString(),
          attempt: currentRetry,
          uploadDuration: uploadDuration,
          errorType: uploadError.name || 'Unknown',
          stack: uploadError.stack
        };

        const errors = [...(backupFile.metadata.errors || []), errorInfo];
        const hasExceededRetries = currentRetry >= updatedBackup.metadata.maxRetries;

        await this.updateBackupMetadata(backupFile.filePath, {
          status: hasExceededRetries ? 'failed' : 'pending',
          lastError: errorInfo,
          metadata: {
            errors: errors,
            lastFailureDuration: uploadDuration
          }
        });

        if (hasExceededRetries) {
          logger.error(`❌ Backup falló definitivamente: ${backupId}`, {
            type: backupFile.type,
            finalAttempt: currentRetry,
            maxRetries: updatedBackup.metadata.maxRetries,
            error: uploadError.message,
            errorType: uploadError.name,
            totalErrors: errors.length,
            backupAge: `${Math.round(backupAge / 1000)}s`,
            uploadDuration: `${uploadDuration}ms`,
            allErrors: errors.map(e => ({ attempt: e.attempt, message: e.message, timestamp: e.timestamp }))
          });
        } else {
          const nextRetryIn = this.retryDelay * Math.pow(2, currentRetry - 1); // Exponential backoff
          logger.warn(`⚠️ Backup falló, se reintentará: ${backupId}`, {
            type: backupFile.type,
            attempt: currentRetry,
            maxRetries: updatedBackup.metadata.maxRetries,
            error: uploadError.message,
            errorType: uploadError.name,
            nextRetryIn: `${nextRetryIn}ms`,
            backupAge: `${Math.round(backupAge / 1000)}s`,
            uploadDuration: `${uploadDuration}ms`
          });
        }

        // Registrar métricas de backup fallido
        await this.metrics.recordBackupOperation('failed', {
          id: backupId,
          type: backupFile.type,
          retryCount: currentRetry,
          maxRetries: updatedBackup.metadata.maxRetries,
          uploadDuration: uploadDuration,
          errorType: uploadError.name || 'Unknown'
        }, false, uploadError.message);

        return {
          success: false,
          backupId,
          error: uploadError.message,
          errorType: uploadError.name || 'Unknown',
          retryCount: currentRetry,
          maxRetries: updatedBackup.metadata.maxRetries,
          willRetry: !hasExceededRetries,
          uploadDuration: uploadDuration,
          backupAge: backupAge
        };
      }

    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error(`❌ Error crítico procesando backup local: ${backupFile?.id}`, {
        error: error.message,
        stack: error.stack,
        duration: `${duration}ms`,
        backupId: backupFile?.id,
        backupType: backupFile?.type,
        filePath: backupFile?.filePath
      });
      
      return {
        success: false,
        error: error.message,
        backupId: backupFile?.id,
        duration: duration
      };
    }
  }

  /**
   * Elimina archivos de backup procesados exitosamente
   * @param {string} backupId - ID del backup a eliminar
   * @returns {Object} Resultado de la operación
   */
  async deleteLocalBackup(backupId) {
    try {
      if (!backupId) {
        throw new Error('ID de backup requerido');
      }

      // Buscar el archivo de backup
      const backupFile = await this.findBackupFile(backupId);
      if (!backupFile) {
        throw new Error(`Backup ${backupId} no encontrado`);
      }

      // Verificar que el backup esté completado antes de eliminar
      if (backupFile.status !== 'completed') {
        throw new Error(`No se puede eliminar backup ${backupId} con status: ${backupFile.status}`);
      }

      // Eliminar archivo
      await fs.unlink(backupFile.filePath);
      
      logger.info(`🗑️ Backup local eliminado: ${backupId}`);
      
      return {
        success: true,
        backupId,
        message: 'Backup eliminado exitosamente'
      };

    } catch (error) {
      logger.error(`❌ Error eliminando backup local ${backupId}:`, error.message);
      return {
        success: false,
        error: error.message,
        backupId
      };
    }
  }

  /**
   * Actualiza metadata de un archivo de backup
   * @param {string} filePath - Ruta del archivo de backup
   * @param {Object} updates - Actualizaciones a aplicar
   * @returns {Object} Datos actualizados del backup
   */
  async updateBackupMetadata(filePath, updates) {
    try {
      const backupData = await FileUtils.readJsonFile(filePath);
      
      // Actualizar campos de nivel superior
      Object.keys(updates).forEach(key => {
        if (key === 'metadata') {
          // Merge metadata
          backupData.metadata = { ...backupData.metadata, ...updates.metadata };
        } else {
          backupData[key] = updates[key];
        }
      });
      
      await FileUtils.writeJsonFile(filePath, backupData);
      
      return backupData;

    } catch (error) {
      logger.error(`❌ Error actualizando metadata de backup:`, error.message);
      throw error;
    }
  }

  /**
   * Busca un archivo de backup por ID
   * @param {string} backupId - ID del backup a buscar
   * @returns {Object|null} Datos del backup o null si no se encuentra
   */
  async findBackupFile(backupId) {
    try {
      await FileUtils.ensureDirectoryExists(this.backupPath);
      
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        file.startsWith('backup_') && file.endsWith('.json')
      );

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          if (backupData.id === backupId) {
            return {
              ...backupData,
              filePath
            };
          }
        } catch (error) {
          logger.warn(`⚠️ Error leyendo backup ${file}:`, error.message);
        }
      }

      return null;

    } catch (error) {
      logger.error(`❌ Error buscando backup ${backupId}:`, error.message);
      return null;
    }
  }

  /**
   * Genera un ID corto para identificación única
   * @returns {string} ID corto
   */
  generateShortId() {
    return Math.random().toString(36).substring(2, 8);
  }

  /**
   * Obtiene todos los archivos de backup (pendientes, completados, fallidos)
   * @returns {Array} Lista de todos los archivos de backup
   */
  async getAllBackupFiles() {
    try {
      await FileUtils.ensureDirectoryExists(this.backupPath);
      
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        file.startsWith('backup_') && file.endsWith('.json')
      );

      const allBackups = [];

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          allBackups.push({
            ...backupData,
            filePath
          });
        } catch (error) {
          logger.warn(`⚠️ Error leyendo backup ${file}:`, error.message);
        }
      }

      // Ordenar por timestamp (más antiguos primero)
      allBackups.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));

      return allBackups;

    } catch (error) {
      logger.error('❌ Error obteniendo todos los archivos de backup:', error.message);
      return [];
    }
  }

  /**
   * Limpia backups completados antiguos
   */
  async cleanupCompletedBackups(maxAge = 24 * 60 * 60 * 1000) { // 24 horas por defecto
    const startTime = Date.now();
    
    try {
      logger.info(`🧹 Iniciando limpieza de backups completados antiguos`, {
        directory: this.backupPath,
        maxAge: `${Math.round(maxAge / (60 * 60 * 1000))}h`,
        retentionHours: this.retentionHours
      });

      const files = await fs.readdir(this.backupPath);
      const backupFiles = files.filter(file => 
        (file.startsWith('gps_backup_') || file.startsWith('backup_')) && file.endsWith('.json')
      );

      logger.debug(`📁 Archivos encontrados para evaluación de limpieza: ${backupFiles.length}`);

      let cleanedCount = 0;
      let skippedCount = 0;
      let errorCount = 0;
      const now = Date.now();
      const configuredMaxAge = this.retentionHours * 60 * 60 * 1000;
      const actualMaxAge = Math.min(maxAge, configuredMaxAge);
      
      const cleanupStats = {
        completed: 0,
        pending: 0,
        failed: 0,
        processing: 0,
        tooRecent: 0,
        noProcessedDate: 0
      };

      logger.debug(`⏰ Configuración de limpieza:`, {
        configuredMaxAge: `${Math.round(configuredMaxAge / (60 * 60 * 1000))}h`,
        providedMaxAge: `${Math.round(maxAge / (60 * 60 * 1000))}h`,
        actualMaxAge: `${Math.round(actualMaxAge / (60 * 60 * 1000))}h`
      });

      for (const file of backupFiles) {
        try {
          const filePath = path.join(this.backupPath, file);
          const backupData = await FileUtils.readJsonFile(filePath);
          
          cleanupStats[backupData.status] = (cleanupStats[backupData.status] || 0) + 1;
          
          // Solo limpiar backups completados antiguos
          if (backupData.status === 'completed' && backupData.processedAt) {
            const processedTime = new Date(backupData.processedAt).getTime();
            const age = now - processedTime;
            
            if (age > actualMaxAge) {
              await fs.unlink(filePath);
              cleanedCount++;
              
              logger.debug(`🗑️ Backup completado eliminado: ${backupData.id}`, {
                type: backupData.type,
                age: `${Math.round(age / (60 * 60 * 1000))}h`,
                recordCount: backupData.metadata?.recordCount || 0,
                processedAt: backupData.processedAt,
                filePath: filePath
              });
            } else {
              cleanupStats.tooRecent++;
              skippedCount++;
              
              logger.debug(`⏭️ Backup completado demasiado reciente, omitido: ${backupData.id}`, {
                age: `${Math.round(age / (60 * 60 * 1000))}h`,
                maxAge: `${Math.round(actualMaxAge / (60 * 60 * 1000))}h`
              });
            }
          } else if (backupData.status === 'completed' && !backupData.processedAt) {
            cleanupStats.noProcessedDate++;
            skippedCount++;
            
            logger.warn(`⚠️ Backup completado sin fecha de procesamiento, omitido: ${backupData.id}`, {
              status: backupData.status,
              hasProcessedAt: !!backupData.processedAt
            });
          } else {
            skippedCount++;
            
            logger.debug(`⏭️ Backup omitido (no completado): ${backupData.id}`, {
              status: backupData.status,
              reason: 'not_completed'
            });
          }
          
        } catch (error) {
          errorCount++;
          logger.warn(`⚠️ Error limpiando backup ${file}:`, {
            error: error.message,
            filePath: path.join(this.backupPath, file)
          });
        }
      }

      const duration = Date.now() - startTime;

      if (cleanedCount > 0) {
        logger.info(`✅ Limpieza de backups completada exitosamente`, {
          cleaned: cleanedCount,
          skipped: skippedCount,
          errors: errorCount,
          totalEvaluated: backupFiles.length,
          duration: `${duration}ms`,
          stats: cleanupStats
        });
      } else {
        logger.debug(`📋 Limpieza completada sin eliminaciones`, {
          totalEvaluated: backupFiles.length,
          skipped: skippedCount,
          errors: errorCount,
          duration: `${duration}ms`,
          stats: cleanupStats
        });
      }

      return {
        success: true,
        cleaned: cleanedCount,
        skipped: skippedCount,
        errors: errorCount,
        duration: duration,
        stats: cleanupStats
      };

    } catch (error) {
      const duration = Date.now() - startTime;
      logger.error('❌ Error crítico limpiando backups completados:', {
        error: error.message,
        stack: error.stack,
        directory: this.backupPath,
        duration: `${duration}ms`
      });
      
      return {
        success: false,
        error: error.message,
        cleaned: 0,
        duration: duration
      };
    }
  }
}