import { logger } from './logger.js';
import { FileUtils } from './FileUtils.js';
import { config } from '../config/env.js';

/**
 * Recolector de métricas para monitoreo del microservicio
 * Implementa patrón Singleton para evitar múltiples cargas de archivo
 */
export class MetricsCollector {
  static instance = null;
  static isLoaded = false;

  constructor() {
    // Implementar Singleton
    if (MetricsCollector.instance) {
      return MetricsCollector.instance;
    }
    this.metrics = {
      processing: {
        totalRuns: 0,
        successfulRuns: 0,
        failedRuns: 0,
        totalRecordsProcessed: 0,
        totalProcessingTime: 0,
        averageProcessingTime: 0,
        lastProcessing: null,
        lastError: null
      },
      redis: {
        connections: 0,
        disconnections: 0,
        errors: 0,
        lastConnection: null,
        lastError: null
      },
      gcs: {
        uploads: {
          gps: { total: 0, successful: 0, failed: 0, totalSize: 0, totalTime: 0, avgSize: 0, avgTime: 0 },
          mobile: { total: 0, successful: 0, failed: 0, totalSize: 0, totalTime: 0, avgSize: 0, avgTime: 0 }
        },
        storage: {
          totalFiles: 0,
          totalSize: 0,
          oldestFile: null,
          newestFile: null,
          filesByType: { gps: 0, mobile: 0, unknown: 0 }
        },
        lastUpload: null,
        lastError: null
      },
      bigquery: {
        uploads: 0,
        successfulUploads: 0,
        failedUploads: 0,
        totalRecordsUploaded: 0,
        lastUpload: null,
        lastError: null,
        batchJobs: {
          gps: { total: 0, successful: 0, failed: 0, totalRecords: 0, totalTime: 0, avgRecords: 0, avgTime: 0 },
          mobile: { total: 0, successful: 0, failed: 0, totalRecords: 0, totalTime: 0, avgRecords: 0, avgTime: 0 }
        },
        lastBatchJob: null
      },
      costs: {
        estimatedGCSCost: 0,
        estimatedBigQueryCost: 0,
        totalEstimatedCost: 0,
        lastCostUpdate: null
      },
      validation: {
        totalRecordsValidated: 0,
        validRecords: 0,
        invalidRecords: 0,
        validationRate: 0
      },
      system: {
        startTime: new Date().toISOString(),
        uptime: 0,
        memoryUsage: null,
        lastHealthCheck: null
      },
      backup: {
        local: {
          total: 0,
          pending: 0,
          processing: 0,
          completed: 0,
          failed: 0,
          totalRecords: 0,
          totalRetries: 0,
          avgRetryTime: 0,
          maxRetryTimeExceeded: 0,
          lastBackupCreated: null,
          lastBackupProcessed: null,
          lastError: null,
          byType: {
            gps: { total: 0, pending: 0, completed: 0, failed: 0, totalRecords: 0 },
            mobile: { total: 0, pending: 0, completed: 0, failed: 0, totalRecords: 0 }
          }
        },
        retryTimes: [], // Array para calcular tiempo promedio de retry
        alerts: {
          maxRetriesExceeded: [],
          oldPendingBackups: []
        }
      }
    };

    this.metricsFile = 'tmp/metrics.json';
    
    // Solo cargar métricas una vez
    if (!MetricsCollector.isLoaded) {
      this.loadMetrics();
      MetricsCollector.isLoaded = true;
    }
    
    // Guardar instancia singleton
    MetricsCollector.instance = this;
  }

  /**
   * Método estático para obtener instancia singleton
   */
  static getInstance() {
    if (!MetricsCollector.instance) {
      new MetricsCollector();
    }
    return MetricsCollector.instance;
  }

  /**
   * Registra métricas de procesamiento
   */
  async recordProcessing(data) {
    try {
      const { recordsProcessed, processingTime, success, error } = data;

      this.metrics.processing.totalRuns++;
      this.metrics.processing.totalRecordsProcessed += recordsProcessed || 0;
      this.metrics.processing.totalProcessingTime += processingTime || 0;
      this.metrics.processing.lastProcessing = new Date().toISOString();

      if (success) {
        this.metrics.processing.successfulRuns++;
      } else {
        this.metrics.processing.failedRuns++;
        this.metrics.processing.lastError = {
          message: error,
          timestamp: new Date().toISOString()
        };
      }

      // Calcular tiempo promedio
      if (this.metrics.processing.totalRuns > 0) {
        this.metrics.processing.averageProcessingTime = 
          this.metrics.processing.totalProcessingTime / this.metrics.processing.totalRuns;
      }

      await this.saveMetrics();
      logger.debug('📊 Métricas de procesamiento registradas');

    } catch (error) {
      logger.error('❌ Error registrando métricas de procesamiento:', error.message);
    }
  }

  /**
   * Registra métricas de Redis
   */
  async recordRedisOperation(operation, success, error = null) {
    try {
      switch (operation) {
        case 'connect':
          this.metrics.redis.connections++;
          if (success) {
            this.metrics.redis.lastConnection = new Date().toISOString();
          }
          break;
        case 'disconnect':
          this.metrics.redis.disconnections++;
          break;
      }

      if (!success) {
        this.metrics.redis.errors++;
        this.metrics.redis.lastError = {
          operation,
          message: error,
          timestamp: new Date().toISOString()
        };
      }

      await this.saveMetrics();

    } catch (err) {
      logger.error('❌ Error registrando métricas de Redis:', err.message);
    }
  }

  /**
   * Registra métricas de GCS
   */
  async recordGCSOperation(dataType, fileSize, uploadTime, success, error = null) {
    try {
      const typeMetrics = this.metrics.gcs.uploads[dataType];
      if (!typeMetrics) {
        logger.warn(`⚠️ Tipo de datos GCS no reconocido: ${dataType}`);
        return;
      }

      typeMetrics.total++;
      this.metrics.gcs.lastUpload = new Date().toISOString();

      if (success) {
        typeMetrics.successful++;
        typeMetrics.totalSize += fileSize || 0;
        typeMetrics.totalTime += uploadTime || 0;

        // Calcular promedios
        if (typeMetrics.successful > 0) {
          typeMetrics.avgSize = Math.round(typeMetrics.totalSize / typeMetrics.successful);
          typeMetrics.avgTime = Math.round(typeMetrics.totalTime / typeMetrics.successful);
        }
      } else {
        typeMetrics.failed++;
        this.metrics.gcs.lastError = {
          dataType,
          message: error,
          timestamp: new Date().toISOString()
        };
      }

      await this.saveMetrics();
      logger.debug(`📊 Métricas GCS registradas para ${dataType}`);

    } catch (err) {
      logger.error('❌ Error registrando métricas de GCS:', err.message);
    }
  }

  /**
   * Actualiza métricas de almacenamiento GCS
   */
  async updateGCSStorageMetrics(storageStats) {
    try {
      const { totalFiles, totalSize, filesByType, oldestFile, newestFile } = storageStats;

      this.metrics.gcs.storage.totalFiles = totalFiles || 0;
      this.metrics.gcs.storage.totalSize = totalSize || 0;
      this.metrics.gcs.storage.oldestFile = oldestFile;
      this.metrics.gcs.storage.newestFile = newestFile;
      this.metrics.gcs.storage.filesByType = {
        gps: filesByType?.gps || 0,
        mobile: filesByType?.mobile || 0,
        unknown: filesByType?.unknown || 0
      };

      await this.saveMetrics();
      logger.debug('📊 Métricas de almacenamiento GCS actualizadas');

    } catch (error) {
      logger.error('❌ Error actualizando métricas de almacenamiento GCS:', error.message);
    }
  }

  /**
   * Registra métricas de BigQuery
   */
  async recordBigQueryOperation(recordsUploaded, success, error = null) {
    try {
      this.metrics.bigquery.uploads++;
      this.metrics.bigquery.lastUpload = new Date().toISOString();

      if (success) {
        this.metrics.bigquery.successfulUploads++;
        this.metrics.bigquery.totalRecordsUploaded += recordsUploaded || 0;
      } else {
        this.metrics.bigquery.failedUploads++;
        this.metrics.bigquery.lastError = {
          message: error,
          timestamp: new Date().toISOString()
        };
      }

      await this.saveMetrics();

    } catch (err) {
      logger.error('❌ Error registrando métricas de BigQuery:', err.message);
    }
  }

  /**
   * Registra métricas de batch job BigQuery
   */
  async recordBigQueryBatchJob(dataType, recordsProcessed, processingTime, success, jobId, error = null) {
    try {
      const typeMetrics = this.metrics.bigquery.batchJobs[dataType];
      if (!typeMetrics) {
        logger.warn(`⚠️ Tipo de datos BigQuery no reconocido: ${dataType}`);
        return;
      }

      typeMetrics.total++;
      this.metrics.bigquery.lastBatchJob = {
        jobId,
        dataType,
        timestamp: new Date().toISOString(),
        success
      };

      if (success) {
        typeMetrics.successful++;
        typeMetrics.totalRecords += recordsProcessed || 0;
        typeMetrics.totalTime += processingTime || 0;

        // Calcular promedios
        if (typeMetrics.successful > 0) {
          typeMetrics.avgRecords = Math.round(typeMetrics.totalRecords / typeMetrics.successful);
          typeMetrics.avgTime = Math.round(typeMetrics.totalTime / typeMetrics.successful);
        }
      } else {
        typeMetrics.failed++;
        this.metrics.bigquery.lastError = {
          jobId,
          dataType,
          message: error,
          timestamp: new Date().toISOString()
        };
      }

      await this.saveMetrics();
      logger.debug(`📊 Métricas BigQuery batch registradas para ${dataType}`);

    } catch (err) {
      logger.error('❌ Error registrando métricas de BigQuery batch:', err.message);
    }
  }

  /**
   * Actualiza estimaciones de costos GCP
   */
  async updateGCPCosts(gcsStorageGB, bigQueryProcessedTB) {
    try {
      // Precios estimados (USD por mes/operación)
      const GCS_STORAGE_COST_PER_GB = 0.020; // $0.020 por GB/mes
      const BIGQUERY_QUERY_COST_PER_TB = 5.00; // $5.00 por TB procesado

      // Calcular costos estimados
      const estimatedGCSCost = (gcsStorageGB || 0) * GCS_STORAGE_COST_PER_GB;
      const estimatedBigQueryCost = (bigQueryProcessedTB || 0) * BIGQUERY_QUERY_COST_PER_TB;

      this.metrics.costs.estimatedGCSCost = Math.round(estimatedGCSCost * 100) / 100;
      this.metrics.costs.estimatedBigQueryCost = Math.round(estimatedBigQueryCost * 100) / 100;
      this.metrics.costs.totalEstimatedCost = Math.round((estimatedGCSCost + estimatedBigQueryCost) * 100) / 100;
      this.metrics.costs.lastCostUpdate = new Date().toISOString();

      await this.saveMetrics();
      logger.debug('💰 Estimaciones de costos GCP actualizadas');

    } catch (error) {
      logger.error('❌ Error actualizando costos GCP:', error.message);
    }
  }

  /**
   * Registra métricas de validación
   */
  async recordValidation(totalRecords, validRecords, invalidRecords) {
    try {
      this.metrics.validation.totalRecordsValidated += totalRecords;
      this.metrics.validation.validRecords += validRecords;
      this.metrics.validation.invalidRecords += invalidRecords;

      // Calcular tasa de validación
      if (this.metrics.validation.totalRecordsValidated > 0) {
        this.metrics.validation.validationRate = 
          (this.metrics.validation.validRecords / this.metrics.validation.totalRecordsValidated * 100);
      }

      await this.saveMetrics();

    } catch (error) {
      logger.error('❌ Error registrando métricas de validación:', error.message);
    }
  }

  /**
   * Actualiza métricas del sistema
   */
  async updateSystemMetrics() {
    try {
      const startTime = new Date(this.metrics.system.startTime);
      this.metrics.system.uptime = Date.now() - startTime.getTime();
      
      // Obtener uso de memoria
      const memUsage = process.memoryUsage();
      this.metrics.system.memoryUsage = {
        rss: Math.round(memUsage.rss / 1024 / 1024 * 100) / 100, // MB
        heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024 * 100) / 100,
        heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024 * 100) / 100,
        external: Math.round(memUsage.external / 1024 / 1024 * 100) / 100,
        timestamp: new Date().toISOString()
      };

      await this.saveMetrics();

    } catch (error) {
      logger.error('❌ Error actualizando métricas del sistema:', error.message);
    }
  }

  /**
   * Registra health check
   */
  async recordHealthCheck(isHealthy, services) {
    try {
      this.metrics.system.lastHealthCheck = {
        healthy: isHealthy,
        services,
        timestamp: new Date().toISOString()
      };

      await this.saveMetrics();

    } catch (error) {
      logger.error('❌ Error registrando health check:', error.message);
    }
  }

  /**
   * Obtiene métricas específicas de GCS
   */
  async getGCSMetrics() {
    return {
      uploads: this.metrics.gcs.uploads,
      storage: this.metrics.gcs.storage,
      lastUpload: this.metrics.gcs.lastUpload,
      lastError: this.metrics.gcs.lastError,
      summary: {
        totalUploads: this.metrics.gcs.uploads.gps.total + this.metrics.gcs.uploads.mobile.total,
        successfulUploads: this.metrics.gcs.uploads.gps.successful + this.metrics.gcs.uploads.mobile.successful,
        failedUploads: this.metrics.gcs.uploads.gps.failed + this.metrics.gcs.uploads.mobile.failed,
        successRate: this.calculateSuccessRate(
          this.metrics.gcs.uploads.gps.successful + this.metrics.gcs.uploads.mobile.successful,
          this.metrics.gcs.uploads.gps.total + this.metrics.gcs.uploads.mobile.total
        ),
        totalStorageSize: this.formatBytes(this.metrics.gcs.storage.totalSize),
        avgUploadSize: this.formatBytes(
          (this.metrics.gcs.uploads.gps.avgSize + this.metrics.gcs.uploads.mobile.avgSize) / 2
        )
      }
    };
  }

  /**
   * Obtiene métricas específicas de BigQuery
   */
  async getBigQueryMetrics() {
    return {
      batchJobs: this.metrics.bigquery.batchJobs,
      legacy: {
        uploads: this.metrics.bigquery.uploads,
        successfulUploads: this.metrics.bigquery.successfulUploads,
        failedUploads: this.metrics.bigquery.failedUploads,
        totalRecordsUploaded: this.metrics.bigquery.totalRecordsUploaded
      },
      lastBatchJob: this.metrics.bigquery.lastBatchJob,
      lastError: this.metrics.bigquery.lastError,
      summary: {
        totalBatchJobs: this.metrics.bigquery.batchJobs.gps.total + this.metrics.bigquery.batchJobs.mobile.total,
        successfulBatchJobs: this.metrics.bigquery.batchJobs.gps.successful + this.metrics.bigquery.batchJobs.mobile.successful,
        failedBatchJobs: this.metrics.bigquery.batchJobs.gps.failed + this.metrics.bigquery.batchJobs.mobile.failed,
        successRate: this.calculateSuccessRate(
          this.metrics.bigquery.batchJobs.gps.successful + this.metrics.bigquery.batchJobs.mobile.successful,
          this.metrics.bigquery.batchJobs.gps.total + this.metrics.bigquery.batchJobs.mobile.total
        ),
        totalRecordsProcessed: this.metrics.bigquery.batchJobs.gps.totalRecords + this.metrics.bigquery.batchJobs.mobile.totalRecords,
        avgProcessingTime: Math.round(
          (this.metrics.bigquery.batchJobs.gps.avgTime + this.metrics.bigquery.batchJobs.mobile.avgTime) / 2
        )
      }
    };
  }

  /**
   * Obtiene métricas de costos GCP
   */
  async getCostMetrics() {
    return {
      ...this.metrics.costs,
      breakdown: {
        gcsStorage: `$${this.metrics.costs.estimatedGCSCost}`,
        bigQueryProcessing: `$${this.metrics.costs.estimatedBigQueryCost}`,
        total: `$${this.metrics.costs.totalEstimatedCost}`
      }
    };
  }

  /**
   * Obtiene todas las métricas
   */
  async getMetrics() {
    await this.updateSystemMetrics();
    return {
      ...this.metrics,
      summary: this.generateSummary()
    };
  }

  /**
   * Genera resumen de métricas
   */
  generateSummary() {
    const processing = this.metrics.processing;
    const bigquery = this.metrics.bigquery;
    const gcs = this.metrics.gcs;
    const validation = this.metrics.validation;
    const costs = this.metrics.costs;

    // Calcular totales GCS
    const totalGCSUploads = gcs.uploads.gps.total + gcs.uploads.mobile.total;
    const successfulGCSUploads = gcs.uploads.gps.successful + gcs.uploads.mobile.successful;

    // Calcular totales BigQuery batch
    const totalBQBatch = bigquery.batchJobs.gps.total + bigquery.batchJobs.mobile.total;
    const successfulBQBatch = bigquery.batchJobs.gps.successful + bigquery.batchJobs.mobile.successful;

    return {
      successRate: processing.totalRuns > 0 ? 
        (processing.successfulRuns / processing.totalRuns * 100).toFixed(2) : 0,
      
      gcsSuccessRate: this.calculateSuccessRate(successfulGCSUploads, totalGCSUploads),
      
      bigQueryBatchSuccessRate: this.calculateSuccessRate(successfulBQBatch, totalBQBatch),
      
      bigQueryLegacySuccessRate: bigquery.uploads > 0 ? 
        (bigquery.successfulUploads / bigquery.uploads * 100).toFixed(2) : 0,
      
      validationRate: validation.validationRate.toFixed(2),
      
      averageProcessingTime: Math.round(processing.averageProcessingTime),
      
      totalRecordsProcessed: processing.totalRecordsProcessed,
      
      gcsStorageSize: this.formatBytes(gcs.storage.totalSize),
      
      estimatedMonthlyCost: `$${costs.totalEstimatedCost}`,
      
      uptime: this.formatUptime(this.metrics.system.uptime),
      
      memoryUsage: this.metrics.system.memoryUsage ? 
        `${this.metrics.system.memoryUsage.heapUsed}MB` : 'N/A',
      
      backupStatus: {
        pendingBackups: this.metrics.backup.local.pending,
        totalBackups: this.metrics.backup.local.total,
        successRate: this.calculateBackupSuccessRate(),
        avgRetryTime: `${this.metrics.backup.local.avgRetryTime}ms`,
        alertsCount: this.metrics.backup.alerts.maxRetriesExceeded.length + 
                    this.metrics.backup.alerts.oldPendingBackups.length
      }
    };
  }

  /**
   * Calcula tasa de éxito
   */
  calculateSuccessRate(successful, total) {
    return total > 0 ? (successful / total * 100).toFixed(2) : '0.00';
  }

  /**
   * Formatea bytes a formato legible
   */
  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Formatea el tiempo de actividad
   */
  formatUptime(milliseconds) {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) {
      return `${days}d ${hours % 24}h ${minutes % 60}m`;
    } else if (hours > 0) {
      return `${hours}h ${minutes % 60}m`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  }

  /**
   * Guarda métricas en archivo
   */
  async saveMetrics() {
    try {
      await FileUtils.writeJsonFile(this.metricsFile, this.metrics);
    } catch (error) {
      logger.error('❌ Error guardando métricas:', error.message);
    }
  }

  /**
   * Carga métricas desde archivo
   */
  async loadMetrics() {
    try {
      // En desarrollo, opcionalmente no cargar métricas previas
      if (config.server.environment === 'development' && process.env.SKIP_METRICS_LOAD === 'true') {
        logger.debug('📊 Saltando carga de métricas (desarrollo)');
        return;
      }

      const exists = await FileUtils.pathExists(this.metricsFile);
      if (exists) {
        const savedMetrics = await FileUtils.readJsonFile(this.metricsFile);
        
        // Merge con métricas por defecto para mantener estructura
        this.metrics = {
          ...this.metrics,
          ...savedMetrics,
          system: {
            ...this.metrics.system,
            ...savedMetrics.system,
            startTime: savedMetrics.system?.startTime || this.metrics.system.startTime
          }
        };
        
        logger.debug('📊 Métricas cargadas desde archivo');
      }
    } catch (error) {
      logger.warn('⚠️ Error cargando métricas, usando valores por defecto:', error.message);
    }
  }

  /**
   * Resetea métricas
   */
  async resetMetrics() {
    try {
      const startTime = this.metrics.system.startTime;
      
      this.metrics = {
        processing: {
          totalRuns: 0,
          successfulRuns: 0,
          failedRuns: 0,
          totalRecordsProcessed: 0,
          totalProcessingTime: 0,
          averageProcessingTime: 0,
          lastProcessing: null,
          lastError: null
        },
        redis: {
          connections: 0,
          disconnections: 0,
          errors: 0,
          lastConnection: null,
          lastError: null
        },
        gcs: {
          uploads: {
            gps: { total: 0, successful: 0, failed: 0, totalSize: 0, totalTime: 0, avgSize: 0, avgTime: 0 },
            mobile: { total: 0, successful: 0, failed: 0, totalSize: 0, totalTime: 0, avgSize: 0, avgTime: 0 }
          },
          storage: {
            totalFiles: 0,
            totalSize: 0,
            oldestFile: null,
            newestFile: null,
            filesByType: { gps: 0, mobile: 0, unknown: 0 }
          },
          lastUpload: null,
          lastError: null
        },
        bigquery: {
          uploads: 0,
          successfulUploads: 0,
          failedUploads: 0,
          totalRecordsUploaded: 0,
          lastUpload: null,
          lastError: null,
          batchJobs: {
            gps: { total: 0, successful: 0, failed: 0, totalRecords: 0, totalTime: 0, avgRecords: 0, avgTime: 0 },
            mobile: { total: 0, successful: 0, failed: 0, totalRecords: 0, totalTime: 0, avgRecords: 0, avgTime: 0 }
          },
          lastBatchJob: null
        },
        costs: {
          estimatedGCSCost: 0,
          estimatedBigQueryCost: 0,
          totalEstimatedCost: 0,
          lastCostUpdate: null
        },
        validation: {
          totalRecordsValidated: 0,
          validRecords: 0,
          invalidRecords: 0,
          validationRate: 0
        },
        system: {
          startTime: startTime, // Mantener tiempo de inicio original
          uptime: 0,
          memoryUsage: null,
          lastHealthCheck: null
        },
        backup: {
          local: {
            total: 0,
            pending: 0,
            processing: 0,
            completed: 0,
            failed: 0,
            totalRecords: 0,
            totalRetries: 0,
            avgRetryTime: 0,
            maxRetryTimeExceeded: 0,
            lastBackupCreated: null,
            lastBackupProcessed: null,
            lastError: null,
            byType: {
              gps: { total: 0, pending: 0, completed: 0, failed: 0, totalRecords: 0 },
              mobile: { total: 0, pending: 0, completed: 0, failed: 0, totalRecords: 0 }
            }
          },
          retryTimes: [],
          alerts: {
            maxRetriesExceeded: [],
            oldPendingBackups: []
          }
        }
      };

      await this.saveMetrics();
      logger.info('🔄 Métricas reseteadas');

    } catch (error) {
      logger.error('❌ Error reseteando métricas:', error.message);
    }
  }

  /**
   * Registra métricas de backup local
   */
  async recordBackupOperation(operation, backupData, success = true, error = null) {
    try {
      const backupMetrics = this.metrics.backup.local;
      const timestamp = new Date().toISOString();

      switch (operation) {
        case 'created':
          backupMetrics.total++;
          backupMetrics.pending++;
          backupMetrics.totalRecords += backupData.recordCount || 0;
          backupMetrics.lastBackupCreated = timestamp;
          
          // Métricas por tipo
          if (backupData.type && backupMetrics.byType[backupData.type]) {
            backupMetrics.byType[backupData.type].total++;
            backupMetrics.byType[backupData.type].pending++;
            backupMetrics.byType[backupData.type].totalRecords += backupData.recordCount || 0;
          }
          break;

        case 'processing':
          if (backupMetrics.pending > 0) backupMetrics.pending--;
          backupMetrics.processing++;
          
          if (backupData.type && backupMetrics.byType[backupData.type]) {
            if (backupMetrics.byType[backupData.type].pending > 0) {
              backupMetrics.byType[backupData.type].pending--;
            }
          }
          break;

        case 'completed':
          if (backupMetrics.processing > 0) backupMetrics.processing--;
          backupMetrics.completed++;
          backupMetrics.lastBackupProcessed = timestamp;
          
          // Registrar tiempo de retry si aplica
          if (backupData.retryTime) {
            this.recordBackupRetryTime(backupData.retryTime);
          }
          
          if (backupData.type && backupMetrics.byType[backupData.type]) {
            backupMetrics.byType[backupData.type].completed++;
          }
          break;

        case 'failed':
          if (backupMetrics.processing > 0) backupMetrics.processing--;
          
          if (backupData.retryCount >= backupData.maxRetries) {
            backupMetrics.failed++;
            backupMetrics.maxRetryTimeExceeded++;
            
            // Registrar alerta de máximo de reintentos excedido
            this.recordBackupAlert('maxRetriesExceeded', {
              backupId: backupData.id,
              type: backupData.type,
              retryCount: backupData.retryCount,
              timestamp: timestamp,
              error: error
            });
            
            if (backupData.type && backupMetrics.byType[backupData.type]) {
              backupMetrics.byType[backupData.type].failed++;
            }
          } else {
            backupMetrics.pending++;
            
            if (backupData.type && backupMetrics.byType[backupData.type]) {
              backupMetrics.byType[backupData.type].pending++;
            }
          }
          
          backupMetrics.totalRetries++;
          backupMetrics.lastError = {
            backupId: backupData.id,
            message: error,
            timestamp: timestamp
          };
          break;

        case 'retry':
          backupMetrics.totalRetries++;
          
          if (backupData.retryTime) {
            this.recordBackupRetryTime(backupData.retryTime);
          }
          break;
      }

      await this.saveMetrics();
      logger.debug(`📊 Métricas de backup registradas: ${operation}`);

    } catch (err) {
      logger.error('❌ Error registrando métricas de backup:', err.message);
    }
  }

  /**
   * Registra tiempo de retry para calcular promedio
   */
  recordBackupRetryTime(retryTime) {
    try {
      this.metrics.backup.retryTimes.push({
        time: retryTime,
        timestamp: new Date().toISOString()
      });

      // Mantener solo los últimos 100 registros para el promedio
      if (this.metrics.backup.retryTimes.length > 100) {
        this.metrics.backup.retryTimes = this.metrics.backup.retryTimes.slice(-100);
      }

      // Calcular tiempo promedio de retry
      const totalTime = this.metrics.backup.retryTimes.reduce((sum, entry) => sum + entry.time, 0);
      this.metrics.backup.local.avgRetryTime = Math.round(totalTime / this.metrics.backup.retryTimes.length);

    } catch (error) {
      logger.error('❌ Error registrando tiempo de retry:', error.message);
    }
  }

  /**
   * Registra alertas de backup
   */
  recordBackupAlert(alertType, alertData) {
    try {
      const alerts = this.metrics.backup.alerts[alertType];
      if (alerts) {
        alerts.push(alertData);
        
        // Mantener solo las últimas 50 alertas
        if (alerts.length > 50) {
          this.metrics.backup.alerts[alertType] = alerts.slice(-50);
        }
      }

    } catch (error) {
      logger.error('❌ Error registrando alerta de backup:', error.message);
    }
  }

  /**
   * Actualiza métricas de backup basándose en el estado actual
   */
  async updateBackupMetrics(backupStats) {
    try {
      if (!backupStats) return;

      const backupMetrics = this.metrics.backup.local;
      
      // Actualizar contadores principales
      backupMetrics.total = backupStats.total || 0;
      backupMetrics.pending = backupStats.pending || 0;
      backupMetrics.processing = backupStats.processing || 0;
      backupMetrics.completed = backupStats.completed || 0;
      backupMetrics.failed = backupStats.failed || 0;
      backupMetrics.totalRecords = backupStats.totalRecords || 0;

      // Verificar backups pendientes antiguos para alertas
      if (backupStats.oldestPending) {
        const oldestPendingTime = new Date(backupStats.oldestPending);
        const hoursSinceOldest = (Date.now() - oldestPendingTime.getTime()) / (1000 * 60 * 60);
        
        // Alerta si hay backups pendientes de más de 2 horas
        if (hoursSinceOldest > 2) {
          this.recordBackupAlert('oldPendingBackups', {
            oldestPending: backupStats.oldestPending,
            hoursSinceOldest: Math.round(hoursSinceOldest),
            pendingCount: backupStats.pending,
            timestamp: new Date().toISOString()
          });
        }
      }

      await this.saveMetrics();
      logger.debug('📊 Métricas de backup actualizadas');

    } catch (error) {
      logger.error('❌ Error actualizando métricas de backup:', error.message);
    }
  }

  /**
   * Obtiene métricas específicas de backup
   */
  async getBackupMetrics() {
    return {
      local: this.metrics.backup.local,
      alerts: this.metrics.backup.alerts,
      summary: {
        totalBackups: this.metrics.backup.local.total,
        pendingBackups: this.metrics.backup.local.pending,
        successRate: this.calculateBackupSuccessRate(),
        avgRetryTime: this.metrics.backup.local.avgRetryTime,
        alertsCount: {
          maxRetriesExceeded: this.metrics.backup.alerts.maxRetriesExceeded.length,
          oldPendingBackups: this.metrics.backup.alerts.oldPendingBackups.length
        },
        byType: {
          gps: {
            ...this.metrics.backup.local.byType.gps,
            successRate: this.calculateTypeSuccessRate('gps')
          },
          mobile: {
            ...this.metrics.backup.local.byType.mobile,
            successRate: this.calculateTypeSuccessRate('mobile')
          }
        }
      }
    };
  }

  /**
   * Calcula tasa de éxito de backups
   */
  calculateBackupSuccessRate() {
    const total = this.metrics.backup.local.total;
    const completed = this.metrics.backup.local.completed;
    return total > 0 ? (completed / total * 100).toFixed(2) : '0.00';
  }

  /**
   * Calcula tasa de éxito por tipo de backup
   */
  calculateTypeSuccessRate(type) {
    const typeMetrics = this.metrics.backup.local.byType[type];
    if (!typeMetrics) return '0.00';
    
    const total = typeMetrics.total;
    const completed = typeMetrics.completed;
    return total > 0 ? (completed / total * 100).toFixed(2) : '0.00';
  }

  /**
   * Obtiene alertas recientes de backup
   */
  getRecentBackupAlerts(hours = 24) {
    try {
      const cutoffTime = new Date(Date.now() - (hours * 60 * 60 * 1000));
      const recentAlerts = [];

      Object.keys(this.metrics.backup.alerts).forEach(alertType => {
        const alerts = this.metrics.backup.alerts[alertType];
        const recent = alerts.filter(alert => 
          new Date(alert.timestamp) > cutoffTime
        );
        
        recent.forEach(alert => {
          recentAlerts.push({
            type: alertType,
            ...alert
          });
        });
      });

      // Ordenar por timestamp (más recientes primero)
      recentAlerts.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

      return recentAlerts;

    } catch (error) {
      logger.error('❌ Error obteniendo alertas recientes de backup:', error.message);
      return [];
    }
  }

  /**
   * Limpia recursos
   */
  async flush() {
    try {
      await this.saveMetrics();
      logger.debug('💾 Métricas guardadas en flush');
    } catch (error) {
      logger.error('❌ Error en flush de métricas:', error.message);
    }
  }
}