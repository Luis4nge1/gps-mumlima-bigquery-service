import { logger } from '../utils/logger.js';
import { GPSProcessorService } from '../services/GPSProcessorService.js';
import { metrics } from '../utils/metrics.js';
import { BackupManager } from '../utils/BackupManager.js';
import { GCSAdapter } from '../adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../services/BigQueryBatchProcessor.js';
import { GCSRecoveryManager } from '../services/GCSRecoveryManager.js';

/**
 * Controlador para endpoints de salud y estado
 */
export class HealthController {
  constructor(processor = null) {
    // Usar procesador pasado como parámetro o crear uno nuevo
    this.processor = processor || new GPSProcessorService();
    this.metrics = metrics;
    this.backupManager = new BackupManager();
    this.gcsAdapter = new GCSAdapter();
    this.bigQueryProcessor = new BigQueryBatchProcessor();
    this.gcsRecoveryManager = new GCSRecoveryManager();
  }

  /**
   * Health check básico
   */
  async basicHealth() {
    try {
      const health = await this.processor.healthCheck();
      
      return {
        status: health.healthy ? 'healthy' : 'unhealthy',
        timestamp: new Date().toISOString(),
        services: health.services,
        uptime: process.uptime(),
        version: process.env.npm_package_version || '1.0.0'
      };

    } catch (error) {
      logger.error('❌ Error en health check básico:', error.message);
      return {
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: error.message
      };
    }
  }

  /**
   * Health check detallado
   */
  async detailedHealth() {
    try {
      const basicHealth = await this.basicHealth();
      const processorStats = await this.processor.getProcessorStats();
      const metrics = await this.metrics.getMetrics();
      const backupStats = await this.backupManager.getBackupStats();
      const backupMetrics = await this.metrics.getBackupMetrics();
      
      // Obtener información del procesador atómico
      const atomicProcessorStats = await this.processor.atomicProcessor.getStats();
      const atomicProcessorHealth = await this.processor.atomicProcessor.healthCheck();

      // Actualizar métricas de backup con estado actual
      await this.metrics.updateBackupMetrics(backupStats);

      return {
        ...basicHealth,
        detailed: true,
        stats: processorStats,
        metrics: metrics.summary,
        atomicProcessor: {
          enabled: atomicProcessorStats.atomicProcessingEnabled,
          mode: atomicProcessorStats.processingMode,
          healthy: atomicProcessorHealth.healthy,
          initialized: atomicProcessorStats.initialized,
          stats: atomicProcessorStats,
          warnings: atomicProcessorHealth.warnings || []
        },
        featureFlags: {
          atomicProcessingEnabled: atomicProcessorStats.atomicProcessingEnabled,
          configValue: atomicProcessorStats.featureFlags?.configValue,
          riskOfDataLoss: !atomicProcessorStats.atomicProcessingEnabled
        },
        backup: {
          stats: backupStats,
          metrics: backupMetrics.summary,
          healthy: backupStats.pending < 10 && backupStats.failed === 0,
          alerts: this.metrics.getRecentBackupAlerts(24).length
        },
        memory: {
          used: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
          total: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
          unit: 'MB'
        },
        environment: process.env.NODE_ENV || 'development'
      };

    } catch (error) {
      logger.error('❌ Error en health check detallado:', error.message);
      return {
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: error.message,
        detailed: true
      };
    }
  }

  /**
   * Obtiene métricas del sistema
   */
  async getMetrics() {
    try {
      const metrics = await this.metrics.getMetrics();
      return {
        success: true,
        timestamp: new Date().toISOString(),
        metrics
      };

    } catch (error) {
      logger.error('❌ Error obteniendo métricas:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message
      };
    }
  }

  /**
   * Obtiene estado del procesador
   */
  async getProcessorStatus() {
    try {
      const stats = await this.processor.getProcessorStats();
      return {
        success: true,
        timestamp: new Date().toISOString(),
        processor: stats
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado del procesador:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message
      };
    }
  }

  /**
   * Ejecuta procesamiento manual
   */
  async triggerManualProcessing() {
    try {
      logger.info('🔧 Procesamiento manual solicitado');
      
      const result = await this.processor.processGPSData();
      
      return {
        success: result.success,
        timestamp: new Date().toISOString(),
        result,
        triggered: 'manual'
      };

    } catch (error) {
      logger.error('❌ Error en procesamiento manual:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message,
        triggered: 'manual'
      };
    }
  }

  /**
   * Ejecuta recovery manual de backups pendientes
   */
  async triggerRecovery() {
    try {
      logger.info('🔄 Recovery manual solicitado');
      
      const result = await this.backupManager.processAllPendingBackups(async (backupData) => {
        // Simular procesamiento de BigQuery
        try {
          // Aquí iría la lógica real de subida a BigQuery
          logger.info(`🔄 Procesando recovery de ${Array.isArray(backupData) ? backupData.length : 0} registros`);
          
          return {
            success: true,
            recordsProcessed: Array.isArray(backupData) ? backupData.length : 0
          };
        } catch (error) {
          return {
            success: false,
            error: error.message
          };
        }
      });
      
      return {
        success: result.success,
        timestamp: new Date().toISOString(),
        result,
        triggered: 'manual_recovery'
      };

    } catch (error) {
      logger.error('❌ Error en recovery manual:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message,
        triggered: 'manual_recovery'
      };
    }
  }

  /**
   * Obtiene estado del sistema de recovery
   */
  async getRecoveryStatus() {
    try {
      const backupStats = await this.backupManager.getBackupStats();
      const pendingBackups = await this.backupManager.getPendingBackups();
      
      return {
        success: true,
        timestamp: new Date().toISOString(),
        backupStats,
        pendingBackups: pendingBackups.map(backup => ({
          id: backup.id,
          timestamp: backup.timestamp,
          retryCount: backup.retryCount,
          maxRetries: backup.maxRetries,
          recordCount: backup.metadata?.recordCount || 0,
          lastRetryAt: backup.lastRetryAt || null,
          error: backup.error?.message || null
        }))
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado de recovery:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message
      };
    }
  }

  /**
   * Obtiene métricas específicas de backup local
   */
  async getBackupMetrics() {
    try {
      const backupMetrics = await this.metrics.getBackupMetrics();
      const backupStats = await this.backupManager.getBackupStats();
      const recentAlerts = this.metrics.getRecentBackupAlerts(24);
      
      // Actualizar métricas con estado actual
      await this.metrics.updateBackupMetrics(backupStats);
      
      return {
        success: true,
        timestamp: new Date().toISOString(),
        service: 'Local Backup System',
        metrics: backupMetrics,
        currentStats: backupStats,
        recentAlerts: recentAlerts,
        summary: {
          healthy: backupStats.pending < 10 && backupStats.failed === 0,
          pendingBackups: backupStats.pending,
          failedBackups: backupStats.failed,
          successRate: backupMetrics.summary.successRate,
          avgRetryTime: backupMetrics.summary.avgRetryTime,
          alertsLast24h: recentAlerts.length,
          oldestPending: backupStats.oldestPending,
          newestCompleted: backupStats.newestCompleted
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo métricas de backup:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        service: 'Local Backup System',
        error: error.message
      };
    }
  }

  /**
   * Health check específico para GCS
   */
  async getGCSHealth() {
    try {
      const gcsStatus = await this.gcsAdapter.getStatus();
      const gcsMetrics = await this.metrics.getGCSMetrics();
      
      let bucketStats = null;
      try {
        bucketStats = await this.gcsAdapter.getBucketStats();
      } catch (statsError) {
        logger.warn('⚠️ No se pudieron obtener estadísticas del bucket:', statsError.message);
      }

      const isHealthy = gcsStatus.initialized && (gcsStatus.simulationMode || gcsStatus.bucketExists !== false);

      return {
        success: true,
        healthy: isHealthy,
        timestamp: new Date().toISOString(),
        service: 'Google Cloud Storage',
        status: gcsStatus,
        metrics: gcsMetrics,
        bucketStats,
        details: {
          initialized: gcsStatus.initialized,
          simulationMode: gcsStatus.simulationMode,
          bucketName: gcsStatus.bucketName,
          credentialsValid: gcsStatus.credentialsExist !== false
        }
      };

    } catch (error) {
      logger.error('❌ Error en health check de GCS:', error.message);
      return {
        success: false,
        healthy: false,
        timestamp: new Date().toISOString(),
        service: 'Google Cloud Storage',
        error: error.message
      };
    }
  }

  /**
   * Health check específico para BigQuery
   */
  async getBigQueryHealth() {
    try {
      const bqStatus = await this.bigQueryProcessor.getStatus();
      const bqMetrics = await this.metrics.getBigQueryMetrics();
      
      let tableStats = null;
      try {
        tableStats = await this.bigQueryProcessor.getTableStats();
      } catch (statsError) {
        logger.warn('⚠️ No se pudieron obtener estadísticas de tablas:', statsError.message);
      }

      let recentJobs = null;
      try {
        recentJobs = await this.bigQueryProcessor.listRecentJobs({ maxResults: 10 });
      } catch (jobsError) {
        logger.warn('⚠️ No se pudieron obtener jobs recientes:', jobsError.message);
      }

      const isHealthy = bqStatus.initialized && (bqStatus.simulationMode || bqStatus.datasetExists !== false);

      return {
        success: true,
        healthy: isHealthy,
        timestamp: new Date().toISOString(),
        service: 'BigQuery Batch Processor',
        status: bqStatus,
        metrics: bqMetrics,
        tableStats,
        recentJobs: recentJobs?.slice(0, 5), // Solo los 5 más recientes
        details: {
          initialized: bqStatus.initialized,
          simulationMode: bqStatus.simulationMode,
          projectId: bqStatus.projectId,
          datasetId: bqStatus.datasetId,
          credentialsValid: bqStatus.credentialsExist !== false
        }
      };

    } catch (error) {
      logger.error('❌ Error en health check de BigQuery:', error.message);
      return {
        success: false,
        healthy: false,
        timestamp: new Date().toISOString(),
        service: 'BigQuery Batch Processor',
        error: error.message
      };
    }
  }

  /**
   * Estado del sistema de recovery GCS
   */
  async getGCSRecoveryStatus() {
    try {
      const recoveryStats = await this.gcsRecoveryManager.getRecoveryStats();
      const pendingFiles = await this.gcsRecoveryManager.getPendingGCSFiles();
      
      return {
        success: true,
        timestamp: new Date().toISOString(),
        service: 'GCS Recovery Manager',
        recoveryStats,
        pendingFiles: pendingFiles.map(file => ({
          fileName: file.fileName,
          dataType: file.dataType,
          retryCount: file.retryCount,
          maxRetries: file.maxRetries,
          lastRetryAt: file.lastRetryAt,
          error: file.error?.message || null
        }))
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado de recovery GCS:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        service: 'GCS Recovery Manager',
        error: error.message
      };
    }
  }

  /**
   * Estadísticas de archivos GCS
   */
  async getGCSFileStats() {
    try {
      const bucketStats = await this.gcsAdapter.getBucketStats();
      const fileList = await this.gcsAdapter.listFiles({ maxResults: 50 });
      
      // Agrupar archivos por tipo y fecha
      const filesByType = fileList.reduce((acc, file) => {
        const dataType = file.metadata?.dataType || 'unknown';
        if (!acc[dataType]) {
          acc[dataType] = [];
        }
        acc[dataType].push({
          name: file.name,
          size: file.size,
          created: file.created,
          recordCount: file.metadata?.recordCount || 0
        });
        return acc;
      }, {});

      return {
        success: true,
        timestamp: new Date().toISOString(),
        service: 'GCS File Statistics',
        bucketStats,
        filesByType,
        totalFiles: fileList.length,
        recentFiles: fileList
          .sort((a, b) => new Date(b.created) - new Date(a.created))
          .slice(0, 10)
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas de archivos GCS:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        service: 'GCS File Statistics',
        error: error.message
      };
    }
  }

  /**
   * Fuerza procesamiento batch manual
   */
  async triggerManualBatchProcessing() {
    try {
      logger.info('🔧 Procesamiento batch manual solicitado');
      
      // Listar archivos pendientes en GCS
      const pendingFiles = await this.gcsAdapter.listFiles({ maxResults: 10 });
      
      if (pendingFiles.length === 0) {
        return {
          success: true,
          timestamp: new Date().toISOString(),
          message: 'No hay archivos pendientes para procesar',
          filesProcessed: 0
        };
      }

      // Procesar archivos en BigQuery
      const filesToProcess = pendingFiles.map(file => ({
        gcsUri: file.gcsPath,
        dataType: file.metadata?.dataType || 'unknown',
        metadata: file.metadata
      }));

      const result = await this.bigQueryProcessor.processBatch(filesToProcess, {
        maxConcurrency: 2,
        continueOnError: true
      });
      
      return {
        success: result.success,
        timestamp: new Date().toISOString(),
        result,
        triggered: 'manual_batch_processing'
      };

    } catch (error) {
      logger.error('❌ Error en procesamiento batch manual:', error.message);
      return {
        success: false,
        timestamp: new Date().toISOString(),
        error: error.message,
        triggered: 'manual_batch_processing'
      };
    }
  }

  // ==================== MÉTODOS HÍBRIDOS PARA MIGRACIÓN ====================

  /**
   * Obtiene estado del sistema híbrido
   */
  async getHybridStatus() {
    try {
      const { migrationConfig } = await import('../config/migrationConfig.js');
      
      // Verificar si el procesador es híbrido
      const isHybrid = this.processor.constructor.name === 'HybridGPSProcessor';
      
      if (!isHybrid) {
        return {
          error: 'Processor is not in hybrid mode',
          currentProcessor: this.processor.constructor.name,
          timestamp: new Date().toISOString()
        };
      }

      const status = await this.processor.getProcessorStats();
      
      return {
        hybrid: true,
        timestamp: new Date().toISOString(),
        ...status
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado híbrido:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Obtiene métricas del sistema híbrido
   */
  async getHybridMetrics() {
    try {
      const { migrationMetrics } = await import('../utils/MigrationMetrics.js');
      
      const stats = migrationMetrics.getStats();
      
      return {
        timestamp: new Date().toISOString(),
        ...stats
      };

    } catch (error) {
      logger.error('❌ Error obteniendo métricas híbridas:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Obtiene historial de rollbacks
   */
  async getHybridRollbacks() {
    try {
      const { rollbackManager } = await import('../utils/RollbackManager.js');
      
      const history = rollbackManager.getRollbackHistory();
      const status = rollbackManager.getStatus();
      
      return {
        timestamp: new Date().toISOString(),
        status: status,
        history: history
      };

    } catch (error) {
      logger.error('❌ Error obteniendo rollbacks:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Obtiene comparaciones entre flujos
   */
  async getHybridComparisons() {
    try {
      const { migrationMetrics } = await import('../utils/MigrationMetrics.js');
      
      const comparison = migrationMetrics.compareFlows();
      const stats = migrationMetrics.getStats();
      
      return {
        timestamp: new Date().toISOString(),
        currentComparison: comparison,
        statistics: {
          legacy: stats.legacy,
          newFlow: stats.newFlow,
          comparison: stats.comparison
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo comparaciones:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Cambia la fase de migración
   */
  async changeMigrationPhase(newPhase) {
    try {
      const { migrationConfig } = await import('../config/migrationConfig.js');
      
      // Verificar si el procesador es híbrido
      const isHybrid = this.processor.constructor.name === 'HybridGPSProcessor';
      
      if (isHybrid) {
        const result = await this.processor.setMigrationPhase(newPhase);
        return {
          success: result.success,
          timestamp: new Date().toISOString(),
          newPhase: result.newPhase,
          error: result.error
        };
      } else {
        migrationConfig.setMigrationPhase(newPhase);
        
        return {
          success: true,
          timestamp: new Date().toISOString(),
          newPhase: newPhase,
          note: 'Restart service to apply changes fully'
        };
      }

    } catch (error) {
      logger.error('❌ Error cambiando fase de migración:', error.message);
      return {
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Obtiene la fase de migración actual
   */
  async getMigrationPhase() {
    try {
      const { migrationConfig } = await import('../config/migrationConfig.js');
      
      const status = migrationConfig.getStatus();
      
      return {
        timestamp: new Date().toISOString(),
        currentPhase: status.currentPhase,
        phaseDescription: status.phaseDescription,
        flowDecision: status.flowDecision,
        availablePhases: ['legacy', 'hybrid', 'migration', 'new']
      };

    } catch (error) {
      logger.error('❌ Error obteniendo fase de migración:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }
}