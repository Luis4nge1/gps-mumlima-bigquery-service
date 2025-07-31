import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { GPSProcessorService } from './GPSProcessorService.js';
import { FileCleanup } from '../utils/FileCleanup.js';
import { BackupManager } from '../utils/BackupManager.js';
import { DistributedLock } from '../utils/DistributedLock.js';

/**
 * Servicio de programación para ejecutar procesamiento automático
 */
export class SchedulerService {
  constructor() {
    this.processor = new GPSProcessorService();
    this.fileCleanup = new FileCleanup();
    this.backupManager = new BackupManager();
    this.intervalId = null;
    this.cleanupIntervalId = null;
    this.backupCleanupIntervalId = null;
    this.isRunning = false;
    this.intervalMinutes = config.scheduler.intervalMinutes;
    this.enabled = config.scheduler.enabled;
    this.maxConcurrentJobs = config.scheduler.maxConcurrentJobs;
    this.currentJobs = 0;
    this.executionCount = 0;
    this.backupCleanupIntervalMinutes = config.backup.cleanupIntervalMinutes;
    this.stats = {
      totalExecutions: 0,
      successfulExecutions: 0,
      failedExecutions: 0,
      lastExecution: null,
      lastError: null,
      startTime: null,
      backupsProcessed: 0,
      backupsFailed: 0,
      lastBackupProcessing: null
    };
  }

  /**
   * Inicia el scheduler
   */
  start() {
    if (!this.enabled) {
      logger.info('📅 Scheduler deshabilitado por configuración');
      return false;
    }

    if (this.isRunning) {
      logger.warn('⚠️ Scheduler ya está ejecutándose');
      return false;
    }

    try {
      const intervalMs = this.intervalMinutes * 60 * 1000;
      
      logger.info(`📅 Iniciando scheduler: cada ${this.intervalMinutes} minutos`);
      
      this.intervalId = setInterval(() => {
        this.executeScheduledJob();
      }, intervalMs);

      // Limpieza automática cada 30 minutos
      this.cleanupIntervalId = setInterval(() => {
        this.executeCleanup();
      }, 30 * 60 * 1000);

      // Limpieza de backups según configuración
      const backupCleanupMs = this.backupCleanupIntervalMinutes * 60 * 1000;
      this.backupCleanupIntervalId = setInterval(() => {
        this.executeBackupCleanup();
      }, backupCleanupMs);

      this.isRunning = true;
      this.stats.startTime = new Date().toISOString();

      logger.info(`✅ Scheduler iniciado exitosamente (limpieza de backups cada ${this.backupCleanupIntervalMinutes} minutos)`);
      return true;

    } catch (error) {
      logger.error('❌ Error iniciando scheduler:', error.message);
      return false;
    }
  }

  /**
   * Detiene el scheduler
   */
  stop() {
    if (!this.isRunning) {
      logger.info('📅 Scheduler no está ejecutándose');
      return false;
    }

    try {
      if (this.intervalId) {
        clearInterval(this.intervalId);
        this.intervalId = null;
      }

      if (this.cleanupIntervalId) {
        clearInterval(this.cleanupIntervalId);
        this.cleanupIntervalId = null;
      }

      if (this.backupCleanupIntervalId) {
        clearInterval(this.backupCleanupIntervalId);
        this.backupCleanupIntervalId = null;
      }

      this.isRunning = false;
      
      logger.info('🛑 Scheduler detenido exitosamente');
      return true;

    } catch (error) {
      logger.error('❌ Error deteniendo scheduler:', error.message);
      return false;
    }
  }

  /**
   * Reinicia el scheduler con nueva configuración
   */
  restart(newIntervalMinutes = null) {
    try {
      logger.info('🔄 Reiniciando scheduler...');
      
      this.stop();
      
      if (newIntervalMinutes && newIntervalMinutes > 0) {
        this.intervalMinutes = newIntervalMinutes;
        logger.info(`⚙️ Nuevo intervalo configurado: ${newIntervalMinutes} minutos`);
      }
      
      return this.start();

    } catch (error) {
      logger.error('❌ Error reiniciando scheduler:', error.message);
      return false;
    }
  }

  /**
   * Ejecuta limpieza automática de archivos
   */
  async executeCleanup() {
    try {
      logger.info('🧹 Ejecutando limpieza automática...');
      
      const result = await this.fileCleanup.cleanupAll();
      
      if (result.success) {
        logger.info('✅ Limpieza automática completada');
      } else {
        logger.warn('⚠️ Limpieza automática con errores:', result.error);
      }

      // También ejecutar limpieza de backups expirados
      await this.executeBackupCleanup();

    } catch (error) {
      logger.error('❌ Error en limpieza automática:', error.message);
    }
  }

  /**
   * Procesa archivos de backup locales pendientes
   * @returns {Object} Resultado del procesamiento de backups
   */
  async processLocalBackups() {
    try {
      // Obtener archivos de backup pendientes
      const backupFiles = await this.backupManager.getLocalBackupFiles();
      
      if (backupFiles.length === 0) {
        logger.debug('📋 No hay backups locales pendientes para procesar');
        return {
          success: true,
          processed: 0,
          failed: 0,
          alerts: []
        };
      }

      logger.info(`🔄 Procesando ${backupFiles.length} backups locales pendientes...`);

      let processedCount = 0;
      let failedCount = 0;
      const alerts = [];

      for (const backupFile of backupFiles) {
        try {
          // Función de upload que usa el procesador GPS existente
          const gcsUploadFunction = async (data, type) => {
            try {
              // Usar el método de upload existente del procesador
              return await this.processor.uploadDataToGCS(data, type, {
                source: 'backup_retry',
                backupId: backupFile.id,
                retryAttempt: backupFile.metadata.retryCount + 1
              });
            } catch (error) {
              return {
                success: false,
                error: error.message
              };
            }
          };

          // Procesar el archivo de backup
          const result = await this.backupManager.processLocalBackupFile(backupFile, gcsUploadFunction);
          
          if (result.success) {
            processedCount++;
            
            // Si fue exitoso, eliminar el backup
            await this.backupManager.deleteLocalBackup(result.backupId);
            
            logger.info(`✅ Backup ${result.backupId} procesado y eliminado exitosamente`);
          } else {
            failedCount++;
            
            // Verificar si excedió el máximo de reintentos para generar alerta
            if (!result.willRetry) {
              const alertMessage = `Backup ${result.backupId} falló definitivamente después de ${result.retryCount} intentos: ${result.error}`;
              alerts.push(alertMessage);
              logger.error(`🚨 ${alertMessage}`);
            } else {
              logger.warn(`⚠️ Backup ${result.backupId} falló, se reintentará (${result.retryCount}/${result.maxRetries}): ${result.error}`);
            }
          }

          // Pequeña pausa entre procesamientos para no sobrecargar el sistema
          if (backupFiles.length > 1) {
            await new Promise(resolve => setTimeout(resolve, 1000));
          }

        } catch (error) {
          failedCount++;
          const alertMessage = `Error procesando backup ${backupFile.id}: ${error.message}`;
          alerts.push(alertMessage);
          logger.error(`❌ ${alertMessage}`);
        }
      }

      const result = {
        success: failedCount === 0,
        processed: processedCount,
        failed: failedCount,
        total: backupFiles.length,
        alerts
      };

      if (processedCount > 0 || failedCount > 0) {
        logger.info(`📊 Procesamiento de backups completado: ${processedCount} exitosos, ${failedCount} fallidos`);
      }

      return result;

    } catch (error) {
      logger.error('❌ Error procesando backups locales:', error.message);
      return {
        success: false,
        processed: 0,
        failed: 0,
        alerts: [`Error general procesando backups: ${error.message}`]
      };
    }
  }

  /**
   * Ejecuta limpieza automática de archivos de backup expirados
   */
  async executeBackupCleanup() {
    try {
      logger.debug('🧹 Ejecutando limpieza de backups expirados...');
      
      const result = await this.backupManager.cleanupCompletedBackups();
      
      if (result.success && result.cleaned > 0) {
        logger.info(`✅ Limpieza de backups completada: ${result.cleaned} archivos eliminados`);
      } else if (!result.success) {
        logger.warn('⚠️ Error en limpieza de backups:', result.error);
      }

    } catch (error) {
      logger.error('❌ Error en limpieza de backups:', error.message);
    }
  }

  /**
   * Ejecuta un trabajo programado
   */
  async executeScheduledJob() {
    // Verificar límite de trabajos concurrentes
    if (this.currentJobs >= this.maxConcurrentJobs) {
      logger.warn(`⚠️ Límite de trabajos concurrentes alcanzado (${this.maxConcurrentJobs}), saltando ejecución`);
      return;
    }

    // ✅ CRÍTICO: Usar lock distribuido para evitar ejecuciones superpuestas
    let lock = null;
    let lockAcquired = false;

    try {
      // Asegurar que el procesador esté inicializado
      if (!this.processor.redisRepo?.redis) {
        await this.processor.initialize();
      }

      // Verificar que tenemos acceso al cliente Redis
      if (this.processor.redisRepo?.redis) {
        lock = new DistributedLock(
          this.processor.redisRepo.redis, 
          'gps:scheduler:lock', 
          this.intervalMinutes * 60 * 1000 + 30000 // TTL = intervalo + 30s
        );

        lockAcquired = await lock.acquire();
        if (!lockAcquired) {
          logger.debug('🔒 Otra instancia está procesando, saltando ejecución');
          return;
        }
      } else {
        logger.warn('⚠️ Cliente Redis no disponible, continuando sin lock distribuido');
      }
    } catch (error) {
      logger.warn('⚠️ Error con lock distribuido, continuando sin protección:', error.message);
    }

    this.currentJobs++;
    this.stats.totalExecutions++;
    this.executionCount++;

    const startTime = Date.now();

    // Ejecutar limpieza cada 10 ejecuciones
    if (this.executionCount % 10 === 0) {
      await this.executeCleanup();
    }
    
    try {
      logger.info('🔄 Ejecutando trabajo programado...');
      
      // 1. PRIMERO: Procesar backups locales pendientes antes de nuevos datos
      const backupResult = await this.processLocalBackups();
      
      // 2. SEGUNDO: Procesar nuevos datos de Redis
      const result = await this.processor.processGPSData();
      
      // Combinar resultados
      const combinedResult = {
        success: result.success,
        recordsProcessed: result.recordsProcessed,
        processingTime: result.processingTime,
        backupsProcessed: backupResult.processed,
        backupsFailed: backupResult.failed,
        backupAlerts: backupResult.alerts || []
      };
      
      if (result.success) {
        this.stats.successfulExecutions++;
        this.stats.lastExecution = {
          timestamp: new Date().toISOString(),
          success: true,
          recordsProcessed: combinedResult.recordsProcessed,
          processingTime: combinedResult.processingTime,
          backupsProcessed: combinedResult.backupsProcessed,
          backupsFailed: combinedResult.backupsFailed
        };

        logger.info(`✅ Trabajo programado completado: ${combinedResult.recordsProcessed} registros procesados, ${combinedResult.backupsProcessed} backups procesados`);
        
        // Mostrar alertas de backups si las hay
        if (combinedResult.backupAlerts.length > 0) {
          combinedResult.backupAlerts.forEach(alert => {
            logger.warn(`🚨 ALERTA BACKUP: ${alert}`);
          });
        }
      } else {
        this.stats.failedExecutions++;
        this.stats.lastError = {
          timestamp: new Date().toISOString(),
          error: result.error
        };

        logger.error('❌ Error en trabajo programado:', result.error);
      }

    } catch (error) {
      this.stats.failedExecutions++;
      this.stats.lastError = {
        timestamp: new Date().toISOString(),
        error: error.message
      };

      logger.error('❌ Excepción en trabajo programado:', error.message);

    } finally {
      this.currentJobs--;
      
      // Liberar lock distribuido
      if (lock && lockAcquired) {
        try {
          await lock.release();
        } catch (error) {
          logger.warn('⚠️ Error liberando lock:', error.message);
        }
      }
      
      const executionTime = Date.now() - startTime;
      logger.info(`⏱️ Trabajo programado finalizado en ${executionTime}ms`);
    }
  }

  /**
   * Ejecuta un trabajo manual (fuera del schedule)
   */
  async executeManualJob() {
    try {
      logger.info('🔧 Ejecutando trabajo manual...');
      
      const result = await this.processor.processGPSData();
      
      logger.info('✅ Trabajo manual completado');
      return result;

    } catch (error) {
      logger.error('❌ Error en trabajo manual:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene el estado del scheduler
   */
  async getStatus() {
    const uptime = this.stats.startTime ? 
      Date.now() - new Date(this.stats.startTime).getTime() : 0;

    // Obtener estadísticas de backups
    const backupStats = await this.backupManager.getBackupStats();

    return {
      enabled: this.enabled,
      running: this.isRunning,
      intervalMinutes: this.intervalMinutes,
      maxConcurrentJobs: this.maxConcurrentJobs,
      currentJobs: this.currentJobs,
      uptime: this.formatUptime(uptime),
      stats: {
        ...this.stats,
        successRate: this.stats.totalExecutions > 0 ? 
          (this.stats.successfulExecutions / this.stats.totalExecutions * 100).toFixed(2) : 0
      },
      nextExecution: this.getNextExecutionTime(),
      backups: {
        cleanupIntervalMinutes: this.backupCleanupIntervalMinutes,
        stats: backupStats || {
          total: 0,
          pending: 0,
          processing: 0,
          completed: 0,
          failed: 0,
          totalRecords: 0
        }
      }
    };
  }

  /**
   * Calcula el tiempo de la próxima ejecución
   */
  getNextExecutionTime() {
    if (!this.isRunning || !this.stats.startTime) {
      return null;
    }

    try {
      const startTime = new Date(this.stats.startTime);
      const intervalMs = this.intervalMinutes * 60 * 1000;
      const elapsed = Date.now() - startTime.getTime();
      const cycles = Math.floor(elapsed / intervalMs);
      const nextExecutionTime = new Date(startTime.getTime() + (cycles + 1) * intervalMs);
      
      return nextExecutionTime.toISOString();

    } catch (error) {
      logger.error('❌ Error calculando próxima ejecución:', error.message);
      return null;
    }
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
   * Configura nuevo intervalo
   */
  setInterval(minutes) {
    if (minutes <= 0) {
      throw new Error('El intervalo debe ser mayor a 0 minutos');
    }

    const wasRunning = this.isRunning;
    
    if (wasRunning) {
      this.stop();
    }

    this.intervalMinutes = minutes;
    logger.info(`⚙️ Intervalo del scheduler actualizado: ${minutes} minutos`);

    if (wasRunning) {
      this.start();
    }

    return true;
  }

  /**
   * Habilita o deshabilita el scheduler
   */
  setEnabled(enabled) {
    const wasRunning = this.isRunning;
    
    if (wasRunning) {
      this.stop();
    }

    this.enabled = enabled;
    logger.info(`⚙️ Scheduler ${enabled ? 'habilitado' : 'deshabilitado'}`);

    if (enabled && wasRunning) {
      this.start();
    }

    return true;
  }

  /**
   * Obtiene estadísticas detalladas
   */
  async getDetailedStats() {
    const status = await this.getStatus();
    
    return {
      ...status,
      processor: this.processor ? 'initialized' : 'not_initialized',
      configuration: {
        intervalMinutes: this.intervalMinutes,
        enabled: this.enabled,
        maxConcurrentJobs: this.maxConcurrentJobs,
        backupCleanupIntervalMinutes: this.backupCleanupIntervalMinutes
      },
      performance: {
        averageExecutionTime: this.stats.totalExecutions > 0 ? 
          'N/A' : 'No data', // Se podría calcular si se guardaran los tiempos
        lastExecutionDuration: this.stats.lastExecution?.processingTime || 'N/A'
      }
    };
  }

  /**
   * Obtiene alertas de backups que exceden el máximo de reintentos
   * @returns {Array} Lista de alertas de backups
   */
  async getBackupAlerts() {
    try {
      const backupFiles = await this.backupManager.getLocalBackupFiles();
      const alerts = [];

      // Buscar también archivos que ya fallaron definitivamente
      const allFiles = await this.backupManager.getAllBackupFiles();
      
      for (const backup of allFiles) {
        if (backup.status === 'failed' || 
            (backup.metadata && backup.metadata.retryCount >= backup.metadata.maxRetries)) {
          alerts.push({
            backupId: backup.id,
            type: backup.type || 'unknown',
            timestamp: backup.timestamp,
            retryCount: backup.metadata?.retryCount || 0,
            maxRetries: backup.metadata?.maxRetries || 3,
            lastError: backup.error?.message || backup.lastError?.message || 'Error desconocido',
            recordCount: backup.metadata?.recordCount || 0
          });
        }
      }

      return alerts;

    } catch (error) {
      logger.error('❌ Error obteniendo alertas de backup:', error.message);
      return [];
    }
  }

  /**
   * Ejecuta procesamiento manual de backups pendientes
   * @returns {Object} Resultado del procesamiento
   */
  async processBackupsManually() {
    try {
      logger.info('🔧 Ejecutando procesamiento manual de backups...');
      
      const result = await this.processLocalBackups();
      
      logger.info('✅ Procesamiento manual de backups completado');
      return result;

    } catch (error) {
      logger.error('❌ Error en procesamiento manual de backups:', error.message);
      throw error;
    }
  }

  /**
   * Limpia recursos del scheduler
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del scheduler...');
      
      this.stop();
      
      if (this.processor) {
        await this.processor.cleanup();
      }
      
      logger.info('✅ Recursos del scheduler limpiados');

    } catch (error) {
      logger.error('❌ Error limpiando recursos del scheduler:', error.message);
    }
  }
}