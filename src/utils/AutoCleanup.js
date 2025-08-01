import { logger } from './logger.js';
import { MetricsCollector } from './MetricsCollector.js';
import { AlertManager } from './AlertManager.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Sistema de limpieza automática para archivos GCS y locales
 */
export class AutoCleanup {
  constructor(gcsAdapter = null) {
    this.enabled = process.env.AUTO_CLEANUP_ENABLED === 'true';
    this.schedule = process.env.AUTO_CLEANUP_SCHEDULE || '0 2 * * *'; // 2 AM diario
    
    // Configuración de retención (en días)
    this.retention = {
      gcsFiles: parseInt(process.env.GCS_FILE_RETENTION_DAYS) || 7,
      backupFiles: parseInt(process.env.BACKUP_FILE_RETENTION_DAYS) || 30,
      logFiles: parseInt(process.env.LOG_FILE_RETENTION_DAYS) || 30,
      metricsFiles: parseInt(process.env.METRICS_FILE_RETENTION_DAYS) || 30
    };

    // Configuración de batch para limpieza GCS
    this.batchSize = parseInt(process.env.GCS_CLEANUP_BATCH_SIZE) || 100;
    this.retentionHours = parseInt(process.env.GCS_CLEANUP_RETENTION_HOURS) || 24;

    this.gcsAdapter = gcsAdapter;
    this.metricsCollector = MetricsCollector.getInstance();
    this.alertManager = new AlertManager();

    // Estadísticas de limpieza
    this.stats = {
      lastRun: null,
      totalFilesDeleted: 0,
      totalSpaceFreed: 0,
      errors: 0,
      lastError: null
    };
  }

  /**
   * Ejecuta limpieza completa
   */
  async runFullCleanup() {
    if (!this.enabled) {
      logger.info('🧹 Limpieza automática deshabilitada');
      return;
    }

    logger.info('🧹 Iniciando limpieza automática completa');
    const startTime = Date.now();

    try {
      const results = await Promise.allSettled([
        this.cleanupGCSFiles(),
        this.cleanupBackupFiles(),
        this.cleanupLogFiles(),
        this.cleanupMetricsFiles(),
        this.cleanupTempFiles()
      ]);

      // Procesar resultados
      const summary = this.procesCleanupResults(results);
      
      this.stats.lastRun = new Date().toISOString();
      this.stats.totalFilesDeleted += summary.totalFiles;
      this.stats.totalSpaceFreed += summary.totalSize;

      const duration = Date.now() - startTime;
      
      logger.info('✅ Limpieza automática completada', {
        duration: `${duration}ms`,
        filesDeleted: summary.totalFiles,
        spaceFreed: this.formatBytes(summary.totalSize),
        errors: summary.errors
      });

      // Actualizar métricas
      await this.updateCleanupMetrics(summary);

      return summary;

    } catch (error) {
      this.stats.errors++;
      this.stats.lastError = {
        message: error.message,
        timestamp: new Date().toISOString()
      };

      logger.error('❌ Error en limpieza automática:', error.message);
      throw error;
    }
  }

  /**
   * Limpia archivos antiguos de GCS
   */
  async cleanupGCSFiles() {
    if (!this.gcsAdapter) {
      logger.warn('⚠️ GCSAdapter no disponible para limpieza');
      return { files: 0, size: 0, errors: [] };
    }

    try {
      logger.info('🧹 Limpiando archivos GCS antiguos');
      
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - this.retention.gcsFiles);

      // Listar archivos antiguos
      const oldFiles = await this.gcsAdapter.listOldFiles(cutoffDate, this.batchSize);
      
      if (oldFiles.length === 0) {
        logger.info('✅ No hay archivos GCS antiguos para limpiar');
        return { files: 0, size: 0, errors: [] };
      }

      logger.info(`🗑️ Eliminando ${oldFiles.length} archivos GCS antiguos`);

      let deletedFiles = 0;
      let deletedSize = 0;
      const errors = [];

      // Eliminar archivos en lotes
      for (const file of oldFiles) {
        try {
          await this.gcsAdapter.deleteFile(file.name);
          deletedFiles++;
          deletedSize += file.size || 0;
          
          logger.debug(`🗑️ Archivo GCS eliminado: ${file.name}`);
          
        } catch (error) {
          errors.push({
            file: file.name,
            error: error.message
          });
          logger.warn(`⚠️ Error eliminando archivo GCS ${file.name}:`, error.message);
        }
      }

      logger.info(`✅ Limpieza GCS completada: ${deletedFiles} archivos, ${this.formatBytes(deletedSize)}`);

      return {
        files: deletedFiles,
        size: deletedSize,
        errors
      };

    } catch (error) {
      logger.error('❌ Error en limpieza GCS:', error.message);
      return { files: 0, size: 0, errors: [{ error: error.message }] };
    }
  }

  /**
   * Limpia archivos de backup antiguos
   */
  async cleanupBackupFiles() {
    return this.cleanupDirectoryByAge('tmp/backup', this.retention.backupFiles, 'backup');
  }

  /**
   * Limpia archivos de log antiguos
   */
  async cleanupLogFiles() {
    return this.cleanupDirectoryByAge('logs', this.retention.logFiles, 'log');
  }

  /**
   * Limpia archivos de métricas antiguos
   */
  async cleanupMetricsFiles() {
    return this.cleanupDirectoryByAge('tmp', this.retention.metricsFiles, 'metrics', ['metrics.json', 'metrics-*.json']);
  }

  /**
   * Limpia archivos temporales
   */
  async cleanupTempFiles() {
    const tempDirs = ['tmp/gcs-simulation', 'tmp/bigquery-simulation'];
    let totalFiles = 0;
    let totalSize = 0;
    const errors = [];

    for (const dir of tempDirs) {
      try {
        const result = await this.cleanupDirectoryByAge(dir, 1, 'temp'); // 1 día para archivos temp
        totalFiles += result.files;
        totalSize += result.size;
        errors.push(...result.errors);
      } catch (error) {
        errors.push({ dir, error: error.message });
      }
    }

    return { files: totalFiles, size: totalSize, errors };
  }

  /**
   * Limpia directorio por antigüedad de archivos
   */
  async cleanupDirectoryByAge(dirPath, retentionDays, type, patterns = null) {
    try {
      const exists = await this.pathExists(dirPath);
      if (!exists) {
        return { files: 0, size: 0, errors: [] };
      }

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      const files = await fs.readdir(dirPath, { withFileTypes: true });
      let deletedFiles = 0;
      let deletedSize = 0;
      const errors = [];

      for (const file of files) {
        if (!file.isFile()) continue;

        // Filtrar por patrones si se especifican
        if (patterns && !patterns.some(pattern => 
          pattern.includes('*') ? 
            new RegExp(pattern.replace('*', '.*')).test(file.name) : 
            file.name === pattern
        )) {
          continue;
        }

        try {
          const filePath = path.join(dirPath, file.name);
          const stats = await fs.stat(filePath);

          if (stats.mtime < cutoffDate) {
            await fs.unlink(filePath);
            deletedFiles++;
            deletedSize += stats.size;
            
            logger.debug(`🗑️ Archivo ${type} eliminado: ${filePath}`);
          }

        } catch (error) {
          errors.push({
            file: file.name,
            error: error.message
          });
        }
      }

      if (deletedFiles > 0) {
        logger.info(`✅ Limpieza ${type} completada: ${deletedFiles} archivos, ${this.formatBytes(deletedSize)}`);
      }

      return { files: deletedFiles, size: deletedSize, errors };

    } catch (error) {
      logger.error(`❌ Error limpiando directorio ${type} (${dirPath}):`, error.message);
      return { files: 0, size: 0, errors: [{ error: error.message }] };
    }
  }

  /**
   * Procesa resultados de limpieza
   */
  procesCleanupResults(results) {
    let totalFiles = 0;
    let totalSize = 0;
    let totalErrors = 0;
    const errorDetails = [];

    results.forEach((result, index) => {
      const types = ['GCS', 'Backup', 'Log', 'Metrics', 'Temp'];
      const type = types[index];

      if (result.status === 'fulfilled') {
        const value = result.value;
        totalFiles += value.files;
        totalSize += value.size;
        totalErrors += value.errors.length;
        
        if (value.errors.length > 0) {
          errorDetails.push({
            type,
            errors: value.errors
          });
        }
      } else {
        totalErrors++;
        errorDetails.push({
          type,
          errors: [{ error: result.reason.message }]
        });
      }
    });

    return {
      totalFiles,
      totalSize,
      errors: totalErrors,
      errorDetails
    };
  }

  /**
   * Actualiza métricas de limpieza
   */
  async updateCleanupMetrics(summary) {
    try {
      // Crear métricas específicas de limpieza si no existen
      const metrics = await this.metricsCollector.getMetrics();
      
      if (!metrics.cleanup) {
        metrics.cleanup = {
          totalRuns: 0,
          totalFilesDeleted: 0,
          totalSpaceFreed: 0,
          lastRun: null,
          errors: 0
        };
      }

      metrics.cleanup.totalRuns++;
      metrics.cleanup.totalFilesDeleted += summary.totalFiles;
      metrics.cleanup.totalSpaceFreed += summary.totalSize;
      metrics.cleanup.lastRun = new Date().toISOString();
      metrics.cleanup.errors += summary.errors;

      await this.metricsCollector.saveMetrics();

    } catch (error) {
      logger.error('❌ Error actualizando métricas de limpieza:', error.message);
    }
  }

  /**
   * Ejecuta limpieza inteligente basada en uso
   */
  async runIntelligentCleanup() {
    if (!this.enabled) return;

    logger.info('🧠 Iniciando limpieza inteligente');

    try {
      // Analizar patrones de uso
      const usagePatterns = await this.analyzeUsagePatterns();
      
      // Generar estrategia de limpieza optimizada
      const strategy = this.generateCleanupStrategy(usagePatterns);
      
      // Ejecutar limpieza con estrategia
      const results = await this.executeCleanupStrategy(strategy);
      
      logger.info('✅ Limpieza inteligente completada', {
        strategy: strategy.name,
        filesDeleted: results.totalFiles,
        spaceFreed: this.formatBytes(results.totalSize)
      });

      return results;

    } catch (error) {
      logger.error('❌ Error en limpieza inteligente:', error.message);
      throw error;
    }
  }

  /**
   * Analiza patrones de uso para optimizar limpieza
   */
  async analyzeUsagePatterns() {
    try {
      const patterns = {
        diskUsage: await this.getDiskUsage(),
        fileAgeDistribution: await this.getFileAgeDistribution(),
        accessPatterns: await this.getAccessPatterns(),
        costImpact: await this.getCostImpact()
      };

      return patterns;

    } catch (error) {
      logger.error('❌ Error analizando patrones de uso:', error.message);
      return this.getDefaultPatterns();
    }
  }

  /**
   * Obtiene uso de disco
   */
  async getDiskUsage() {
    try {
      const dirs = ['tmp', 'logs', 'tmp/backup'];
      const usage = {};

      for (const dir of dirs) {
        const exists = await this.pathExists(dir);
        if (!exists) continue;

        const files = await fs.readdir(dir, { withFileTypes: true });
        let totalSize = 0;
        let fileCount = 0;

        for (const file of files) {
          if (!file.isFile()) continue;
          
          try {
            const filePath = path.join(dir, file.name);
            const stats = await fs.stat(filePath);
            totalSize += stats.size;
            fileCount++;
          } catch (error) {
            // Ignorar errores de archivos individuales
          }
        }

        usage[dir] = {
          totalSize,
          fileCount,
          avgFileSize: fileCount > 0 ? totalSize / fileCount : 0
        };
      }

      return usage;

    } catch (error) {
      return {};
    }
  }

  /**
   * Obtiene distribución de edad de archivos
   */
  async getFileAgeDistribution() {
    const distribution = {
      veryOld: 0,    // > 30 días
      old: 0,        // 7-30 días
      recent: 0,     // 1-7 días
      new: 0         // < 1 día
    };

    try {
      const dirs = ['tmp', 'logs', 'tmp/backup'];
      const now = new Date();

      for (const dir of dirs) {
        const exists = await this.pathExists(dir);
        if (!exists) continue;

        const files = await fs.readdir(dir, { withFileTypes: true });

        for (const file of files) {
          if (!file.isFile()) continue;
          
          try {
            const filePath = path.join(dir, file.name);
            const stats = await fs.stat(filePath);
            const ageInDays = (now - stats.mtime) / (1000 * 60 * 60 * 24);

            if (ageInDays > 30) distribution.veryOld++;
            else if (ageInDays > 7) distribution.old++;
            else if (ageInDays > 1) distribution.recent++;
            else distribution.new++;

          } catch (error) {
            // Ignorar errores de archivos individuales
          }
        }
      }

      return distribution;

    } catch (error) {
      return distribution;
    }
  }

  /**
   * Obtiene patrones de acceso (simplificado)
   */
  async getAccessPatterns() {
    return {
      highAccess: ['logs', 'tmp/metrics.json'],
      mediumAccess: ['tmp/backup'],
      lowAccess: ['tmp/gcs-simulation', 'tmp/bigquery-simulation']
    };
  }

  /**
   * Obtiene impacto en costos
   */
  async getCostImpact() {
    const estimate = await this.getCleanupEstimate();
    
    return {
      potentialSavings: estimate.estimatedSize * 0.02 / (1024 * 1024 * 1024), // $0.02 per GB
      storageReduction: estimate.estimatedSize,
      files: estimate.estimatedFiles
    };
  }

  /**
   * Genera estrategia de limpieza optimizada
   */
  generateCleanupStrategy(patterns) {
    const diskUsage = Object.values(patterns.diskUsage || {})
      .reduce((sum, usage) => sum + usage.totalSize, 0);
    
    const totalFiles = patterns.fileAgeDistribution?.veryOld + 
                      patterns.fileAgeDistribution?.old || 0;

    // Estrategia agresiva si hay mucho uso de disco
    if (diskUsage > 1024 * 1024 * 1024) { // > 1GB
      return {
        name: 'aggressive',
        description: 'Limpieza agresiva por alto uso de disco',
        retention: {
          gcsFiles: Math.max(3, this.retention.gcsFiles - 2),
          backupFiles: Math.max(7, this.retention.backupFiles - 7),
          logFiles: Math.max(7, this.retention.logFiles - 7),
          metricsFiles: Math.max(7, this.retention.metricsFiles - 7)
        },
        priority: ['veryOld', 'old', 'recent']
      };
    }

    // Estrategia conservadora si hay pocos archivos antiguos
    if (totalFiles < 50) {
      return {
        name: 'conservative',
        description: 'Limpieza conservadora por pocos archivos antiguos',
        retention: {
          gcsFiles: this.retention.gcsFiles + 2,
          backupFiles: this.retention.backupFiles + 7,
          logFiles: this.retention.logFiles + 7,
          metricsFiles: this.retention.metricsFiles + 7
        },
        priority: ['veryOld']
      };
    }

    // Estrategia balanceada (por defecto)
    return {
      name: 'balanced',
      description: 'Limpieza balanceada estándar',
      retention: this.retention,
      priority: ['veryOld', 'old']
    };
  }

  /**
   * Ejecuta estrategia de limpieza
   */
  async executeCleanupStrategy(strategy) {
    logger.info(`🧹 Ejecutando estrategia: ${strategy.name} - ${strategy.description}`);

    // Actualizar configuración temporal
    const originalRetention = { ...this.retention };
    this.retention = strategy.retention;

    try {
      const results = await this.runFullCleanup();
      
      // Restaurar configuración original
      this.retention = originalRetention;
      
      return results;

    } catch (error) {
      // Restaurar configuración original en caso de error
      this.retention = originalRetention;
      throw error;
    }
  }

  /**
   * Obtiene patrones por defecto
   */
  getDefaultPatterns() {
    return {
      diskUsage: {},
      fileAgeDistribution: { veryOld: 0, old: 0, recent: 0, new: 0 },
      accessPatterns: { highAccess: [], mediumAccess: [], lowAccess: [] },
      costImpact: { potentialSavings: 0, storageReduction: 0, files: 0 }
    };
  }

  /**
   * Ejecuta limpieza de emergencia
   */
  async runEmergencyCleanup() {
    logger.warn('🚨 Ejecutando limpieza de emergencia');

    const emergencyStrategy = {
      name: 'emergency',
      description: 'Limpieza de emergencia por espacio crítico',
      retention: {
        gcsFiles: 1,
        backupFiles: 3,
        logFiles: 3,
        metricsFiles: 3
      },
      priority: ['veryOld', 'old', 'recent', 'new']
    };

    return this.executeCleanupStrategy(emergencyStrategy);
  }

  /**
   * Programa limpieza automática
   */
  scheduleCleanup() {
    if (!this.enabled) return;

    logger.info(`📅 Limpieza automática programada: ${this.schedule}`);
    
    // Ejecutar limpieza inicial después de 1 minuto
    setTimeout(() => {
      this.runIntelligentCleanup().catch(error => {
        logger.error('❌ Error en limpieza programada:', error.message);
      });
    }, 60000);

    // Programar ejecución diaria (simplificado)
    this.cleanupInterval = setInterval(() => {
      this.runIntelligentCleanup().catch(error => {
        logger.error('❌ Error en limpieza programada:', error.message);
      });
    }, 24 * 60 * 60 * 1000); // 24 horas

    // Programar verificación de espacio cada hora
    this.diskCheckInterval = setInterval(() => {
      this.checkDiskSpace().catch(error => {
        logger.error('❌ Error verificando espacio en disco:', error.message);
      });
    }, 60 * 60 * 1000); // 1 hora
  }

  /**
   * Detiene la limpieza programada
   */
  stopCleanup() {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
    if (this.diskCheckInterval) {
      clearInterval(this.diskCheckInterval);
      this.diskCheckInterval = null;
    }
    logger.info('🛑 Limpieza automática detenida');
  }

  /**
   * Verifica espacio en disco y ejecuta limpieza de emergencia si es necesario
   */
  async checkDiskSpace() {
    try {
      const usage = await this.getDiskUsage();
      const totalSize = Object.values(usage).reduce((sum, u) => sum + u.totalSize, 0);
      
      // Si el uso supera 2GB, ejecutar limpieza de emergencia
      if (totalSize > 2 * 1024 * 1024 * 1024) {
        logger.warn(`⚠️ Alto uso de disco detectado: ${this.formatBytes(totalSize)}`);
        await this.runEmergencyCleanup();
      }

    } catch (error) {
      logger.error('❌ Error verificando espacio en disco:', error.message);
    }
  }

  /**
   * Obtiene estadísticas de limpieza
   */
  getStats() {
    return {
      ...this.stats,
      configuration: {
        enabled: this.enabled,
        schedule: this.schedule,
        retention: this.retention,
        batchSize: this.batchSize
      }
    };
  }

  /**
   * Obtiene estimación de espacio a liberar
   */
  async getCleanupEstimate() {
    try {
      const estimates = await Promise.allSettled([
        this.estimateGCSCleanup(),
        this.estimateDirectoryCleanup('tmp/backup', this.retention.backupFiles),
        this.estimateDirectoryCleanup('logs', this.retention.logFiles),
        this.estimateDirectoryCleanup('tmp', this.retention.metricsFiles)
      ]);

      let totalFiles = 0;
      let totalSize = 0;

      estimates.forEach(result => {
        if (result.status === 'fulfilled') {
          totalFiles += result.value.files;
          totalSize += result.value.size;
        }
      });

      return {
        estimatedFiles: totalFiles,
        estimatedSize: totalSize,
        formattedSize: this.formatBytes(totalSize),
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error estimando limpieza:', error.message);
      return { estimatedFiles: 0, estimatedSize: 0, formattedSize: '0 B' };
    }
  }

  /**
   * Estima limpieza de GCS
   */
  async estimateGCSCleanup() {
    if (!this.gcsAdapter) {
      return { files: 0, size: 0 };
    }

    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - this.retention.gcsFiles);
      
      const oldFiles = await this.gcsAdapter.listOldFiles(cutoffDate, this.batchSize);
      const totalSize = oldFiles.reduce((sum, file) => sum + (file.size || 0), 0);

      return { files: oldFiles.length, size: totalSize };

    } catch (error) {
      return { files: 0, size: 0 };
    }
  }

  /**
   * Estima limpieza de directorio
   */
  async estimateDirectoryCleanup(dirPath, retentionDays) {
    try {
      const exists = await this.pathExists(dirPath);
      if (!exists) return { files: 0, size: 0 };

      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      const files = await fs.readdir(dirPath, { withFileTypes: true });
      let oldFiles = 0;
      let oldSize = 0;

      for (const file of files) {
        if (!file.isFile()) continue;

        try {
          const filePath = path.join(dirPath, file.name);
          const stats = await fs.stat(filePath);

          if (stats.mtime < cutoffDate) {
            oldFiles++;
            oldSize += stats.size;
          }
        } catch (error) {
          // Ignorar errores de archivos individuales
        }
      }

      return { files: oldFiles, size: oldSize };

    } catch (error) {
      return { files: 0, size: 0 };
    }
  }

  /**
   * Verifica si existe un path
   */
  async pathExists(path) {
    try {
      await fs.access(path);
      return true;
    } catch {
      return false;
    }
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
}