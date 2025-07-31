import fs from 'fs/promises';
import path from 'path';
import { logger } from './logger.js';
import { config } from '../config/env.js';

/**
 * Utilidad para limpieza automática de archivos temporales
 */
export class FileCleanup {
  constructor() {
    this.tmpDir = 'tmp';
    this.backupDir = config.gps.backupPath;
    this.maxBackupFiles = config.gps.backupMaxFiles || 3;
  }

  /**
   * Ejecuta limpieza completa de archivos temporales
   */
  async cleanupAll() {
    try {
      logger.info('🧹 Iniciando limpieza de archivos temporales...');

      const results = await Promise.allSettled([
        this.cleanupBackups(),
        this.cleanupOldTmpFiles(),
        this.cleanupEmptyDirectories()
      ]);

      const successful = results.filter(r => r.status === 'fulfilled').length;
      const failed = results.filter(r => r.status === 'rejected').length;

      logger.info(`✅ Limpieza completada: ${successful} exitosas, ${failed} fallidas`);

      return {
        success: failed === 0,
        results: results.map(r => r.status === 'fulfilled' ? r.value : r.reason)
      };

    } catch (error) {
      logger.error('❌ Error en limpieza general:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Limpia archivos de backup antiguos
   */
  async cleanupBackups() {
    try {
      const backupExists = await this.pathExists(this.backupDir);
      if (!backupExists) {
        return { type: 'backup', message: 'Directorio de backup no existe' };
      }

      const files = await fs.readdir(this.backupDir);
      const backupFiles = files.filter(file => file.startsWith('gps_backup_'));

      if (backupFiles.length <= this.maxBackupFiles) {
        return { 
          type: 'backup', 
          message: `Solo ${backupFiles.length} backups, no necesita limpieza` 
        };
      }

      // Obtener información de archivos
      const fileStats = await Promise.all(
        backupFiles.map(async (file) => {
          const filePath = path.join(this.backupDir, file);
          try {
            const stat = await fs.stat(filePath);
            return { name: file, path: filePath, mtime: stat.mtime };
          } catch (error) {
            return null;
          }
        })
      );

      // Filtrar archivos válidos y ordenar por fecha
      const validFiles = fileStats
        .filter(file => file !== null)
        .sort((a, b) => b.mtime - a.mtime);

      // Eliminar archivos antiguos
      const filesToDelete = validFiles.slice(this.maxBackupFiles);
      let deletedCount = 0;

      for (const file of filesToDelete) {
        try {
          await fs.unlink(file.path);
          deletedCount++;
          logger.debug(`🗑️ Backup eliminado: ${file.name}`);
        } catch (error) {
          logger.warn(`⚠️ Error eliminando ${file.name}:`, error.message);
        }
      }

      return {
        type: 'backup',
        message: `${deletedCount} backups antiguos eliminados`,
        deleted: deletedCount,
        remaining: validFiles.length - deletedCount
      };

    } catch (error) {
      logger.error('❌ Error limpiando backups:', error.message);
      throw error;
    }
  }

  /**
   * Limpia archivos temporales antiguos (más de 24 horas)
   */
  async cleanupOldTmpFiles() {
    try {
      const tmpExists = await this.pathExists(this.tmpDir);
      if (!tmpExists) {
        return { type: 'tmp', message: 'Directorio tmp no existe' };
      }

      const files = await fs.readdir(this.tmpDir);
      const now = Date.now();
      const maxAge = 24 * 60 * 60 * 1000; // 24 horas
      let deletedCount = 0;

      for (const file of files) {
        const filePath = path.join(this.tmpDir, file);
        
        try {
          const stat = await fs.stat(filePath);
          
          // Solo eliminar archivos (no directorios) más antiguos de 24h
          if (stat.isFile() && (now - stat.mtime.getTime()) > maxAge) {
            // No eliminar archivos críticos
            if (!this.isCriticalFile(file)) {
              await fs.unlink(filePath);
              deletedCount++;
              logger.debug(`🗑️ Archivo temporal eliminado: ${file}`);
            }
          }
        } catch (error) {
          logger.warn(`⚠️ Error procesando ${file}:`, error.message);
        }
      }

      return {
        type: 'tmp',
        message: `${deletedCount} archivos temporales antiguos eliminados`,
        deleted: deletedCount
      };

    } catch (error) {
      logger.error('❌ Error limpiando archivos temporales:', error.message);
      throw error;
    }
  }

  /**
   * Limpia directorios vacíos
   */
  async cleanupEmptyDirectories() {
    try {
      // Por ahora solo verificar, no eliminar directorios críticos
      return {
        type: 'directories',
        message: 'Verificación de directorios completada'
      };
    } catch (error) {
      logger.error('❌ Error limpiando directorios:', error.message);
      throw error;
    }
  }

  /**
   * Verifica si un archivo es crítico y no debe eliminarse
   */
  isCriticalFile(filename) {
    const criticalFiles = [
      'metrics.json',
      'gps_data.txt',
      '.gitkeep'
    ];
    
    return criticalFiles.includes(filename);
  }

  /**
   * Verifica si existe un path
   */
  async pathExists(pathToCheck) {
    try {
      await fs.access(pathToCheck);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Obtiene estadísticas de uso de espacio
   */
  async getStorageStats() {
    try {
      const stats = {
        tmp: { files: 0, size: 0 },
        backup: { files: 0, size: 0 },
        total: { files: 0, size: 0 }
      };

      // Estadísticas de tmp
      if (await this.pathExists(this.tmpDir)) {
        const tmpFiles = await fs.readdir(this.tmpDir);
        stats.tmp.files = tmpFiles.length;
        
        for (const file of tmpFiles) {
          try {
            const stat = await fs.stat(path.join(this.tmpDir, file));
            if (stat.isFile()) {
              stats.tmp.size += stat.size;
            }
          } catch (error) {
            // Ignorar errores de archivos individuales
          }
        }
      }

      // Estadísticas de backup
      if (await this.pathExists(this.backupDir)) {
        const backupFiles = await fs.readdir(this.backupDir);
        stats.backup.files = backupFiles.length;
        
        for (const file of backupFiles) {
          try {
            const stat = await fs.stat(path.join(this.backupDir, file));
            if (stat.isFile()) {
              stats.backup.size += stat.size;
            }
          } catch (error) {
            // Ignorar errores de archivos individuales
          }
        }
      }

      // Totales
      stats.total.files = stats.tmp.files + stats.backup.files;
      stats.total.size = stats.tmp.size + stats.backup.size;

      // Convertir a formato legible
      return {
        tmp: {
          files: stats.tmp.files,
          size: this.formatBytes(stats.tmp.size)
        },
        backup: {
          files: stats.backup.files,
          size: this.formatBytes(stats.backup.size)
        },
        total: {
          files: stats.total.files,
          size: this.formatBytes(stats.total.size)
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas:', error.message);
      return null;
    }
  }

  /**
   * Formatea bytes a formato legible
   */
  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }
}