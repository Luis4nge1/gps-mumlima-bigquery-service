import fs from 'fs/promises';
import path from 'path';
import { logger } from './logger.js';

/**
 * Utilidades para manejo de archivos
 */
export class FileUtils {
  /**
   * Verifica si un path existe
   */
  static async pathExists(filePath) {
    try {
      await fs.access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Crea un directorio si no existe
   */
  static async ensureDirectoryExists(dirPath) {
    try {
      const exists = await this.pathExists(dirPath);
      if (!exists) {
        await fs.mkdir(dirPath, { recursive: true });
        logger.info(`📁 Directorio creado: ${dirPath}`);
      }
      return true;
    } catch (error) {
      logger.error(`❌ Error creando directorio ${dirPath}:`, error.message);
      throw error;
    }
  }

  /**
   * Obtiene el tamaño de un archivo
   */
  static async getFileSize(filePath) {
    try {
      const stats = await fs.stat(filePath);
      return stats.size;
    } catch (error) {
      logger.error(`❌ Error obteniendo tamaño de ${filePath}:`, error.message);
      return 0;
    }
  }

  /**
   * Formatea el tamaño de archivo en formato legible
   */
  static formatFileSize(bytes) {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  }

  /**
   * Lee un archivo JSON de forma segura
   */
  static async readJsonFile(filePath) {
    try {
      const content = await fs.readFile(filePath, 'utf8');
      return JSON.parse(content);
    } catch (error) {
      logger.error(`❌ Error leyendo archivo JSON ${filePath}:`, error.message);
      throw error;
    }
  }

  /**
   * Escribe un archivo JSON de forma segura
   */
  static async writeJsonFile(filePath, data) {
    try {
      await this.ensureDirectoryExists(path.dirname(filePath));
      const content = JSON.stringify(data, null, 2);
      await fs.writeFile(filePath, content, 'utf8');
      return true;
    } catch (error) {
      logger.error(`❌ Error escribiendo archivo JSON ${filePath}:`, error.message);
      throw error;
    }
  }

  /**
   * Obtiene información detallada de un archivo
   */
  static async getFileInfo(filePath) {
    try {
      const stats = await fs.stat(filePath);
      return {
        path: filePath,
        size: stats.size,
        sizeFormatted: this.formatFileSize(stats.size),
        created: stats.birthtime,
        modified: stats.mtime,
        accessed: stats.atime,
        isFile: stats.isFile(),
        isDirectory: stats.isDirectory()
      };
    } catch (error) {
      logger.error(`❌ Error obteniendo info de ${filePath}:`, error.message);
      return null;
    }
  }

  /**
   * Lista archivos en un directorio con filtros
   */
  static async listFiles(dirPath, options = {}) {
    try {
      const {
        extension = null,
        pattern = null,
        includeStats = false
      } = options;

      const files = await fs.readdir(dirPath);
      let filteredFiles = files;

      // Filtrar por extensión
      if (extension) {
        filteredFiles = filteredFiles.filter(file => 
          path.extname(file).toLowerCase() === extension.toLowerCase()
        );
      }

      // Filtrar por patrón
      if (pattern) {
        const regex = new RegExp(pattern);
        filteredFiles = filteredFiles.filter(file => regex.test(file));
      }

      // Incluir estadísticas si se solicita
      if (includeStats) {
        const filesWithStats = [];
        for (const file of filteredFiles) {
          const filePath = path.join(dirPath, file);
          const info = await this.getFileInfo(filePath);
          if (info) {
            filesWithStats.push(info);
          }
        }
        return filesWithStats;
      }

      return filteredFiles.map(file => path.join(dirPath, file));

    } catch (error) {
      logger.error(`❌ Error listando archivos en ${dirPath}:`, error.message);
      return [];
    }
  }

  /**
   * Elimina archivos antiguos en un directorio
   */
  static async cleanOldFiles(dirPath, maxAge = 7 * 24 * 60 * 60 * 1000) { // 7 días por defecto
    try {
      const files = await this.listFiles(dirPath, { includeStats: true });
      const now = new Date();
      let deletedCount = 0;

      for (const file of files) {
        if (file.isFile) {
          const age = now - file.modified;
          if (age > maxAge) {
            try {
              await fs.unlink(file.path);
              logger.info(`🗑️ Archivo antiguo eliminado: ${path.basename(file.path)}`);
              deletedCount++;
            } catch (deleteError) {
              logger.warn(`⚠️ Error eliminando ${file.path}:`, deleteError.message);
            }
          }
        }
      }

      if (deletedCount > 0) {
        logger.info(`🧹 Limpieza completada: ${deletedCount} archivos eliminados`);
      }

      return deletedCount;

    } catch (error) {
      logger.error(`❌ Error limpiando archivos antiguos en ${dirPath}:`, error.message);
      return 0;
    }
  }

  /**
   * Elimina un archivo
   */
  static async deleteFile(filePath) {
    try {
      const exists = await this.pathExists(filePath);
      if (exists) {
        await fs.unlink(filePath);
        logger.debug(`🗑️ Archivo eliminado: ${filePath}`);
        return true;
      }
      return false;
    } catch (error) {
      logger.error(`❌ Error eliminando archivo ${filePath}:`, error.message);
      throw error;
    }
  }

  /**
   * Crea un archivo de log con rotación
   */
  static async appendToLogFile(filePath, content, maxSize = 10 * 1024 * 1024) { // 10MB por defecto
    try {
      await this.ensureDirectoryExists(path.dirname(filePath));
      
      // Verificar tamaño del archivo
      const exists = await this.pathExists(filePath);
      if (exists) {
        const size = await this.getFileSize(filePath);
        if (size > maxSize) {
          // Rotar archivo
          const rotatedPath = `${filePath}.${Date.now()}`;
          await fs.rename(filePath, rotatedPath);
          logger.info(`🔄 Archivo de log rotado: ${path.basename(rotatedPath)}`);
        }
      }

      // Agregar timestamp al contenido
      const timestamp = new Date().toISOString();
      const logEntry = `[${timestamp}] ${content}\n`;
      
      await fs.appendFile(filePath, logEntry, 'utf8');
      return true;

    } catch (error) {
      logger.error(`❌ Error escribiendo a log ${filePath}:`, error.message);
      return false;
    }
  }
}