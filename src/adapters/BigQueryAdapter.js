import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { FileUtils } from '../utils/FileUtils.js';
import { BackupManager } from '../utils/BackupManager.js';

/**
 * Adaptador para BigQuery (simulado con archivos de texto)
 * En el futuro se reemplazará con la API real de BigQuery
 */
export class BigQueryAdapter {
  constructor() {
    this.outputPath = config.gps.outputFilePath;
    this.backupEnabled = config.gps.backupEnabled;
    this.backupPath = config.gps.backupPath;
    this.backupManager = new BackupManager();
  }

  /**
   * Inicializa el adaptador creando directorios necesarios
   */
  async initialize() {
    try {
      // Crear directorio de salida si no existe
      await FileUtils.ensureDirectoryExists(path.dirname(this.outputPath));
      
      // Crear directorio de backup si está habilitado
      if (this.backupEnabled) {
        await FileUtils.ensureDirectoryExists(this.backupPath);
      }

      logger.info('✅ BigQuery Adapter inicializado');
    } catch (error) {
      logger.error('❌ Error inicializando BigQuery Adapter:', error.message);
      throw error;
    }
  }

  /**
   * Simula la subida de datos a BigQuery escribiendo a un archivo
   */
  async uploadData(gpsData) {
    try {
      if (!gpsData || gpsData.length === 0) {
        logger.info('📤 No hay datos para subir a BigQuery');
        return { success: true, recordsProcessed: 0, message: 'No data to process' };
      }

      await this.initialize();

      // Primero procesar backups pendientes
      await this.processRecoveryBackups();

      // Preparar datos para escritura
      const formattedData = this.formatDataForBigQuery(gpsData);
      const timestamp = new Date().toISOString();
      
      // Crear backup ANTES de intentar subir (para recovery)
      let backupResult = null;
      if (this.backupEnabled && gpsData.length > 0) {
        backupResult = await this.backupManager.createBackup(formattedData, {
          originalRecordCount: gpsData.length,
          processingTimestamp: timestamp
        });
      }

      try {
        // Crear contenido del archivo
        const fileContent = this.createFileContent(formattedData, timestamp);
        
        // Escribir archivo principal (simular BigQuery)
        await fs.writeFile(this.outputPath, fileContent, 'utf8');

        // Si llegamos aquí, la subida fue exitosa
        if (backupResult && backupResult.success) {
          await this.backupManager.markAsCompleted(backupResult.backupId, backupResult.filePath, {
            recordsProcessed: gpsData.length,
            outputFile: this.outputPath,
            fileSize: Buffer.byteLength(fileContent, 'utf8')
          });
        }

        logger.info(`📤 Datos subidos exitosamente a BigQuery (simulado): ${gpsData.length} registros`);
        
        return {
          success: true,
          recordsProcessed: gpsData.length,
          outputFile: this.outputPath,
          timestamp: timestamp,
          fileSize: Buffer.byteLength(fileContent, 'utf8'),
          backupId: backupResult?.backupId
        };

      } catch (uploadError) {
        // Si falla la subida, marcar backup como pendiente para retry
        if (backupResult && backupResult.success) {
          await this.backupManager.markAsFailed(backupResult.backupId, backupResult.filePath, uploadError);
        }
        
        throw uploadError; // Re-lanzar el error
      }

    } catch (error) {
      logger.error('❌ Error subiendo datos a BigQuery:', error.message);
      return {
        success: false,
        error: error.message,
        recordsProcessed: 0
      };
    }
  }

  /**
   * Procesa backups pendientes de recovery
   */
  async processRecoveryBackups() {
    try {
      const result = await this.backupManager.processAllPendingBackups(async (backupData) => {
        try {
          // Crear contenido del archivo desde backup
          const fileContent = this.createFileContent(backupData, new Date().toISOString());
          
          // Intentar escribir a BigQuery (simulado)
          await fs.writeFile(this.outputPath, fileContent, 'utf8');
          
          logger.info(`🔄 Recovery exitoso: ${Array.isArray(backupData) ? backupData.length : 0} registros desde backup`);
          
          return {
            success: true,
            recordsProcessed: Array.isArray(backupData) ? backupData.length : 0,
            outputFile: this.outputPath
          };
          
        } catch (error) {
          logger.error('❌ Error en recovery de backup:', error.message);
          return {
            success: false,
            error: error.message
          };
        }
      });

      if (result.processed > 0) {
        logger.info(`🔄 Recovery completado: ${result.processed} backups procesados`);
      }

      return result;

    } catch (error) {
      logger.error('❌ Error procesando recovery backups:', error.message);
      return { success: false, error: error.message, processed: 0 };
    }
  }

  /**
   * Formatea los datos GPS para BigQuery
   */
  formatDataForBigQuery(gpsData) {
    return gpsData.map((record, index) => {
      // Si el record ya es un objeto, usarlo directamente
      let gpsRecord = typeof record === 'string' ? 
        this.parseGPSRecord(record) : record;

      // Asegurar que tenga los campos requeridos
      return {
        id: gpsRecord.id || `gps_${Date.now()}_${index}`,
        latitude: gpsRecord.latitude || gpsRecord.lat || 0,
        longitude: gpsRecord.longitude || gpsRecord.lng || gpsRecord.lon || 0,
        timestamp: gpsRecord.timestamp || gpsRecord.time || new Date().toISOString(),
        speed: gpsRecord.speed || 0,
        heading: gpsRecord.heading || gpsRecord.bearing || 0,
        altitude: gpsRecord.altitude || gpsRecord.alt || 0,
        accuracy: gpsRecord.accuracy || 0,
        device_id: gpsRecord.device_id || gpsRecord.deviceId || 'unknown',
        processed_at: new Date().toISOString(),
        ...gpsRecord // Incluir campos adicionales
      };
    });
  }

  /**
   * Parsea un registro GPS desde string
   */
  parseGPSRecord(recordString) {
    try {
      return JSON.parse(recordString);
    } catch (error) {
      logger.warn('⚠️ Error parseando registro GPS, usando formato básico');
      return {
        raw_data: recordString,
        error: 'parse_failed'
      };
    }
  }

  /**
   * Crea el contenido del archivo con formato estructurado
   */
  createFileContent(formattedData, timestamp) {
    const header = [
      '='.repeat(80),
      `GPS DATA EXPORT TO BIGQUERY`,
      `Timestamp: ${timestamp}`,
      `Records: ${formattedData.length}`,
      `Generated by: GPS-BigQuery Microservice`,
      '='.repeat(80),
      ''
    ].join('\n');

    const jsonData = JSON.stringify(formattedData, null, 2);
    
    const footer = [
      '',
      '='.repeat(80),
      `END OF EXPORT - ${formattedData.length} records processed`,
      `Export completed at: ${new Date().toISOString()}`,
      '='.repeat(80)
    ].join('\n');

    return header + jsonData + footer;
  }

  /**
   * Crea un archivo de backup
   */
  async createBackup(content, timestamp) {
    try {
      const backupFileName = `gps_backup_${timestamp.replace(/[:.]/g, '-')}.txt`;
      const backupFilePath = path.join(this.backupPath, backupFileName);
      
      await fs.writeFile(backupFilePath, content, 'utf8');
      
      logger.info(`💾 Backup creado: ${backupFilePath}`);
      
      // Limpiar backups antiguos (mantener solo los últimos 3)
      await this.cleanOldBackups();
      
    } catch (error) {
      logger.error('❌ Error creando backup:', error.message);
    }
  }

  /**
   * Limpia backups antiguos
   */
  async cleanOldBackups() {
    try {
      const files = await fs.readdir(this.backupPath);
      const backupFiles = files
        .filter(file => file.startsWith('gps_backup_'))
        .map(file => ({
          name: file,
          path: path.join(this.backupPath, file),
          stat: null
        }));

      // Obtener estadísticas de archivos
      for (const file of backupFiles) {
        try {
          file.stat = await fs.stat(file.path);
        } catch (error) {
          logger.warn(`⚠️ Error obteniendo stats de ${file.name}`);
        }
      }

      // Ordenar por fecha de modificación (más reciente primero)
      const filesToDelete = backupFiles
        .filter(file => file.stat)
        .sort((a, b) => b.stat.mtime - a.stat.mtime)
        .slice(3); // Mantener solo los primeros 3, eliminar el resto

      if (filesToDelete.length > 0) {
        logger.info(`🧹 Limpiando ${filesToDelete.length} backups antiguos`);
      }

      filesToDelete
        .forEach(async (file) => {
          try {
            await fs.unlink(file.path);
            logger.info(`🗑️ Backup antiguo eliminado: ${file.name}`);
          } catch (error) {
            logger.warn(`⚠️ Error eliminando backup ${file.name}:`, error.message);
          }
        });

    } catch (error) {
      logger.error('❌ Error limpiando backups antiguos:', error.message);
    }
  }

  /**
   * Verifica el estado del adaptador
   */
  async getStatus() {
    try {
      const outputDir = path.dirname(this.outputPath);
      const outputExists = await FileUtils.pathExists(outputDir);
      
      let backupStatus = null;
      if (this.backupEnabled) {
        const backupExists = await FileUtils.pathExists(this.backupPath);
        const backupFiles = backupExists ? 
          (await fs.readdir(this.backupPath)).filter(f => f.startsWith('gps_backup_')).length : 0;
        
        backupStatus = {
          enabled: true,
          directory: this.backupPath,
          exists: backupExists,
          fileCount: backupFiles
        };
      }

      return {
        initialized: outputExists,
        outputFile: this.outputPath,
        outputDirectory: outputDir,
        backup: backupStatus || { enabled: false }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estado del BigQuery Adapter:', error.message);
      return {
        initialized: false,
        error: error.message
      };
    }
  }

  /**
   * Método para futuro: configurar cliente real de BigQuery
   */
  async initializeBigQueryClient() {
    // TODO: Implementar cuando se tenga acceso a la API de BigQuery
    /*
    const { BigQuery } = require('@google-cloud/bigquery');
    
    this.bigQueryClient = new BigQuery({
      projectId: config.bigquery.projectId,
      keyFilename: config.bigquery.keyFilename,
      location: config.bigquery.location
    });
    */
    
    logger.info('🚧 BigQuery client real no implementado aún - usando simulación');
  }
}