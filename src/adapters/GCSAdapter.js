import { Storage } from '@google-cloud/storage';
import fs from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { FileUtils } from '../utils/FileUtils.js';

/**
 * Adaptador para Google Cloud Storage
 * Maneja operaciones de upload, download, listado y eliminación de archivos
 */
export class GCSAdapter {
  constructor() {
    this.storage = null;
    this.bucket = null;
    this.bucketName = config.gcs?.bucketName || process.env.GCS_BUCKET_NAME || 'gps-data-bucket';
    this.keyFilename = config.gcs?.keyFilename || process.env.GOOGLE_APPLICATION_CREDENTIALS || 'service-account.json';
    this.projectId = config.gcs?.projectId || process.env.GCP_PROJECT_ID || '';
    this.isInitialized = false;
    this.simulationMode = process.env.GCS_SIMULATION_MODE === 'true' || false;
    this.localStoragePath = 'tmp/gcs-simulation/';
  }

  /**
   * Inicializa el cliente de Google Cloud Storage
   */
  async initialize() {
    try {
      if (this.isInitialized) {
        return;
      }

      if (this.simulationMode) {
        logger.info('🔧 GCS Adapter iniciado en modo simulación');
        await FileUtils.ensureDirectoryExists(this.localStoragePath);
        this.isInitialized = true;
        return;
      }

      // Verificar que existe el archivo de credenciales
      const credentialsExist = await FileUtils.pathExists(this.keyFilename);
      if (!credentialsExist) {
        throw new Error(`Archivo de credenciales no encontrado: ${this.keyFilename}`);
      }

      // Inicializar cliente de Storage
      this.storage = new Storage({
        projectId: this.projectId,
        keyFilename: this.keyFilename
      });

      // Obtener referencia al bucket
      this.bucket = this.storage.bucket(this.bucketName);

      // Verificar que el bucket existe
      const [exists] = await this.bucket.exists();
      if (!exists) {
        logger.warn(`⚠️ Bucket ${this.bucketName} no existe, intentando crear...`);
        await this.bucket.create();
        logger.info(`✅ Bucket ${this.bucketName} creado exitosamente`);
      }

      this.isInitialized = true;
      logger.info(`✅ GCS Adapter inicializado - Bucket: ${this.bucketName}`);

    } catch (error) {
      logger.error('❌ Error inicializando GCS Adapter:', error.message);
      
      // Fallback a modo simulación si falla la inicialización
      if (!this.simulationMode) {
        logger.warn('🔧 Fallback a modo simulación por error de inicialización');
        this.simulationMode = true;
        await FileUtils.ensureDirectoryExists(this.localStoragePath);
        this.isInitialized = true;
      } else {
        throw error;
      }
    }
  }

  /**
   * Sube un archivo JSON Lines (NEWLINE_DELIMITED_JSON) a GCS con metadata
   * @param {string} jsonLines - Datos en formato JSON Lines (una línea por objeto)
   * @param {string} fileName - Nombre del archivo
   * @param {Object} metadata - Metadata adicional
   * @returns {Object} Resultado de la operación
   */
  async uploadJSONLines(jsonLines, fileName, metadata = {}) {
    try {
      await this.initialize();

      const timestamp = new Date().toISOString();
      
      // Agregar metadata por defecto
      const fileMetadata = {
        contentType: 'application/json',
        metadata: {
          uploadedAt: timestamp,
          recordCount: jsonLines.split('\n').filter(line => line.trim()).length,
          dataType: metadata.dataType || 'unknown',
          processingId: metadata.processingId || 'unknown',
          format: 'newline_delimited_json',
          ...metadata
        }
      };

      if (this.simulationMode) {
        return await this.uploadJSONLinesSimulated(jsonLines, fileName, fileMetadata);
      }

      // Upload real a GCS
      const file = this.bucket.file(fileName);
      
      await file.save(jsonLines, {
        metadata: fileMetadata,
        resumable: false
      });

      const fileSize = Buffer.byteLength(jsonLines, 'utf8');
      
      logger.info(`📤 Archivo JSON Lines subido a GCS: ${fileName} (${fileSize} bytes)`);

      return {
        success: true,
        fileName,
        bucketName: this.bucketName,
        fileSize,
        uploadedAt: timestamp,
        metadata: fileMetadata.metadata,
        gcsPath: `gs://${this.bucketName}/${fileName}`,
        gcsUri: `gs://${this.bucketName}/${fileName}`
      };

    } catch (error) {
      logger.error(`❌ Error subiendo archivo JSON Lines a GCS: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Simula upload de archivo JSON Lines localmente
   */
  async uploadJSONLinesSimulated(jsonLines, fileName, metadata) {
    try {
      const filePath = path.join(this.localStoragePath, fileName);
      const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
      
      // Crear directorio si no existe
      const fileDir = path.dirname(filePath);
      await fs.mkdir(fileDir, { recursive: true });
      
      // Guardar archivo de datos
      await fs.writeFile(filePath, jsonLines, 'utf8');
      
      // Guardar metadata
      await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), 'utf8');
      
      const fileSize = Buffer.byteLength(jsonLines, 'utf8');
      const timestamp = new Date().toISOString();
      
      logger.info(`📤 Archivo JSON Lines simulado guardado: ${fileName} (${fileSize} bytes)`);

      return {
        success: true,
        fileName,
        bucketName: this.bucketName,
        fileSize,
        uploadedAt: timestamp,
        metadata: metadata.metadata,
        gcsPath: `gs://${this.bucketName}/${fileName}`,
        gcsUri: `gs://${this.bucketName}/${fileName}`,
        localPath: filePath,
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error en upload JSON Lines simulado: ${fileName}`, error.message);
      throw error;
    }
  }

  /**
   * Sube un archivo JSON a GCS con metadata
   * @param {Object} data - Datos a subir
   * @param {string} fileName - Nombre del archivo
   * @param {Object} metadata - Metadata adicional
   * @returns {Object} Resultado de la operación
   */
  async uploadJSON(data, fileName, metadata = {}) {
    try {
      await this.initialize();

      const jsonContent = JSON.stringify(data, null, 2);
      const timestamp = new Date().toISOString();
      
      // Agregar metadata por defecto
      const fileMetadata = {
        contentType: 'application/json',
        metadata: {
          uploadedAt: timestamp,
          recordCount: Array.isArray(data.data) ? data.data.length : 0,
          dataType: data.metadata?.type || 'unknown',
          processingId: data.metadata?.processingId || 'unknown',
          ...metadata
        }
      };

      if (this.simulationMode) {
        return await this.uploadJSONSimulated(jsonContent, fileName, fileMetadata);
      }

      // Upload real a GCS
      const file = this.bucket.file(fileName);
      
      await file.save(jsonContent, {
        metadata: fileMetadata,
        resumable: false
      });

      const fileSize = Buffer.byteLength(jsonContent, 'utf8');
      
      logger.info(`📤 Archivo subido a GCS: ${fileName} (${fileSize} bytes)`);

      return {
        success: true,
        fileName,
        bucketName: this.bucketName,
        fileSize,
        uploadedAt: timestamp,
        metadata: fileMetadata.metadata,
        gcsPath: `gs://${this.bucketName}/${fileName}`,
        gcsUri: `gs://${this.bucketName}/${fileName}`
      };

    } catch (error) {
      logger.error(`❌ Error subiendo archivo a GCS: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Simula upload de archivo JSON localmente
   */
  async uploadJSONSimulated(jsonContent, fileName, metadata) {
    try {
      const filePath = path.join(this.localStoragePath, fileName);
      const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
      
      // Crear directorio si no existe
      const fileDir = path.dirname(filePath);
      await fs.mkdir(fileDir, { recursive: true });
      
      // Guardar archivo de datos
      await fs.writeFile(filePath, jsonContent, 'utf8');
      
      // Guardar metadata
      await fs.writeFile(metadataPath, JSON.stringify(metadata, null, 2), 'utf8');
      
      const fileSize = Buffer.byteLength(jsonContent, 'utf8');
      const timestamp = new Date().toISOString();
      
      logger.info(`📤 Archivo simulado guardado: ${fileName} (${fileSize} bytes)`);

      return {
        success: true,
        fileName,
        bucketName: this.bucketName,
        fileSize,
        uploadedAt: timestamp,
        metadata: metadata.metadata,
        gcsPath: `gs://${this.bucketName}/${fileName}`,
        gcsUri: `gs://${this.bucketName}/${fileName}`,
        localPath: filePath,
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error en upload simulado: ${fileName}`, error.message);
      throw error;
    }
  }

  /**
   * Lista archivos en GCS con filtros opcionales
   * @param {Object} options - Opciones de filtrado
   * @returns {Array} Lista de archivos
   */
  async listFiles(options = {}) {
    try {
      await this.initialize();

      const {
        prefix = '',
        maxResults = 100,
        dataType = null
      } = options;

      if (this.simulationMode) {
        return await this.listFilesSimulated(options);
      }

      // Listar archivos reales de GCS
      const [files] = await this.bucket.getFiles({
        prefix,
        maxResults
      });

      const fileList = [];

      for (const file of files) {
        try {
          const [metadata] = await file.getMetadata();
          
          // Filtrar por tipo de datos si se especifica
          if (dataType && metadata.metadata?.dataType !== dataType) {
            continue;
          }

          fileList.push({
            name: file.name,
            size: parseInt(metadata.size),
            created: metadata.timeCreated,
            updated: metadata.updated,
            contentType: metadata.contentType,
            metadata: metadata.metadata || {},
            gcsPath: `gs://${this.bucketName}/${file.name}`
          });

        } catch (metadataError) {
          logger.warn(`⚠️ Error obteniendo metadata de ${file.name}:`, metadataError.message);
        }
      }

      logger.info(`📋 Listados ${fileList.length} archivos de GCS`);

      return fileList;

    } catch (error) {
      logger.error('❌ Error listando archivos de GCS:', error.message);
      throw error;
    }
  }

  /**
   * Simula listado de archivos localmente
   */
  async listFilesSimulated(options = {}) {
    try {
      const { prefix = '', dataType = null } = options;
      
      // Verificar si el directorio existe
      const exists = await FileUtils.pathExists(this.localStoragePath);
      if (!exists) {
        return [];
      }
      
      const files = await fs.readdir(this.localStoragePath);
      const dataFiles = files.filter(file => 
        !file.endsWith('.metadata.json') && 
        file.startsWith(prefix)
      );

      const fileList = [];

      for (const fileName of dataFiles) {
        try {
          const filePath = path.join(this.localStoragePath, fileName);
          const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
          
          const stats = await fs.stat(filePath);
          let metadata = {};
          
          try {
            const metadataContent = await fs.readFile(metadataPath, 'utf8');
            const metadataObj = JSON.parse(metadataContent);
            metadata = metadataObj.metadata || {};
          } catch (metadataError) {
            logger.warn(`⚠️ No se pudo leer metadata de ${fileName}`);
          }

          // Filtrar por tipo de datos si se especifica
          if (dataType && metadata.dataType !== dataType) {
            continue;
          }

          fileList.push({
            name: fileName,
            size: stats.size,
            created: stats.birthtime.toISOString(),
            updated: stats.mtime.toISOString(),
            contentType: 'application/json',
            metadata,
            gcsPath: `gs://${this.bucketName}/${fileName}`,
            localPath: filePath,
            simulated: true
          });

        } catch (fileError) {
          logger.warn(`⚠️ Error procesando archivo ${fileName}:`, fileError.message);
        }
      }

      logger.info(`📋 Listados ${fileList.length} archivos simulados`);

      return fileList;

    } catch (error) {
      logger.error('❌ Error listando archivos simulados:', error.message);
      throw error;
    }
  }

  /**
   * Descarga un archivo desde GCS
   * @param {string} fileName - Nombre del archivo
   * @returns {Object} Contenido del archivo y metadata
   */
  async downloadFile(fileName) {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return await this.downloadFileSimulated(fileName);
      }

      // Descarga real desde GCS
      const file = this.bucket.file(fileName);
      
      const [exists] = await file.exists();
      if (!exists) {
        throw new Error(`Archivo no encontrado: ${fileName}`);
      }

      const [content] = await file.download();
      const [metadata] = await file.getMetadata();

      logger.info(`📥 Archivo descargado de GCS: ${fileName}`);

      return {
        success: true,
        fileName,
        content: content.toString('utf8'),
        metadata: metadata.metadata || {},
        size: parseInt(metadata.size),
        contentType: metadata.contentType
      };

    } catch (error) {
      logger.error(`❌ Error descargando archivo de GCS: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Simula descarga de archivo localmente
   */
  async downloadFileSimulated(fileName) {
    try {
      const filePath = path.join(this.localStoragePath, fileName);
      const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
      
      const exists = await FileUtils.pathExists(filePath);
      if (!exists) {
        throw new Error(`Archivo no encontrado: ${fileName}`);
      }

      const content = await fs.readFile(filePath, 'utf8');
      const stats = await fs.stat(filePath);
      
      let metadata = {};
      try {
        const metadataContent = await fs.readFile(metadataPath, 'utf8');
        const metadataObj = JSON.parse(metadataContent);
        metadata = metadataObj.metadata || {};
      } catch (metadataError) {
        logger.warn(`⚠️ No se pudo leer metadata de ${fileName}`);
      }

      logger.info(`📥 Archivo descargado (simulado): ${fileName}`);

      return {
        success: true,
        fileName,
        content,
        metadata,
        size: stats.size,
        contentType: 'application/json',
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error descargando archivo simulado: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Verifica si un archivo existe en GCS
   * @param {string} fileName - Nombre del archivo
   * @returns {Object} Resultado de la verificación
   */
  async fileExists(fileName) {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return await this.fileExistsSimulated(fileName);
      }

      // Verificación real en GCS
      const file = this.bucket.file(fileName);
      const [exists] = await file.exists();

      if (exists) {
        const [metadata] = await file.getMetadata();
        return {
          exists: true,
          fileName,
          size: parseInt(metadata.size),
          created: metadata.timeCreated,
          updated: metadata.updated,
          metadata: metadata.metadata || {},
          gcsPath: `gs://${this.bucketName}/${fileName}`
        };
      }

      return {
        exists: false,
        fileName
      };

    } catch (error) {
      logger.error(`❌ Error verificando existencia de archivo en GCS: ${fileName}`, error.message);
      return {
        exists: false,
        fileName,
        error: error.message
      };
    }
  }

  /**
   * Simula verificación de existencia de archivo localmente
   */
  async fileExistsSimulated(fileName) {
    try {
      const filePath = path.join(this.localStoragePath, fileName);
      const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
      
      const exists = await FileUtils.pathExists(filePath);
      
      if (exists) {
        const stats = await fs.stat(filePath);
        
        let metadata = {};
        try {
          const metadataContent = await fs.readFile(metadataPath, 'utf8');
          const metadataObj = JSON.parse(metadataContent);
          metadata = metadataObj.metadata || {};
        } catch (metadataError) {
          // Ignorar error si no existe metadata
        }

        return {
          exists: true,
          fileName,
          size: stats.size,
          created: stats.birthtime.toISOString(),
          updated: stats.mtime.toISOString(),
          metadata,
          gcsPath: `gs://${this.bucketName}/${fileName}`,
          localPath: filePath,
          simulated: true
        };
      }

      return {
        exists: false,
        fileName,
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error verificando existencia de archivo simulado: ${fileName}`, error.message);
      return {
        exists: false,
        fileName,
        error: error.message,
        simulated: true
      };
    }
  }

  /**
   * Elimina un archivo de GCS
   * @param {string} fileName - Nombre del archivo
   * @returns {Object} Resultado de la operación
   */
  async deleteFile(fileName) {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return await this.deleteFileSimulated(fileName);
      }

      // Eliminación real de GCS
      const file = this.bucket.file(fileName);
      
      const [exists] = await file.exists();
      if (!exists) {
        logger.warn(`⚠️ Archivo no encontrado para eliminar: ${fileName}`);
        return {
          success: true,
          fileName,
          message: 'File not found, considered deleted'
        };
      }

      await file.delete();
      
      logger.info(`🗑️ Archivo eliminado de GCS: ${fileName}`);

      return {
        success: true,
        fileName,
        deletedAt: new Date().toISOString()
      };

    } catch (error) {
      logger.error(`❌ Error eliminando archivo de GCS: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Simula eliminación de archivo localmente
   */
  async deleteFileSimulated(fileName) {
    try {
      const filePath = path.join(this.localStoragePath, fileName);
      const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
      
      const exists = await FileUtils.pathExists(filePath);
      if (!exists) {
        logger.warn(`⚠️ Archivo simulado no encontrado para eliminar: ${fileName}`);
        return {
          success: true,
          fileName,
          message: 'File not found, considered deleted',
          simulated: true
        };
      }

      // Eliminar archivo de datos
      await fs.unlink(filePath);
      
      // Eliminar metadata si existe
      try {
        await fs.unlink(metadataPath);
      } catch (metadataError) {
        // Ignorar error si no existe metadata
      }

      logger.info(`🗑️ Archivo simulado eliminado: ${fileName}`);

      return {
        success: true,
        fileName,
        deletedAt: new Date().toISOString(),
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error eliminando archivo simulado: ${fileName}`, error.message);
      return {
        success: false,
        error: error.message,
        fileName
      };
    }
  }

  /**
   * Obtiene el prefijo de carpeta para un tipo de datos
   * @param {string} dataType - Tipo de datos (gps, mobile)
   * @returns {string} Prefijo de carpeta
   */
  getDataTypePrefix(dataType) {
    const prefixes = {
      gps: process.env.GCS_GPS_PREFIX || 'gps-data/',
      mobile: process.env.GCS_MOBILE_PREFIX || 'mobile-data/'
    };
    
    return prefixes[dataType] || `${dataType}/`;
  }

  /**
   * Genera nombre de archivo único para upload
   * @param {string} dataType - Tipo de datos (gps, mobile)
   * @param {string} processingId - ID de procesamiento
   * @returns {string} Nombre de archivo único
   */
  generateFileName(dataType, processingId) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    // Usar prefijos configurados en .env
    const prefix = this.getDataTypePrefix(dataType);
    return `${prefix}${timestamp}_${processingId}.json`;
  }

  /**
   * Genera nombre de archivo determinístico basado en contenido
   * @param {string} dataType - Tipo de datos (gps, mobile)
   * @param {string} processingId - ID de procesamiento
   * @param {string} contentHash - Hash del contenido (opcional)
   * @returns {string} Nombre de archivo determinístico
   */
  generateDeterministicFileName(dataType, processingId, contentHash = null) {
    const baseId = contentHash || processingId;
    const timestamp = new Date().toISOString().split('T')[0]; // Solo fecha YYYY-MM-DD
    return `${dataType}/${timestamp}_${baseId}.json`;
  }

  /**
   * Obtiene estadísticas del bucket
   * @returns {Object} Estadísticas del bucket
   */
  async getBucketStats() {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return await this.getBucketStatsSimulated();
      }

      // Estadísticas reales del bucket
      const [files] = await this.bucket.getFiles();
      
      let totalSize = 0;
      let filesByType = {};
      
      for (const file of files) {
        try {
          const [metadata] = await file.getMetadata();
          totalSize += parseInt(metadata.size || 0);
          
          const dataType = metadata.metadata?.dataType || 'unknown';
          filesByType[dataType] = (filesByType[dataType] || 0) + 1;
          
        } catch (metadataError) {
          logger.warn(`⚠️ Error obteniendo metadata de ${file.name}`);
        }
      }

      return {
        bucketName: this.bucketName,
        totalFiles: files.length,
        totalSize,
        filesByType,
        simulated: false
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas del bucket:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas simuladas
   */
  async getBucketStatsSimulated() {
    try {
      // Verificar si el directorio existe
      const exists = await FileUtils.pathExists(this.localStoragePath);
      if (!exists) {
        return {
          bucketName: this.bucketName,
          totalFiles: 0,
          totalSize: 0,
          filesByType: {},
          simulated: true
        };
      }
      
      const files = await fs.readdir(this.localStoragePath);
      const dataFiles = files.filter(file => !file.endsWith('.metadata.json'));
      
      let totalSize = 0;
      let filesByType = {};
      
      for (const fileName of dataFiles) {
        try {
          const filePath = path.join(this.localStoragePath, fileName);
          const metadataPath = path.join(this.localStoragePath, `${fileName}.metadata.json`);
          
          const stats = await fs.stat(filePath);
          totalSize += stats.size;
          
          let dataType = 'unknown';
          try {
            const metadataContent = await fs.readFile(metadataPath, 'utf8');
            const metadata = JSON.parse(metadataContent);
            dataType = metadata.metadata?.dataType || 'unknown';
          } catch (metadataError) {
            // Usar unknown si no hay metadata
          }
          
          filesByType[dataType] = (filesByType[dataType] || 0) + 1;
          
        } catch (fileError) {
          logger.warn(`⚠️ Error procesando archivo ${fileName}`);
        }
      }

      return {
        bucketName: this.bucketName,
        totalFiles: dataFiles.length,
        totalSize,
        filesByType,
        simulated: true
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas simuladas:', error.message);
      throw error;
    }
  }

  /**
   * Verifica el estado del adaptador
   * @returns {Object} Estado del adaptador
   */
  async getStatus() {
    try {
      const status = {
        initialized: this.isInitialized,
        simulationMode: this.simulationMode,
        bucketName: this.bucketName,
        projectId: this.projectId
      };

      if (this.simulationMode) {
        status.localStoragePath = this.localStoragePath;
        status.localStorageExists = await FileUtils.pathExists(this.localStoragePath);
      } else {
        status.keyFilename = this.keyFilename;
        status.credentialsExist = await FileUtils.pathExists(this.keyFilename);
        
        if (this.isInitialized && this.bucket) {
          try {
            const [exists] = await this.bucket.exists();
            status.bucketExists = exists;
          } catch (bucketError) {
            status.bucketExists = false;
            status.bucketError = bucketError.message;
          }
        }
      }

      return status;

    } catch (error) {
      logger.error('❌ Error obteniendo estado del GCS Adapter:', error.message);
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
      logger.info('🧹 Limpiando recursos del GCS Adapter...');
      
      this.storage = null;
      this.bucket = null;
      this.isInitialized = false;
      
      logger.info('✅ Recursos del GCS Adapter limpiados');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del GCS Adapter:', error.message);
    }
  }
}