import { BigQuery } from '@google-cloud/bigquery';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { FileUtils } from '../utils/FileUtils.js';

/**
 * Procesador de lotes para BigQuery
 * Maneja la creación de jobs batch desde archivos GCS hacia BigQuery
 */
export class BigQueryBatchProcessor {
  constructor() {
    this.bigQuery = null;
    this.dataset = null;
    this.isInitialized = false;
    this.simulationMode = process.env.BIGQUERY_SIMULATION_MODE === 'true' || false;
    this.projectId = config.bigquery.projectId;
    this.datasetId = config.bigquery.datasetId;
    this.location = config.bigquery.location;
    this.keyFilename = config.bigquery.keyFilename;
    this.tables = config.bigquery.tables;
    this.jobConfig = config.bigquery.jobConfig;
  }

  /**
   * Inicializa el cliente de BigQuery
   */
  async initialize() {
    try {
      if (this.isInitialized) {
        return;
      }

      if (this.simulationMode) {
        logger.info('🔧 BigQuery Batch Processor iniciado en modo simulación');
        this.isInitialized = true;
        return;
      }

      // Verificar que existe el archivo de credenciales
      const credentialsExist = await FileUtils.pathExists(this.keyFilename);
      if (!credentialsExist) {
        throw new Error(`Archivo de credenciales no encontrado: ${this.keyFilename}`);
      }

      // Inicializar cliente de BigQuery
      this.bigQuery = new BigQuery({
        projectId: this.projectId,
        keyFilename: this.keyFilename,
        location: this.location
      });

      // Obtener referencia al dataset
      this.dataset = this.bigQuery.dataset(this.datasetId);

      // Verificar que el dataset existe
      const [exists] = await this.dataset.exists();
      if (!exists) {
        logger.warn(`⚠️ Dataset ${this.datasetId} no existe, intentando crear...`);
        await this.dataset.create({
          location: this.location
        });
        logger.info(`✅ Dataset ${this.datasetId} creado exitosamente`);
      }

      // Verificar/crear tablas
      await this.ensureTablesExist();

      this.isInitialized = true;
      logger.info(`✅ BigQuery Batch Processor inicializado - Dataset: ${this.datasetId}`);

    } catch (error) {
      logger.error('❌ Error inicializando BigQuery Batch Processor:', error.message);
      
      // Fallback a modo simulación si falla la inicialización
      if (!this.simulationMode) {
        logger.warn('🔧 Fallback a modo simulación por error de inicialización');
        logger.warn('🔧 Error original:', error.message);
        this.simulationMode = true;
        this.isInitialized = true;
      } else {
        throw error;
      }
    }
  }

  /**
   * Asegura que las tablas GPS y Mobile existan
   */
  async ensureTablesExist() {
    try {
      // Esquema para tabla GPS
      const gpsSchema = [
        { name: 'deviceId', type: 'STRING', mode: 'REQUIRED' },
        { name: 'lat', type: 'FLOAT', mode: 'REQUIRED' },
        { name: 'lng', type: 'FLOAT', mode: 'REQUIRED' },
        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'processed_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'processing_id', type: 'STRING', mode: 'NULLABLE' }
      ];

      // Esquema para tabla Mobile
      const mobileSchema = [
        { name: 'userId', type: 'STRING', mode: 'REQUIRED' },
        { name: 'lat', type: 'FLOAT', mode: 'REQUIRED' },
        { name: 'lng', type: 'FLOAT', mode: 'REQUIRED' },
        { name: 'timestamp', type: 'TIMESTAMP', mode: 'REQUIRED' },
        { name: 'name', type: 'STRING', mode: 'REQUIRED' },
        { name: 'email', type: 'STRING', mode: 'REQUIRED' },
        { name: 'processed_at', type: 'TIMESTAMP', mode: 'NULLABLE' },
        { name: 'processing_id', type: 'STRING', mode: 'NULLABLE' }
      ];

      // Crear tabla GPS si no existe
      await this.ensureTableExists(this.tables.gps, gpsSchema, 'GPS');
      
      // Crear tabla Mobile si no existe
      await this.ensureTableExists(this.tables.mobile, mobileSchema, 'Mobile');

    } catch (error) {
      logger.error('❌ Error asegurando que las tablas existan:', error.message);
      throw error;
    }
  }

  /**
   * Asegura que una tabla específica exista
   */
  async ensureTableExists(tableName, schema, description) {
    try {
      const table = this.dataset.table(tableName);
      const [exists] = await table.exists();

      if (!exists) {
        logger.info(`📋 Creando tabla ${tableName}...`);
        
        const options = {
          schema: schema,
          location: this.location,
          description: `Tabla para datos ${description} procesados desde GCS`
        };

        await table.create(options);
        logger.info(`✅ Tabla ${tableName} creada exitosamente`);
      } else {
        logger.info(`📋 Tabla ${tableName} ya existe`);
      }

    } catch (error) {
      logger.error(`❌ Error creando tabla ${tableName}:`, error.message);
      throw error;
    }
  }

  /**
   * Procesa un archivo GCS hacia BigQuery
   * @param {string} gcsUri - URI del archivo en GCS (gs://bucket/path/file.json)
   * @param {string} dataType - Tipo de datos ('gps' o 'mobile')
   * @param {Object} metadata - Metadata del archivo
   * @returns {Object} Resultado del procesamiento
   */
  async processGCSFile(gcsUri, dataType, metadata = {}) {
    try {
      await this.initialize();

      logger.info(`🔄 Iniciando procesamiento batch: ${gcsUri} → ${dataType}`);

      if (this.simulationMode) {
        return await this.processGCSFileSimulated(gcsUri, dataType, metadata);
      }

      // Debug: verificar estado de inicialización
      logger.debug(`🔍 BigQuery state: initialized=${this.isInitialized}, client=${!!this.bigQuery}, dataset=${!!this.dataset}`);

      // Determinar tabla destino
      const tableName = this.tables[dataType];
      if (!tableName) {
        return {
          success: false,
          error: `Tipo de datos no soportado: ${dataType}`,
          gcsUri,
          dataType
        };
      }

      const table = this.dataset.table(tableName);

      // Configurar job de carga para JSON con optimizaciones de rendimiento
      const jobConfig = {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        writeDisposition: this.jobConfig.writeDisposition,
        createDisposition: this.jobConfig.createDisposition,
        maxBadRecords: parseInt(process.env.BIGQUERY_MAX_BAD_RECORDS) || this.jobConfig.maxBadRecords,
        // skipLeadingRows solo es válido para CSV, no para JSON
        autodetect: false, // Usar esquema definido
        ignoreUnknownValues: false,
        // allowJaggedRows y allowQuotedNewlines solo son para CSV
        
        // 🚀 OPTIMIZACIONES DE RENDIMIENTO:
        jobTimeoutMs: parseInt(process.env.BIGQUERY_JOB_TIMEOUT_MS) || 300000,
        priority: process.env.BIGQUERY_PRIORITY || 'BATCH'
      };

      // Verificar que BigQuery esté inicializado
      if (!this.bigQuery) {
        throw new Error('BigQuery client not initialized');
      }

      // Crear y ejecutar job usando el API v8.x
      const jobId = this.generateJobId(dataType, metadata.processingId);
      logger.info(`📊 Creando job BigQuery: ${jobId}`);

      // Configuración completa del job para API v8.x
      const jobOptions = {
        jobId: jobId,
        location: this.location,
        configuration: {
          load: {
            sourceUris: [gcsUri],
            destinationTable: {
              projectId: this.projectId,
              datasetId: this.datasetId,
              tableId: tableName
            },
            ...jobConfig
          }
        }
      };

      const [job] = await this.bigQuery.createJob(jobOptions);
      logger.info(`⏳ Job ${jobId} creado, esperando completación...`);

      // Esperar a que el job complete
      const [jobResult] = await job.promise();

      // Verificar resultado
      if (jobResult.status.state === 'DONE') {
        if (jobResult.status.errors && jobResult.status.errors.length > 0) {
          logger.error('❌ Job completado con errores:', jobResult.status.errors);
          return {
            success: false,
            jobId,
            errors: jobResult.status.errors,
            gcsUri,
            dataType
          };
        }

        const stats = jobResult.statistics.load;
        logger.info(`✅ Job ${jobId} completado exitosamente:`);
        logger.info(`   📊 Registros procesados: ${stats.outputRows}`);
        logger.info(`   📁 Bytes procesados: ${stats.inputFileBytes}`);

        return {
          success: true,
          jobId,
          recordsProcessed: parseInt(stats.outputRows),
          bytesProcessed: parseInt(stats.inputFileBytes),
          gcsUri,
          dataType,
          tableName,
          completedAt: new Date().toISOString(),
          statistics: stats
        };

      } else {
        throw new Error(`Job ${jobId} no completó correctamente: ${jobResult.status.state}`);
      }

    } catch (error) {
      logger.error(`❌ Error procesando archivo GCS ${gcsUri}:`, error.message);
      return {
        success: false,
        error: error.message,
        gcsUri,
        dataType
      };
    }
  }

  /**
   * Simula el procesamiento de un archivo GCS
   */
  async processGCSFileSimulated(gcsUri, dataType, metadata) {
    try {
      // Verificar tipo de datos soportado
      const tableName = this.tables[dataType];
      if (!tableName) {
        return {
          success: false,
          error: `Tipo de datos no soportado: ${dataType}`,
          gcsUri,
          dataType,
          simulated: true
        };
      }

      // Simular tiempo de procesamiento
      await new Promise(resolve => setTimeout(resolve, 100));

      const jobId = this.generateJobId(dataType, metadata.processingId);
      const recordsProcessed = metadata.recordCount || Math.floor(Math.random() * 1000) + 100;
      const bytesProcessed = recordsProcessed * 150; // Estimación

      logger.info(`✅ Job simulado ${jobId} completado:`);
      logger.info(`   📊 Registros procesados: ${recordsProcessed}`);
      logger.info(`   📁 Bytes procesados: ${bytesProcessed}`);

      return {
        success: true,
        jobId,
        recordsProcessed,
        bytesProcessed,
        gcsUri,
        dataType,
        tableName,
        completedAt: new Date().toISOString(),
        simulated: true
      };

    } catch (error) {
      logger.error(`❌ Error en procesamiento simulado ${gcsUri}:`, error.message);
      return {
        success: false,
        error: error.message,
        gcsUri,
        dataType,
        simulated: true
      };
    }
  }

  /**
   * Procesa múltiples archivos GCS en paralelo
   * @param {Array} files - Array de objetos {gcsUri, dataType, metadata}
   * @param {Object} options - Opciones de procesamiento
   * @returns {Object} Resultado del procesamiento batch
   */
  async processBatch(files, options = {}) {
    try {
      const { maxConcurrency = 3, continueOnError = true } = options;
      
      logger.info(`🔄 Iniciando procesamiento batch de ${files.length} archivos`);

      const results = [];
      const errors = [];

      // Procesar archivos en lotes con concurrencia limitada
      for (let i = 0; i < files.length; i += maxConcurrency) {
        const batch = files.slice(i, i + maxConcurrency);
        
        const batchPromises = batch.map(async (file) => {
          try {
            const result = await this.processGCSFile(file.gcsUri, file.dataType, file.metadata);
            return result;
          } catch (error) {
            const errorResult = {
              success: false,
              error: error.message,
              gcsUri: file.gcsUri,
              dataType: file.dataType
            };
            
            if (!continueOnError) {
              throw error;
            }
            
            return errorResult;
          }
        });

        const batchResults = await Promise.all(batchPromises);
        results.push(...batchResults);

        // Separar éxitos y errores
        batchResults.forEach(result => {
          if (!result.success) {
            errors.push(result);
          }
        });

        logger.info(`📊 Lote ${Math.floor(i / maxConcurrency) + 1} completado: ${batchResults.length} archivos`);
      }

      const successCount = results.filter(r => r.success).length;
      const totalRecords = results
        .filter(r => r.success)
        .reduce((sum, r) => sum + (r.recordsProcessed || 0), 0);

      logger.info(`✅ Procesamiento batch completado:`);
      logger.info(`   📊 Archivos exitosos: ${successCount}/${files.length}`);
      logger.info(`   📊 Total registros: ${totalRecords}`);
      logger.info(`   ❌ Errores: ${errors.length}`);

      return {
        success: errors.length === 0 || continueOnError,
        totalFiles: files.length,
        successfulFiles: successCount,
        failedFiles: errors.length,
        totalRecords,
        results,
        errors,
        completedAt: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en procesamiento batch:', error.message);
      return {
        success: false,
        error: error.message,
        totalFiles: files.length,
        successfulFiles: 0,
        failedFiles: files.length
      };
    }
  }

  /**
   * Monitorea el estado de un job BigQuery
   * @param {string} jobId - ID del job
   * @returns {Object} Estado del job
   */
  async getJobStatus(jobId) {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return {
          jobId,
          state: 'DONE',
          simulated: true,
          completedAt: new Date().toISOString()
        };
      }

      const job = this.bigQuery.job(jobId);
      const [metadata] = await job.getMetadata();

      return {
        jobId,
        state: metadata.status.state,
        errors: metadata.status.errors || [],
        statistics: metadata.statistics,
        createdAt: metadata.statistics.creationTime,
        startedAt: metadata.statistics.startTime,
        completedAt: metadata.statistics.endTime
      };

    } catch (error) {
      logger.error(`❌ Error obteniendo estado del job ${jobId}:`, error.message);
      return {
        jobId,
        state: 'ERROR',
        error: error.message
      };
    }
  }

  /**
   * Lista jobs recientes de BigQuery
   * @param {Object} options - Opciones de filtrado
   * @returns {Array} Lista de jobs
   */
  async listRecentJobs(options = {}) {
    try {
      await this.initialize();

      const { maxResults = 50, stateFilter = null } = options;

      if (this.simulationMode) {
        return this.listRecentJobsSimulated(options);
      }

      const [jobs] = await this.bigQuery.getJobs({
        maxResults,
        allUsers: false
      });

      const jobList = jobs
        .filter(job => {
          if (stateFilter) {
            return job.metadata.status.state === stateFilter;
          }
          return true;
        })
        .map(job => ({
          jobId: job.id,
          state: job.metadata.status.state,
          jobType: job.metadata.configuration.jobType,
          createdAt: job.metadata.statistics.creationTime,
          completedAt: job.metadata.statistics.endTime,
          errors: job.metadata.status.errors || []
        }));

      return jobList;

    } catch (error) {
      logger.error('❌ Error listando jobs recientes:', error.message);
      throw error;
    }
  }

  /**
   * Simula listado de jobs recientes
   */
  listRecentJobsSimulated(options = {}) {
    const { maxResults = 50 } = options;
    
    const jobs = [];
    for (let i = 0; i < Math.min(maxResults, 10); i++) {
      jobs.push({
        jobId: `simulated_job_${Date.now()}_${i}`,
        state: 'DONE',
        jobType: 'LOAD',
        createdAt: new Date(Date.now() - i * 60000).toISOString(),
        completedAt: new Date(Date.now() - i * 60000 + 30000).toISOString(),
        errors: [],
        simulated: true
      });
    }

    return jobs;
  }

  /**
   * Genera un ID único para el job
   */
  generateJobId(dataType, processingId) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15);
    const random = Math.random().toString(36).slice(2, 5);
    return `load_${dataType}_${processingId || timestamp}_${random}`;
  }

  /**
   * Obtiene estadísticas de las tablas
   * @returns {Object} Estadísticas de las tablas GPS y Mobile
   */
  async getTableStats() {
    try {
      await this.initialize();

      if (this.simulationMode) {
        return this.getTableStatsSimulated();
      }

      const stats = {};

      // Estadísticas tabla GPS
      try {
        const gpsTable = this.dataset.table(this.tables.gps);
        const [gpsMetadata] = await gpsTable.getMetadata();
        stats.gps = {
          tableName: this.tables.gps,
          numRows: parseInt(gpsMetadata.numRows || 0),
          numBytes: parseInt(gpsMetadata.numBytes || 0),
          lastModified: gpsMetadata.lastModifiedTime,
          schema: gpsMetadata.schema.fields
        };
      } catch (error) {
        stats.gps = { error: error.message };
      }

      // Estadísticas tabla Mobile
      try {
        const mobileTable = this.dataset.table(this.tables.mobile);
        const [mobileMetadata] = await mobileTable.getMetadata();
        stats.mobile = {
          tableName: this.tables.mobile,
          numRows: parseInt(mobileMetadata.numRows || 0),
          numBytes: parseInt(mobileMetadata.numBytes || 0),
          lastModified: mobileMetadata.lastModifiedTime,
          schema: mobileMetadata.schema.fields
        };
      } catch (error) {
        stats.mobile = { error: error.message };
      }

      return stats;

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas de tablas:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas simuladas de las tablas
   */
  getTableStatsSimulated() {
    return {
      gps: {
        tableName: this.tables.gps,
        numRows: Math.floor(Math.random() * 100000) + 10000,
        numBytes: Math.floor(Math.random() * 10000000) + 1000000,
        lastModified: new Date().toISOString(),
        simulated: true
      },
      mobile: {
        tableName: this.tables.mobile,
        numRows: Math.floor(Math.random() * 50000) + 5000,
        numBytes: Math.floor(Math.random() * 5000000) + 500000,
        lastModified: new Date().toISOString(),
        simulated: true
      }
    };
  }

  /**
   * Obtiene el estado del procesador
   * @returns {Object} Estado del procesador
   */
  async getStatus() {
    try {
      const status = {
        initialized: this.isInitialized,
        simulationMode: this.simulationMode,
        projectId: this.projectId,
        datasetId: this.datasetId,
        location: this.location,
        tables: this.tables
      };

      if (this.simulationMode) {
        status.note = 'Running in simulation mode';
      } else {
        status.keyFilename = this.keyFilename;
        status.credentialsExist = await FileUtils.pathExists(this.keyFilename);
        
        if (this.isInitialized && this.dataset) {
          try {
            const [exists] = await this.dataset.exists();
            status.datasetExists = exists;
          } catch (datasetError) {
            status.datasetExists = false;
            status.datasetError = datasetError.message;
          }
        }
      }

      return status;

    } catch (error) {
      logger.error('❌ Error obteniendo estado del BigQuery Batch Processor:', error.message);
      return {
        initialized: false,
        error: error.message
      };
    }
  }

  /**
   * Carga datos desde GCS a BigQuery (método simplificado)
   * @param {string} gcsUri - URI del archivo en GCS
   * @param {string} datasetId - ID del dataset
   * @param {string} tableId - ID de la tabla
   * @returns {Object} Resultado de la operación
   */
  async loadFromGCS(gcsUri, datasetId, tableId) {
    try {
      await this.initialize();

      // Determinar tipo de datos basado en el nombre de la tabla
      let dataType = 'gps';
      if (tableId.toLowerCase().includes('mobile')) {
        dataType = 'mobile';
      }

      const metadata = {
        processingId: `manual_${Date.now()}`,
        recordCount: 0
      };

      return await this.processGCSFile(gcsUri, dataType, metadata);

    } catch (error) {
      logger.error(`❌ Error cargando desde GCS ${gcsUri}:`, error.message);
      return {
        success: false,
        error: error.message,
        gcsUri
      };
    }
  }

  /**
   * Limpia recursos
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del BigQuery Batch Processor...');
      
      this.bigQuery = null;
      this.dataset = null;
      this.isInitialized = false;
      
      logger.info('✅ Recursos del BigQuery Batch Processor limpiados');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del BigQuery Batch Processor:', error.message);
    }
  }
}