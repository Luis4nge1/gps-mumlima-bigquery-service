import dotenv from 'dotenv';
import path from 'path';
import { gcpConfig, validateGCPConfig } from './gcpConfig.js';
import { logger } from '../utils/logger.js';

// Cargar variables de entorno
dotenv.config();

/**
 * Configuración centralizada del microservicio
 */
export const config = {
  // Configuración del servidor
  server: {
    port: parseInt(process.env.PORT) || 3000,
    host: process.env.HOST || 'localhost',
    environment: process.env.NODE_ENV || 'development'
  },

  // Configuración de Redis
  redis: {
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || null,
    db: parseInt(process.env.REDIS_DB) || 0,
    keyPrefix: process.env.REDIS_KEY_PREFIX || 'gps:',
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true
  },

  // Configuración GPS
  gps: {
    listKey: process.env.GPS_LIST_KEY || 'gps:history:global',
    batchSize: parseInt(process.env.GPS_BATCH_SIZE) || 1000,
    outputFilePath: process.env.GPS_OUTPUT_FILE || 'tmp/gps_data.txt',
    backupEnabled: process.env.GPS_BACKUP_ENABLED === 'true',
    backupPath: process.env.GPS_BACKUP_PATH || 'tmp/backup/',
    backupMaxFiles: parseInt(process.env.GPS_BACKUP_MAX_FILES) || 3,
    backupOnlyOnSuccess: process.env.GPS_BACKUP_ONLY_ON_SUCCESS !== 'false'
  },

  // Configuración de Mobile data
  mobile: {
    listKey: process.env.MOBILE_LIST_KEY || 'mobile:history:global',
    batchSize: parseInt(process.env.MOBILE_BATCH_SIZE) || 1000
  },

  // Configuración de Backup Local (para procesamiento atómico)
  backup: {
    maxRetries: parseInt(process.env.BACKUP_MAX_RETRIES) || 3,
    retentionHours: parseInt(process.env.BACKUP_RETENTION_HOURS) || 24,
    storagePath: process.env.BACKUP_STORAGE_PATH || 'tmp/atomic-backups/',
    cleanupIntervalMinutes: parseInt(process.env.BACKUP_CLEANUP_INTERVAL_MINUTES) || 60,
    atomicProcessingEnabled: process.env.ATOMIC_PROCESSING_ENABLED === 'true',
    atomicProcessingTimeoutMs: parseInt(process.env.ATOMIC_PROCESSING_TIMEOUT_MS) || 30000
  },

  // Configuración del scheduler
  scheduler: {
    intervalMinutes: parseInt(process.env.SCHEDULER_INTERVAL_MINUTES) || 5,
    enabled: process.env.SCHEDULER_ENABLED !== 'false',
    timezone: process.env.SCHEDULER_TIMEZONE || 'America/Lima',
    maxConcurrentJobs: parseInt(process.env.SCHEDULER_MAX_CONCURRENT) || 1
  },

  // Configuración de BigQuery
  bigquery: {
    projectId: process.env.BIGQUERY_PROJECT_ID || '',
    datasetId: process.env.BIGQUERY_DATASET_ID || 'location_data',
    location: process.env.BIGQUERY_LOCATION || 'US',
    keyFilename: process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS || 'service-account.json',
    tables: {
      gps: process.env.BIGQUERY_GPS_TABLE || 'gps_history',
      mobile: process.env.BIGQUERY_MOBILE_TABLE || 'mobile_history'
    },
    jobConfig: {
      writeDisposition: process.env.BIGQUERY_WRITE_DISPOSITION || 'WRITE_APPEND',
      createDisposition: process.env.BIGQUERY_CREATE_DISPOSITION || 'CREATE_IF_NEEDED',
      maxBadRecords: parseInt(process.env.BIGQUERY_MAX_BAD_RECORDS) || 0,
      skipLeadingRows: parseInt(process.env.BIGQUERY_SKIP_LEADING_ROWS) || 0
    }
  },

  // Configuración de GCS
  gcs: {
    projectId: process.env.GCS_PROJECT_ID || process.env.BIGQUERY_PROJECT_ID || '',
    bucketName: process.env.GCS_BUCKET_NAME || 'gps-data-bucket',
    keyFilename: process.env.GCS_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS || 'service-account.json'
  },

  // Configuración de logging
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    format: process.env.LOG_FORMAT || 'combined',
    file: process.env.LOG_FILE || null,
    maxSize: process.env.LOG_MAX_SIZE || '10m',
    maxFiles: parseInt(process.env.LOG_MAX_FILES) || 5
  },

  // Configuración de monitoreo
  monitoring: {
    enabled: process.env.MONITORING_ENABLED === 'true',
    metricsPort: parseInt(process.env.METRICS_PORT) || 9090,
    healthCheckPath: process.env.HEALTH_CHECK_PATH || '/health'
  },

  // Configuración de seguridad
  security: {
    apiKey: process.env.API_KEY || null,
    rateLimitWindow: parseInt(process.env.RATE_LIMIT_WINDOW) || 900000, // 15 min
    rateLimitMax: parseInt(process.env.RATE_LIMIT_MAX) || 100
  },

  // Configuración GCP (delegada a gcpConfig)
  gcp: gcpConfig.getConfig(),

  // Configuración BigQuery (acceso directo para compatibilidad)
  bigquery: gcpConfig.getConfig().bigQuery
};

/**
 * Valida la configuración requerida
 */
export function validateConfig() {
  const required = [
    'REDIS_HOST',
    'GPS_LIST_KEY'
  ];

  const missing = required.filter(key => !process.env[key]);
  
  if (missing.length > 0) {
    throw new Error(`Variables de entorno requeridas faltantes: ${missing.join(', ')}`);
  }

  // Validar configuración de backup local
  validateBackupConfig();

  // Validar configuración GCP
  try {
    validateGCPConfig();
  } catch (error) {
    logger.warn(`⚠️  Advertencia GCP: ${error.message}`);
    if (!gcpConfig.isSimulationMode()) {
      throw error;
    }
  }
}

/**
 * Valida la configuración de backup local
 */
function validateBackupConfig() {
  const backupConfig = config.backup;
  
  // Validar que maxRetries sea un número positivo
  if (backupConfig.maxRetries < 0) {
    throw new Error('BACKUP_MAX_RETRIES debe ser un número mayor o igual a 0');
  }
  
  // Validar que retentionHours sea un número positivo
  if (backupConfig.retentionHours <= 0) {
    throw new Error('BACKUP_RETENTION_HOURS debe ser un número mayor a 0');
  }
  
  // Validar que cleanupIntervalMinutes sea un número positivo
  if (backupConfig.cleanupIntervalMinutes <= 0) {
    throw new Error('BACKUP_CLEANUP_INTERVAL_MINUTES debe ser un número mayor a 0');
  }
  
  // Validar que atomicProcessingTimeoutMs sea un número positivo
  if (backupConfig.atomicProcessingTimeoutMs <= 0) {
    throw new Error('ATOMIC_PROCESSING_TIMEOUT_MS debe ser un número mayor a 0');
  }
  
  // Validar que storagePath sea una ruta válida
  if (!backupConfig.storagePath || backupConfig.storagePath.trim() === '') {
    throw new Error('BACKUP_STORAGE_PATH no puede estar vacío');
  }
  
  // Advertir si el procesamiento atómico está deshabilitado
  if (!backupConfig.atomicProcessingEnabled) {
    console.warn('⚠️  Advertencia: ATOMIC_PROCESSING_ENABLED está deshabilitado. El sistema usará el procesamiento legacy.');
  }
}

/**
 * Obtiene configuración específica por ambiente
 */
export function getEnvironmentConfig() {
  const env = config.server.environment;
  
  const envConfigs = {
    development: {
      logging: { level: 'debug' },
      scheduler: { intervalMinutes: 1 }
    },
    production: {
      logging: { level: 'info' },
      scheduler: { intervalMinutes: 5 }
    },
    test: {
      logging: { level: 'error' },
      scheduler: { enabled: false }
    }
  };

  return envConfigs[env] || {};
}