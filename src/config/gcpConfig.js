import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

// Cargar variables de entorno
dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * Configuración y validación de credenciales GCP
 */
export class GCPConfig {
  constructor() {
    this.simulationMode = process.env.GCP_SIMULATION_MODE === 'true';
    this.serviceAccountPath = this.resolveServiceAccountPath();
    this.config = this.buildConfig();
  }

  /**
   * Resuelve la ruta del archivo service-account.json
   */
  resolveServiceAccountPath() {
    const possiblePaths = [
      process.env.GOOGLE_APPLICATION_CREDENTIALS,
      process.env.GCS_KEY_FILE,
      process.env.BIGQUERY_KEY_FILE,
      './service-account.json',
      'service-account.json'
    ].filter(Boolean);

    for (const credPath of possiblePaths) {
      const resolvedPath = path.resolve(credPath);
      if (fs.existsSync(resolvedPath)) {
        return resolvedPath;
      }
    }

    return null;
  }

  /**
   * Valida las credenciales de GCP
   */
  validateCredentials() {
    if (this.simulationMode) {
      console.log('🔧 Modo simulación GCP activado - omitiendo validación de credenciales');
      return {
        valid: true,
        mode: 'simulation',
        message: 'Ejecutando en modo simulación'
      };
    }

    if (!this.serviceAccountPath) {
      return {
        valid: false,
        mode: 'missing',
        message: 'Archivo service-account.json no encontrado. Rutas verificadas: ' +
          [
            process.env.GOOGLE_APPLICATION_CREDENTIALS,
            './service-account.json',
            'service-account.json'
          ].filter(Boolean).join(', ')
      };
    }

    try {
      const serviceAccount = JSON.parse(fs.readFileSync(this.serviceAccountPath, 'utf8'));

      // Validar estructura básica del service account
      const requiredFields = ['type', 'project_id', 'private_key', 'client_email'];
      const missingFields = requiredFields.filter(field => !serviceAccount[field]);

      if (missingFields.length > 0) {
        return {
          valid: false,
          mode: 'invalid',
          message: `Service account inválido. Campos faltantes: ${missingFields.join(', ')}`
        };
      }

      if (serviceAccount.type !== 'service_account') {
        return {
          valid: false,
          mode: 'invalid',
          message: 'El archivo no es un service account válido'
        };
      }

      return {
        valid: true,
        mode: 'production',
        message: `Credenciales válidas para proyecto: ${serviceAccount.project_id}`,
        projectId: serviceAccount.project_id,
        clientEmail: serviceAccount.client_email
      };

    } catch (error) {
      return {
        valid: false,
        mode: 'error',
        message: `Error al leer service-account.json: ${error.message}`
      };
    }
  }

  /**
   * Construye la configuración completa de GCP
   */
  buildConfig() {
    const baseConfig = {
      // Configuración de proyecto
      projectId: process.env.GCP_PROJECT_ID || process.env.BIGQUERY_PROJECT_ID || '',

      // Configuración de GCS
      gcs: {
        bucketName: process.env.GCS_BUCKET_NAME || 'gps-mobile-data-bucket',
        region: process.env.GCS_REGION || 'us-central1',
        prefixes: {
          gps: process.env.GCS_GPS_PREFIX || 'gps-data/',
          mobile: process.env.GCS_MOBILE_PREFIX || 'mobile-data/'
        },
        keyFilename: this.serviceAccountPath
      },

      // Configuración de BigQuery
      bigQuery: {
        projectId: process.env.GCP_PROJECT_ID || process.env.BIGQUERY_PROJECT_ID || '',
        datasetId: process.env.BIGQUERY_DATASET_ID || 'location_data',
        location: process.env.BIGQUERY_LOCATION || 'US',
        tables: {
          gps: process.env.BIGQUERY_GPS_TABLE_ID || 'gps_records',
          mobile: process.env.BIGQUERY_MOBILE_TABLE_ID || 'mobile_records'
        },
        batchSize: parseInt(process.env.BIGQUERY_BATCH_SIZE) || 1000,
        keyFilename: this.serviceAccountPath,
        jobConfig: {
          writeDisposition: 'WRITE_APPEND',
          createDisposition: 'CREATE_IF_NEEDED',
          maxBadRecords: parseInt(process.env.BIGQUERY_MAX_BAD_RECORDS) || 10
        }
      },

      // Configuración de Redis para datos
      redis: {
        keyPatterns: {
          gps: process.env.REDIS_GPS_KEY_PATTERN || 'gps:last:*',
          mobile: process.env.REDIS_MOBILE_KEY_PATTERN || 'mobile:last:*'
        }
      },

      // Configuración de recovery
      recovery: {
        enabled: process.env.GCS_RECOVERY_ENABLED === 'true',
        maxRetryAttempts: parseInt(process.env.GCS_MAX_RETRY_ATTEMPTS) || 3,
        cleanupProcessedFiles: process.env.GCS_CLEANUP_PROCESSED_FILES === 'true'
      }
    };

    // Configuración específica para modo simulación
    if (this.simulationMode) {
      return {
        ...baseConfig,
        simulation: {
          enabled: true,
          paths: {
            gcs: process.env.GCS_LOCAL_SIMULATION_PATH || 'tmp/gcs-simulation/',
            bigQuery: process.env.BIGQUERY_LOCAL_SIMULATION_PATH || 'tmp/bigquery-simulation/'
          }
        },
        gcs: {
          ...baseConfig.gcs,
          keyFilename: null // No usar credenciales en simulación
        },
        bigQuery: {
          ...baseConfig.bigQuery,
          keyFilename: null // No usar credenciales en simulación
        }
      };
    }

    return baseConfig;
  }

  /**
   * Inicializa directorios para modo simulación
   */
  initializeSimulationDirectories() {
    if (!this.simulationMode) return;

    const dirs = [
      this.config.simulation.paths.gcs,
      this.config.simulation.paths.bigQuery,
      path.join(this.config.simulation.paths.gcs, this.config.gcs.prefixes.gps),
      path.join(this.config.simulation.paths.gcs, this.config.gcs.prefixes.mobile)
    ];

    dirs.forEach(dir => {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
        console.log(`📁 Directorio de simulación creado: ${dir}`);
      }
    });
  }

  /**
   * Obtiene la configuración completa
   */
  getConfig() {
    return this.config;
  }

  /**
   * Verifica si está en modo simulación
   */
  isSimulationMode() {
    return this.simulationMode;
  }

  /**
   * Obtiene información de estado de la configuración
   */
  getStatus() {
    const validation = this.validateCredentials();

    return {
      simulationMode: this.simulationMode,
      credentialsValid: validation.valid,
      credentialsMode: validation.mode,
      credentialsMessage: validation.message,
      serviceAccountPath: this.serviceAccountPath,
      projectId: this.config.projectId,
      gcs: {
        bucketName: this.config.gcs.bucketName,
        region: this.config.gcs.region,
        prefixes: this.config.gcs.prefixes
      },
      bigQuery: {
        datasetId: this.config.bigQuery.datasetId,
        location: this.config.bigQuery.location,
        tables: this.config.bigQuery.tables
      }
    };
  }

  /**
   * Valida configuración completa y lanza errores si es necesario
   */
  validateAndThrow() {
    const validation = this.validateCredentials();

    if (!validation.valid && !this.simulationMode) {
      throw new Error(`Configuración GCP inválida: ${validation.message}`);
    }

    // Validar configuración requerida
    const requiredConfig = [
      { key: 'projectId', value: this.config.projectId, name: 'GCP_PROJECT_ID' },
      { key: 'gcs.bucketName', value: this.config.gcs.bucketName, name: 'GCS_BUCKET_NAME' }
    ];

    const missingConfig = requiredConfig.filter(cfg => !cfg.value);

    if (missingConfig.length > 0) {
      const missing = missingConfig.map(cfg => cfg.name).join(', ');
      throw new Error(`Variables de entorno requeridas faltantes: ${missing}`);
    }

    console.log(`✅ Configuración GCP válida (${validation.mode})`);
    if (validation.projectId) {
      console.log(`📋 Proyecto: ${validation.projectId}`);
    }

    return validation;
  }
}

/**
 * Instancia singleton de configuración GCP
 */
export const gcpConfig = new GCPConfig();

/**
 * Función de conveniencia para validar configuración
 */
export function validateGCPConfig() {
  return gcpConfig.validateAndThrow();
}

/**
 * Función de conveniencia para obtener configuración
 */
export function getGCPConfig() {
  return gcpConfig.getConfig();
}

/**
 * Función de conveniencia para obtener estado
 */
export function getGCPStatus() {
  return gcpConfig.getStatus();
}