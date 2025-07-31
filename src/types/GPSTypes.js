/**
 * Definiciones de tipos y esquemas para datos GPS
 */

/**
 * Esquema base para un registro GPS
 */
export const GPSRecordSchema = {
  // Campos requeridos
  latitude: {
    type: 'number',
    required: true,
    min: -90,
    max: 90,
    description: 'Latitud en grados decimales'
  },
  longitude: {
    type: 'number',
    required: true,
    min: -180,
    max: 180,
    description: 'Longitud en grados decimales'
  },
  
  // Campos opcionales
  timestamp: {
    type: 'string',
    required: false,
    format: 'iso8601',
    description: 'Timestamp en formato ISO 8601'
  },
  speed: {
    type: 'number',
    required: false,
    min: 0,
    max: 500,
    description: 'Velocidad en km/h'
  },
  heading: {
    type: 'number',
    required: false,
    min: 0,
    max: 360,
    description: 'Rumbo en grados (0-360)'
  },
  altitude: {
    type: 'number',
    required: false,
    min: -500,
    max: 10000,
    description: 'Altitud en metros'
  },
  accuracy: {
    type: 'number',
    required: false,
    min: 0,
    description: 'Precisión en metros'
  },
  device_id: {
    type: 'string',
    required: false,
    maxLength: 100,
    description: 'Identificador del dispositivo'
  }
};

/**
 * Esquema para datos procesados de GPS
 */
export const ProcessedGPSRecordSchema = {
  ...GPSRecordSchema,
  
  // Campos agregados durante el procesamiento
  id: {
    type: 'string',
    required: true,
    description: 'ID único del registro'
  },
  processed_at: {
    type: 'string',
    required: true,
    format: 'iso8601',
    description: 'Timestamp de procesamiento'
  },
  validated_at: {
    type: 'string',
    required: true,
    format: 'iso8601',
    description: 'Timestamp de validación'
  },
  validation_version: {
    type: 'string',
    required: true,
    description: 'Versión del validador utilizado'
  },
  batch_id: {
    type: 'number',
    required: false,
    description: 'ID del lote de procesamiento'
  },
  batch_position: {
    type: 'number',
    required: false,
    description: 'Posición dentro del lote'
  }
};

/**
 * Esquema para estadísticas de GPS
 */
export const GPSStatsSchema = {
  totalRecords: {
    type: 'number',
    required: true,
    description: 'Total de registros'
  },
  memoryUsage: {
    type: 'number',
    required: false,
    description: 'Uso de memoria en bytes'
  },
  key: {
    type: 'string',
    required: true,
    description: 'Clave de Redis utilizada'
  },
  timestamp: {
    type: 'string',
    required: true,
    format: 'iso8601',
    description: 'Timestamp de las estadísticas'
  }
};

/**
 * Esquema para resultado de procesamiento
 */
export const ProcessingResultSchema = {
  success: {
    type: 'boolean',
    required: true,
    description: 'Indica si el procesamiento fue exitoso'
  },
  recordsProcessed: {
    type: 'number',
    required: true,
    description: 'Número de registros procesados'
  },
  processingTime: {
    type: 'number',
    required: false,
    description: 'Tiempo de procesamiento en milisegundos'
  },
  outputFile: {
    type: 'string',
    required: false,
    description: 'Ruta del archivo de salida'
  },
  fileSize: {
    type: 'number',
    required: false,
    description: 'Tamaño del archivo generado en bytes'
  },
  error: {
    type: 'string',
    required: false,
    description: 'Mensaje de error si el procesamiento falló'
  },
  validationStats: {
    type: 'object',
    required: false,
    description: 'Estadísticas de validación'
  }
};

/**
 * Esquema para configuración del scheduler
 */
export const SchedulerConfigSchema = {
  intervalMinutes: {
    type: 'number',
    required: true,
    min: 1,
    description: 'Intervalo en minutos entre ejecuciones'
  },
  enabled: {
    type: 'boolean',
    required: true,
    description: 'Indica si el scheduler está habilitado'
  },
  timezone: {
    type: 'string',
    required: false,
    description: 'Zona horaria para el scheduler'
  },
  maxConcurrentJobs: {
    type: 'number',
    required: false,
    min: 1,
    max: 10,
    description: 'Máximo número de trabajos concurrentes'
  }
};

/**
 * Esquema para métricas del sistema
 */
export const MetricsSchema = {
  processing: {
    type: 'object',
    properties: {
      totalRuns: { type: 'number' },
      successfulRuns: { type: 'number' },
      failedRuns: { type: 'number' },
      totalRecordsProcessed: { type: 'number' },
      averageProcessingTime: { type: 'number' }
    }
  },
  redis: {
    type: 'object',
    properties: {
      connections: { type: 'number' },
      disconnections: { type: 'number' },
      errors: { type: 'number' }
    }
  },
  bigquery: {
    type: 'object',
    properties: {
      uploads: { type: 'number' },
      successfulUploads: { type: 'number' },
      failedUploads: { type: 'number' },
      totalRecordsUploaded: { type: 'number' }
    }
  }
};

/**
 * Constantes para tipos de datos GPS
 */
export const GPS_CONSTANTS = {
  // Límites geográficos
  LATITUDE_MIN: -90,
  LATITUDE_MAX: 90,
  LONGITUDE_MIN: -180,
  LONGITUDE_MAX: 180,
  
  // Límites de velocidad (km/h)
  SPEED_MIN: 0,
  SPEED_MAX: 500,
  
  // Límites de altitud (metros)
  ALTITUDE_MIN: -500,
  ALTITUDE_MAX: 10000,
  
  // Límites de rumbo (grados)
  HEADING_MIN: 0,
  HEADING_MAX: 360,
  
  // Valores por defecto
  DEFAULT_DEVICE_ID: 'unknown',
  DEFAULT_ACCURACY: 0,
  DEFAULT_SPEED: 0,
  DEFAULT_ALTITUDE: 0,
  DEFAULT_HEADING: 0,
  
  // Formatos de timestamp
  TIMESTAMP_FORMAT: 'YYYY-MM-DDTHH:mm:ss.sssZ',
  
  // Tamaños de lote
  DEFAULT_BATCH_SIZE: 1000,
  MAX_BATCH_SIZE: 10000,
  
  // Claves de Redis
  DEFAULT_GPS_KEY: 'gps:history:global',
  
  // Estados de validación
  VALIDATION_STATUS: {
    VALID: 'valid',
    INVALID: 'invalid',
    PARTIAL: 'partial'
  },
  
  // Estados de procesamiento
  PROCESSING_STATUS: {
    PENDING: 'pending',
    PROCESSING: 'processing',
    COMPLETED: 'completed',
    FAILED: 'failed'
  }
};

/**
 * Funciones de utilidad para tipos
 */
export class GPSTypeUtils {
  /**
   * Valida si un valor está dentro del rango de latitud
   */
  static isValidLatitude(lat) {
    const num = parseFloat(lat);
    return !isNaN(num) && num >= GPS_CONSTANTS.LATITUDE_MIN && num <= GPS_CONSTANTS.LATITUDE_MAX;
  }

  /**
   * Valida si un valor está dentro del rango de longitud
   */
  static isValidLongitude(lng) {
    const num = parseFloat(lng);
    return !isNaN(num) && num >= GPS_CONSTANTS.LONGITUDE_MIN && num <= GPS_CONSTANTS.LONGITUDE_MAX;
  }

  /**
   * Valida si un timestamp es válido
   */
  static isValidTimestamp(timestamp) {
    try {
      const date = new Date(timestamp);
      return !isNaN(date.getTime());
    } catch {
      return false;
    }
  }

  /**
   * Normaliza un rumbo a 0-360 grados
   */
  static normalizeHeading(heading) {
    const num = parseFloat(heading);
    if (isNaN(num)) return null;
    
    let normalized = num % 360;
    if (normalized < 0) normalized += 360;
    return normalized;
  }

  /**
   * Crea un ID único para un registro GPS
   */
  static generateGPSId(record, index = null) {
    const timestamp = record.timestamp || new Date().toISOString();
    const deviceId = record.device_id || GPS_CONSTANTS.DEFAULT_DEVICE_ID;
    const suffix = index !== null ? `_${index}` : '';
    
    return `gps_${deviceId}_${Date.now()}${suffix}`;
  }

  /**
   * Convierte un registro GPS a formato estándar
   */
  static standardizeGPSRecord(record) {
    return {
      id: record.id || this.generateGPSId(record),
      latitude: parseFloat(record.latitude || record.lat),
      longitude: parseFloat(record.longitude || record.lng || record.lon),
      timestamp: record.timestamp || record.time || new Date().toISOString(),
      speed: record.speed ? parseFloat(record.speed) : GPS_CONSTANTS.DEFAULT_SPEED,
      heading: record.heading || record.bearing ? 
        this.normalizeHeading(record.heading || record.bearing) : GPS_CONSTANTS.DEFAULT_HEADING,
      altitude: record.altitude || record.alt ? 
        parseFloat(record.altitude || record.alt) : GPS_CONSTANTS.DEFAULT_ALTITUDE,
      accuracy: record.accuracy ? parseFloat(record.accuracy) : GPS_CONSTANTS.DEFAULT_ACCURACY,
      device_id: record.device_id || record.deviceId || GPS_CONSTANTS.DEFAULT_DEVICE_ID
    };
  }

  /**
   * Calcula la distancia entre dos puntos GPS (en metros)
   */
  static calculateDistance(point1, point2) {
    const R = 6371000; // Radio de la Tierra en metros
    const lat1Rad = point1.latitude * Math.PI / 180;
    const lat2Rad = point2.latitude * Math.PI / 180;
    const deltaLatRad = (point2.latitude - point1.latitude) * Math.PI / 180;
    const deltaLngRad = (point2.longitude - point1.longitude) * Math.PI / 180;

    const a = Math.sin(deltaLatRad / 2) * Math.sin(deltaLatRad / 2) +
              Math.cos(lat1Rad) * Math.cos(lat2Rad) *
              Math.sin(deltaLngRad / 2) * Math.sin(deltaLngRad / 2);
    
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    
    return R * c;
  }

  /**
   * Valida un esquema contra un objeto
   */
  static validateSchema(obj, schema) {
    const errors = [];
    
    for (const [field, rules] of Object.entries(schema)) {
      const value = obj[field];
      
      // Verificar campos requeridos
      if (rules.required && (value === undefined || value === null)) {
        errors.push(`Field '${field}' is required`);
        continue;
      }
      
      // Si el campo no está presente y no es requerido, continuar
      if (value === undefined || value === null) continue;
      
      // Verificar tipo
      if (rules.type && typeof value !== rules.type) {
        errors.push(`Field '${field}' must be of type ${rules.type}`);
        continue;
      }
      
      // Verificar rangos numéricos
      if (rules.type === 'number') {
        if (rules.min !== undefined && value < rules.min) {
          errors.push(`Field '${field}' must be >= ${rules.min}`);
        }
        if (rules.max !== undefined && value > rules.max) {
          errors.push(`Field '${field}' must be <= ${rules.max}`);
        }
      }
      
      // Verificar longitud de strings
      if (rules.type === 'string') {
        if (rules.maxLength !== undefined && value.length > rules.maxLength) {
          errors.push(`Field '${field}' must be <= ${rules.maxLength} characters`);
        }
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors
    };
  }
}