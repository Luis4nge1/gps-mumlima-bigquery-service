import { logger } from '../utils/logger.js';
import { GPSValidator } from '../validators/GPSValidator.js';
import { RedisRepository } from '../repositories/RedisRepository.js';

/**
 * Servicio para separar y procesar datos GPS y Mobile desde Redis
 * Implementa la separación por tipo de datos según los requerimientos 1.1 y 1.2
 */
export class DataSeparator {
  constructor() {
    this.gpsValidator = new GPSValidator();
    this.gpsKey = 'gps:history:global';
    this.mobileKey = 'mobile:history:global';
    this.redisRepo = new RedisRepository();
  }

  /**
   * Separa datos GPS y Mobile desde las listas de historial global de Redis
   * @param {Array} gpsHistoryData - Datos de gps:history:global
   * @param {Array} mobileHistoryData - Datos de mobile:history:global
   * @returns {Object} Datos separados por tipo
   */
  separateDataByType(gpsHistoryData = [], mobileHistoryData = []) {
    try {
      logger.info('🔄 Iniciando separación de datos GPS y Mobile...');

      const separatedData = {
        gps: {
          data: [],
          metadata: {
            type: 'gps',
            timestamp: new Date().toISOString(),
            recordCount: 0,
            source: 'redis:gps:history:global',
            processingId: this.generateProcessingId('gps')
          }
        },
        mobile: {
          data: [],
          metadata: {
            type: 'mobile',
            timestamp: new Date().toISOString(),
            recordCount: 0,
            source: 'redis:mobile:history:global',
            processingId: this.generateProcessingId('mobile')
          }
        }
      };

      // Procesar datos GPS
      gpsHistoryData.forEach((item) => {
        const parsedData = this.parseRedisValue(item);
        if (parsedData) {
          separatedData.gps.data.push(parsedData);
        }
      });

      // Procesar datos Mobile
      mobileHistoryData.forEach((item) => {
        const parsedData = this.parseRedisValue(item);
        if (parsedData) {
          separatedData.mobile.data.push(parsedData);
        }
      });

      // Actualizar contadores
      separatedData.gps.metadata.recordCount = separatedData.gps.data.length;
      separatedData.mobile.metadata.recordCount = separatedData.mobile.data.length;

      logger.info(`✅ Separación completada: ${separatedData.gps.metadata.recordCount} GPS, ${separatedData.mobile.metadata.recordCount} Mobile`);

      return separatedData;

    } catch (error) {
      logger.error('❌ Error separando datos por tipo:', error.message);
      throw error;
    }
  }

  /**
   * Valida estructura de datos GPS
   * @param {Array} gpsData - Array de datos GPS
   * @returns {Object} Resultado de validación
   */
  validateGPSData(gpsData) {
    try {
      logger.info(`🔍 Validando ${gpsData.length} registros GPS...`);

      const validData = [];
      const invalidData = [];
      const errors = [];

      gpsData.forEach((record, index) => {
        const validation = this.validateGPSRecord(record);

        if (validation.isValid) {
          validData.push(validation.cleanedData);
        } else {
          invalidData.push({
            index,
            record,
            errors: validation.errors
          });
          errors.push(...validation.errors.map(err => `GPS Record ${index}: ${err}`));
        }
      });

      const stats = {
        total: gpsData.length,
        valid: validData.length,
        invalid: invalidData.length,
        validationRate: gpsData.length > 0 ? ((validData.length / gpsData.length) * 100).toFixed(2) : 0
      };

      logger.info(`✅ Validación GPS completada: ${stats.valid}/${stats.total} registros válidos (${stats.validationRate}%)`);

      return {
        isValid: validData.length > 0,
        validData,
        invalidData,
        errors,
        stats
      };

    } catch (error) {
      logger.error('❌ Error validando datos GPS:', error.message);
      throw error;
    }
  }

  /**
   * Valida estructura de datos Mobile
   * @param {Array} mobileData - Array de datos Mobile
   * @returns {Object} Resultado de validación
   */
  validateMobileData(mobileData) {
    try {
      logger.info(`🔍 Validando ${mobileData.length} registros Mobile...`);

      const validData = [];
      const invalidData = [];
      const errors = [];

      mobileData.forEach((record, index) => {
        const validation = this.validateMobileRecord(record);

        if (validation.isValid) {
          validData.push(validation.cleanedData);
        } else {
          invalidData.push({
            index,
            record,
            errors: validation.errors
          });
          errors.push(...validation.errors.map(err => `Mobile Record ${index}: ${err}`));
        }
      });

      const stats = {
        total: mobileData.length,
        valid: validData.length,
        invalid: invalidData.length,
        validationRate: mobileData.length > 0 ? ((validData.length / mobileData.length) * 100).toFixed(2) : 0
      };

      logger.info(`✅ Validación Mobile completada: ${stats.valid}/${stats.total} registros válidos (${stats.validationRate}%)`);

      return {
        isValid: validData.length > 0,
        validData,
        invalidData,
        errors,
        stats
      };

    } catch (error) {
      logger.error('❌ Error validando datos Mobile:', error.message);
      throw error;
    }
  }

  /**
   * Formatea datos para estructura GCS compatible
   * @param {Array} data - Datos validados
   * @param {string} type - Tipo de datos ('gps' o 'mobile')
   * @returns {Object} Estructura compatible con GCS
   */
  formatForGCS(data, type) {
    try {
      logger.info(`📦 Formateando ${data.length} registros ${type} para GCS...`);

      const gcsStructure = {
        metadata: {
          type: type,
          timestamp: new Date().toISOString(),
          recordCount: data.length,
          source: `redis:${type}:history:global`,
          processingId: this.generateProcessingId(type),
          formatVersion: '1.0'
        },
        data: data
      };

      logger.info(`✅ Formateo GCS completado para ${type}: ${data.length} registros`);

      return gcsStructure;

    } catch (error) {
      logger.error(`❌ Error formateando datos ${type} para GCS:`, error.message);
      throw error;
    }
  }

  /**
   * Valida un registro GPS individual - solo campos requeridos
   * @param {Object} record - Registro GPS
   * @returns {Object} Resultado de validación
   */
  validateGPSRecord(record) {
    const errors = [];
    let cleanedData = {};

    try {
      // Convertir string a objeto si es necesario
      let gpsData = typeof record === 'string' ? JSON.parse(record) : record;

      // Validar que sea un objeto
      if (!gpsData || typeof gpsData !== 'object') {
        return {
          isValid: false,
          errors: ['Record is not a valid object'],
          cleanedData: null
        };
      }

      // Validar deviceId (requerido)
      const deviceIdValidation = this.validateDeviceId(gpsData);
      if (!deviceIdValidation.isValid) {
        errors.push(...deviceIdValidation.errors);
      } else {
        cleanedData.deviceId = deviceIdValidation.value;
      }

      // Validar coordenadas (requeridas)
      const latitudeValidation = this.gpsValidator.validateLatitude(gpsData);
      if (!latitudeValidation.isValid) {
        errors.push(...latitudeValidation.errors);
      } else {
        cleanedData.lat = latitudeValidation.value;
      }

      const longitudeValidation = this.gpsValidator.validateLongitude(gpsData);
      if (!longitudeValidation.isValid) {
        errors.push(...longitudeValidation.errors);
      } else {
        cleanedData.lng = longitudeValidation.value;
      }

      // Validar timestamp (requerido)
      const timestampValidation = this.gpsValidator.validateTimestamp(gpsData);
      cleanedData.timestamp = timestampValidation.value;

      return {
        isValid: errors.length === 0,
        errors,
        cleanedData: errors.length === 0 ? cleanedData : null
      };

    } catch (parseError) {
      return {
        isValid: false,
        errors: [`JSON parse error: ${parseError.message}`],
        cleanedData: null
      };
    }
  }

  /**
   * Valida un registro Mobile individual - solo campos requeridos
   * @param {Object} record - Registro Mobile
   * @returns {Object} Resultado de validación
   */
  validateMobileRecord(record) {
    const errors = [];
    let cleanedData = {};

    try {
      // Convertir string a objeto si es necesario
      let mobileData = typeof record === 'string' ? JSON.parse(record) : record;

      // Validar que sea un objeto
      if (!mobileData || typeof mobileData !== 'object') {
        return {
          isValid: false,
          errors: ['Record is not a valid object'],
          cleanedData: null
        };
      }

      // Validar userId (requerido)
      const userIdValidation = this.validateUserId(mobileData);
      if (!userIdValidation.isValid) {
        errors.push(...userIdValidation.errors);
      } else {
        cleanedData.userId = userIdValidation.value;
      }

      // Validar coordenadas (requeridas)
      const latitudeValidation = this.gpsValidator.validateLatitude(mobileData);
      if (!latitudeValidation.isValid) {
        errors.push(...latitudeValidation.errors);
      } else {
        cleanedData.lat = latitudeValidation.value;
      }

      const longitudeValidation = this.gpsValidator.validateLongitude(mobileData);
      if (!longitudeValidation.isValid) {
        errors.push(...longitudeValidation.errors);
      } else {
        cleanedData.lng = longitudeValidation.value;
      }

      // Validar timestamp (requerido)
      const timestampValidation = this.gpsValidator.validateTimestamp(mobileData);
      cleanedData.timestamp = timestampValidation.value;

      // Validar name (requerido)
      const nameValidation = this.validateName(mobileData);
      if (!nameValidation.isValid) {
        errors.push(...nameValidation.errors);
      } else {
        cleanedData.name = nameValidation.value;
      }

      // Validar email (requerido)
      const emailValidation = this.validateEmail(mobileData);
      if (!emailValidation.isValid) {
        errors.push(...emailValidation.errors);
      } else {
        cleanedData.email = emailValidation.value;
      }

      return {
        isValid: errors.length === 0,
        errors,
        cleanedData: errors.length === 0 ? cleanedData : null
      };

    } catch (parseError) {
      return {
        isValid: false,
        errors: [`JSON parse error: ${parseError.message}`],
        cleanedData: null
      };
    }
  }

  /**
   * Valida deviceId para datos GPS
   */
  validateDeviceId(data) {
    const deviceId = data.deviceId || data.device_id;

    if (!deviceId) {
      return { isValid: false, errors: ['deviceId is required for GPS data'], value: null };
    }

    const strDeviceId = String(deviceId).trim();

    if (strDeviceId.length === 0) {
      return { isValid: false, errors: ['deviceId cannot be empty'], value: null };
    }

    // Limpiar caracteres especiales peligrosos
    const cleanDeviceId = strDeviceId.replace(/[<>\"'&]/g, '');

    return { isValid: true, errors: [], value: cleanDeviceId };
  }

  /**
   * Valida userId para datos Mobile
   */
  validateUserId(data) {
    const userId = data.userId || data.user_id;

    if (!userId) {
      return { isValid: false, errors: ['userId is required for mobile data'], value: null };
    }

    const strUserId = String(userId).trim();

    if (strUserId.length === 0) {
      return { isValid: false, errors: ['userId cannot be empty'], value: null };
    }

    // Limpiar caracteres especiales peligrosos
    const cleanUserId = strUserId.replace(/[<>\"'&]/g, '');

    return { isValid: true, errors: [], value: cleanUserId };
  }

  /**
   * Valida name para datos Mobile
   */
  validateName(data) {
    const name = data.name;

    if (!name) {
      return { isValid: false, errors: ['name is required for mobile data'], value: null };
    }

    const strName = String(name).trim();

    if (strName.length === 0) {
      return { isValid: false, errors: ['name cannot be empty'], value: null };
    }

    if (strName.length > 100) {
      return { isValid: false, errors: ['name cannot exceed 100 characters'], value: null };
    }

    // Limpiar caracteres especiales peligrosos
    const cleanName = strName.replace(/[<>\"'&]/g, '');

    return { isValid: true, errors: [], value: cleanName };
  }

  /**
   * Valida email para datos Mobile
   */
  validateEmail(data) {
    const email = data.email;

    if (!email) {
      return { isValid: false, errors: ['email is required for mobile data'], value: null };
    }

    const strEmail = String(email).trim().toLowerCase();

    if (strEmail.length === 0) {
      return { isValid: false, errors: ['email cannot be empty'], value: null };
    }

    // Validación básica de formato de email
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(strEmail)) {
      return { isValid: false, errors: ['email format is invalid'], value: null };
    }

    if (strEmail.length > 254) {
      return { isValid: false, errors: ['email cannot exceed 254 characters'], value: null };
    }

    return { isValid: true, errors: [], value: strEmail };
  }

  /**
   * Parsea valor de Redis
   */
  parseRedisValue(value) {
    try {
      if (typeof value === 'string') {
        return JSON.parse(value);
      }
      return value;
    } catch (error) {
      logger.warn('⚠️ Error parseando valor de Redis:', error.message);
      return null;
    }
  }

  /**
   * Genera ID de procesamiento único
   */
  generateProcessingId(type) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '').slice(0, 15);
    const random = Math.random().toString(36).substr(2, 3);
    return `${type}_${timestamp}_${random}`;
  }

  /**
   * Genera ID único para registro
   */
  generateRecordId(type, record, index) {
    const timestamp = record.timestamp || new Date().toISOString();
    const identifier = record.deviceId || record.userId || 'unknown';
    const timeMs = new Date(timestamp).getTime();
    return `${type}_${identifier}_${timeMs}_${index}`;
  }

  /**
   * Obtiene y separa datos directamente desde Redis
   * @returns {Object} Datos separados por tipo
   */
  async getAndSeparateFromRedis() {
    try {
      logger.info('🔄 Obteniendo datos desde Redis...');

      // Obtener datos GPS desde Redis
      const gpsHistoryData = await this.redisRepo.getListData(this.gpsKey);
      logger.info(`📍 Obtenidos ${gpsHistoryData.length} registros GPS desde ${this.gpsKey}`);

      // Obtener datos Mobile desde Redis
      const mobileHistoryData = await this.redisRepo.getListData(this.mobileKey);
      logger.info(`📱 Obtenidos ${mobileHistoryData.length} registros Mobile desde ${this.mobileKey}`);

      // Separar los datos
      const separatedData = this.separateDataByType(gpsHistoryData, mobileHistoryData);

      return separatedData;

    } catch (error) {
      logger.error('❌ Error obteniendo datos desde Redis:', error.message);
      throw error;
    }
  }

  /**
   * Limpia los datos procesados de Redis
   * @returns {Object} Resultado de la limpieza
   */
  async clearProcessedDataFromRedis() {
    try {
      logger.info('🗑️ Limpiando datos procesados de Redis...');

      const gpsCleared = await this.redisRepo.clearListData(this.gpsKey);
      const mobileCleared = await this.redisRepo.clearListData(this.mobileKey);

      logger.info(`✅ Limpieza completada - GPS: ${gpsCleared}, Mobile: ${mobileCleared}`);

      return {
        success: true,
        gpsCleared,
        mobileCleared
      };

    } catch (error) {
      logger.error('❌ Error limpiando datos de Redis:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Obtiene estadísticas del separador
   */
  getStats() {
    return {
      redisKeys: {
        gps: this.gpsKey,
        mobile: this.mobileKey
      },
      validators: {
        gps: {
          requiredFields: ['deviceId', 'lat', 'lng', 'timestamp'],
          validationRules: {
            deviceId: 'Required, non-empty string identifier',
            lat: 'Required, number between -90 and 90',
            lng: 'Required, number between -180 and 180',
            timestamp: 'Required, valid date string or current time'
          }
        },
        mobile: {
          requiredFields: ['userId', 'lat', 'lng', 'timestamp', 'name', 'email'],
          validationRules: {
            userId: 'Required, non-empty string identifier',
            lat: 'Required, number between -90 and 90',
            lng: 'Required, number between -180 and 180',
            timestamp: 'Required, valid date string or current time',
            name: 'Required, non-empty string, max 100 characters',
            email: 'Required, valid email format, max 254 characters'
          }
        }
      }
    };
  }

  /**
   * Limpia recursos
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del DataSeparator...');
      
      if (this.redisRepo) {
        await this.redisRepo.disconnect();
      }
      
      logger.info('✅ Recursos del DataSeparator limpiados exitosamente');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del DataSeparator:', error.message);
    }
  }
}