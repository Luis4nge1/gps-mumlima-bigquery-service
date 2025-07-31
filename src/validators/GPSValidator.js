import { logger } from '../utils/logger.js';

/**
 * Validador para datos GPS
 */
export class GPSValidator {
  constructor() {
    this.requiredFields = ['latitude', 'longitude'];
    this.optionalFields = ['timestamp', 'speed', 'heading', 'altitude', 'accuracy', 'device_id'];
  }

  /**
   * Valida un registro GPS individual
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

      // Validar campos requeridos
      const latitudeValidation = this.validateLatitude(gpsData);
      if (!latitudeValidation.isValid) {
        errors.push(...latitudeValidation.errors);
      } else {
        cleanedData.latitude = latitudeValidation.value;
      }

      const longitudeValidation = this.validateLongitude(gpsData);
      if (!longitudeValidation.isValid) {
        errors.push(...longitudeValidation.errors);
      } else {
        cleanedData.longitude = longitudeValidation.value;
      }

      // Validar campos opcionales
      const timestampValidation = this.validateTimestamp(gpsData);
      cleanedData.timestamp = timestampValidation.value;

      const speedValidation = this.validateSpeed(gpsData);
      if (speedValidation.value !== null) {
        cleanedData.speed = speedValidation.value;
      }

      const headingValidation = this.validateHeading(gpsData);
      if (headingValidation.value !== null) {
        cleanedData.heading = headingValidation.value;
      }

      const altitudeValidation = this.validateAltitude(gpsData);
      if (altitudeValidation.value !== null) {
        cleanedData.altitude = altitudeValidation.value;
      }

      const accuracyValidation = this.validateAccuracy(gpsData);
      if (accuracyValidation.value !== null) {
        cleanedData.accuracy = accuracyValidation.value;
      }

      const deviceIdValidation = this.validateDeviceId(gpsData);
      cleanedData.device_id = deviceIdValidation.value;

      // Agregar campos adicionales que puedan existir
      const additionalFields = Object.keys(gpsData).filter(key => 
        !this.requiredFields.includes(key) && 
        !this.optionalFields.includes(key) &&
        !['lat', 'lng', 'lon', 'time', 'bearing', 'alt', 'deviceId'].includes(key)
      );

      additionalFields.forEach(field => {
        cleanedData[field] = gpsData[field];
      });

      // Agregar metadatos de validación
      cleanedData.validated_at = new Date().toISOString();
      cleanedData.validation_version = '1.0';

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
   * Valida latitud
   */
  validateLatitude(data) {
    const lat = data.latitude || data.lat;
    
    if (lat === undefined || lat === null) {
      return { isValid: false, errors: ['Latitude is required'], value: null };
    }

    const numLat = parseFloat(lat);
    
    if (isNaN(numLat)) {
      return { isValid: false, errors: ['Latitude must be a number'], value: null };
    }

    if (numLat < -90 || numLat > 90) {
      return { isValid: false, errors: ['Latitude must be between -90 and 90'], value: null };
    }

    return { isValid: true, errors: [], value: numLat };
  }

  /**
   * Valida longitud
   */
  validateLongitude(data) {
    const lng = data.longitude || data.lng || data.lon;
    
    if (lng === undefined || lng === null) {
      return { isValid: false, errors: ['Longitude is required'], value: null };
    }

    const numLng = parseFloat(lng);
    
    if (isNaN(numLng)) {
      return { isValid: false, errors: ['Longitude must be a number'], value: null };
    }

    if (numLng < -180 || numLng > 180) {
      return { isValid: false, errors: ['Longitude must be between -180 and 180'], value: null };
    }

    return { isValid: true, errors: [], value: numLng };
  }

  /**
   * Valida timestamp
   */
  validateTimestamp(data) {
    const timestamp = data.timestamp || data.time;
    
    if (!timestamp) {
      return { isValid: true, errors: [], value: new Date().toISOString() };
    }

    try {
      const date = new Date(timestamp);
      if (isNaN(date.getTime())) {
        return { isValid: true, errors: [], value: new Date().toISOString() };
      }
      
      return { isValid: true, errors: [], value: date.toISOString() };
    } catch (error) {
      return { isValid: true, errors: [], value: new Date().toISOString() };
    }
  }

  /**
   * Valida velocidad
   */
  validateSpeed(data) {
    const speed = data.speed;
    
    if (speed === undefined || speed === null) {
      return { isValid: true, errors: [], value: null };
    }

    const numSpeed = parseFloat(speed);
    
    if (isNaN(numSpeed)) {
      return { isValid: true, errors: [], value: null };
    }

    if (numSpeed < 0) {
      return { isValid: true, errors: [], value: 0 };
    }

    // Velocidad máxima razonable: 500 km/h
    if (numSpeed > 500) {
      return { isValid: true, errors: [], value: 500 };
    }

    return { isValid: true, errors: [], value: numSpeed };
  }

  /**
   * Valida rumbo/dirección
   */
  validateHeading(data) {
    const heading = data.heading || data.bearing;
    
    if (heading === undefined || heading === null) {
      return { isValid: true, errors: [], value: null };
    }

    const numHeading = parseFloat(heading);
    
    if (isNaN(numHeading)) {
      return { isValid: true, errors: [], value: null };
    }

    // Normalizar a 0-360 grados
    let normalizedHeading = numHeading % 360;
    if (normalizedHeading < 0) {
      normalizedHeading += 360;
    }

    return { isValid: true, errors: [], value: normalizedHeading };
  }

  /**
   * Valida altitud
   */
  validateAltitude(data) {
    const altitude = data.altitude || data.alt;
    
    if (altitude === undefined || altitude === null) {
      return { isValid: true, errors: [], value: null };
    }

    const numAltitude = parseFloat(altitude);
    
    if (isNaN(numAltitude)) {
      return { isValid: true, errors: [], value: null };
    }

    // Rango razonable: -500m a 10000m
    if (numAltitude < -500) {
      return { isValid: true, errors: [], value: -500 };
    }

    if (numAltitude > 10000) {
      return { isValid: true, errors: [], value: 10000 };
    }

    return { isValid: true, errors: [], value: numAltitude };
  }

  /**
   * Valida precisión
   */
  validateAccuracy(data) {
    const accuracy = data.accuracy;
    
    if (accuracy === undefined || accuracy === null) {
      return { isValid: true, errors: [], value: null };
    }

    const numAccuracy = parseFloat(accuracy);
    
    if (isNaN(numAccuracy)) {
      return { isValid: true, errors: [], value: null };
    }

    if (numAccuracy < 0) {
      return { isValid: true, errors: [], value: 0 };
    }

    return { isValid: true, errors: [], value: numAccuracy };
  }

  /**
   * Valida ID del dispositivo
   */
  validateDeviceId(data) {
    const deviceId = data.device_id || data.deviceId;
    
    if (!deviceId) {
      return { isValid: true, errors: [], value: 'unknown' };
    }

    const strDeviceId = String(deviceId).trim();
    
    if (strDeviceId.length === 0) {
      return { isValid: true, errors: [], value: 'unknown' };
    }

    // Limpiar caracteres especiales peligrosos
    const cleanDeviceId = strDeviceId.replace(/[<>\"'&]/g, '');
    
    return { isValid: true, errors: [], value: cleanDeviceId };
  }

  /**
   * Valida un lote de registros GPS
   */
  validateGPSBatch(records) {
    const results = {
      valid: [],
      invalid: [],
      stats: {
        total: records.length,
        validCount: 0,
        invalidCount: 0,
        validationRate: 0
      }
    };

    records.forEach((record, index) => {
      const validation = this.validateGPSRecord(record);
      
      if (validation.isValid) {
        results.valid.push({
          index,
          data: validation.cleanedData
        });
        results.stats.validCount++;
      } else {
        results.invalid.push({
          index,
          record,
          errors: validation.errors
        });
        results.stats.invalidCount++;
      }
    });

    results.stats.validationRate = results.stats.total > 0 ? 
      (results.stats.validCount / results.stats.total * 100).toFixed(2) : 0;

    return results;
  }

  /**
   * Obtiene estadísticas del validador
   */
  getValidatorStats() {
    return {
      requiredFields: this.requiredFields,
      optionalFields: this.optionalFields,
      validationRules: {
        latitude: 'Required, number between -90 and 90',
        longitude: 'Required, number between -180 and 180',
        timestamp: 'Optional, valid date string or current time',
        speed: 'Optional, positive number, max 500 km/h',
        heading: 'Optional, number normalized to 0-360 degrees',
        altitude: 'Optional, number between -500m and 10000m',
        accuracy: 'Optional, positive number',
        device_id: 'Optional, string identifier'
      }
    };
  }
}