import { DataSeparator } from '../src/services/DataSeparator.js';
import { logger } from '../src/utils/logger.js';

/**
 * Ejemplo de uso del DataSeparator actualizado
 * Muestra cómo separar datos GPS y Mobile desde Redis
 */
async function demonstrateDataSeparator() {
  const dataSeparator = new DataSeparator();

  try {
    logger.info('🚀 Iniciando demostración del DataSeparator...');

    // Ejemplo 1: Separación manual con datos de ejemplo
    logger.info('\n📋 Ejemplo 1: Separación manual de datos');
    
    const gpsHistoryData = [
      JSON.stringify({
        deviceId: 'device-001',
        lat: -12.044997,
        lng: -77.031228,
        timestamp: '2025-07-24T20:11:15.752Z',
        receivedAt: '2025-07-24T20:10:49.065Z',
        batchId: 'hist_1753387852083_74tv6pk07',
        metadata: { speed: 45 }
      }),
      JSON.stringify({
        deviceId: 'device-007',
        lat: -12.044692,
        lng: -77.030933,
        timestamp: '2025-07-24T20:15:30.000Z',
        receivedAt: '2025-07-24T20:15:10.123Z',
        batchId: 'hist_1753387852083_74tv6pk08'
      })
    ];

    const mobileHistoryData = [
      JSON.stringify({
        userId: 'mobile_user_001',
        lat: 40.7145,
        lng: -74.006,
        name: 'Usuario prueba 1',
        email: 'usuario.prueba@example.gob.pe',
        timestamp: '2025-07-24T09:44:00.000Z',
        receivedAt: '2025-07-24T14:44:33.590Z'
      }),
      JSON.stringify({
        userId: 'mobile_user_002',
        lat: 40.7145,
        lng: -74.006,
        name: 'Usuario prueba 2',
        email: 'usuario.prueba2@example.gob.pe',
        timestamp: '2025-07-24T09:45:00.000Z',
        receivedAt: '2025-07-24T14:45:33.590Z'
      })
    ];

    // Separar los datos
    const separatedData = dataSeparator.separateDataByType(gpsHistoryData, mobileHistoryData);
    
    logger.info(`✅ Datos separados:`);
    logger.info(`   📍 GPS: ${separatedData.gps.metadata.recordCount} registros`);
    logger.info(`   📱 Mobile: ${separatedData.mobile.metadata.recordCount} registros`);

    // Ejemplo 2: Validación de datos GPS
    logger.info('\n🔍 Ejemplo 2: Validación de datos GPS');
    
    const gpsValidation = await dataSeparator.validateGPSData(separatedData.gps.data);
    logger.info(`   ✅ GPS válidos: ${gpsValidation.validData.length}/${gpsValidation.stats.total}`);
    logger.info(`   📊 Tasa de validación: ${gpsValidation.stats.validationRate}%`);

    // Mostrar estructura de datos GPS validados
    if (gpsValidation.validData.length > 0) {
      logger.info('   📋 Estructura GPS validada:');
      const sampleGPS = gpsValidation.validData[0];
      logger.info(`      - deviceId: ${sampleGPS.deviceId}`);
      logger.info(`      - lat: ${sampleGPS.lat}`);
      logger.info(`      - lng: ${sampleGPS.lng}`);
      logger.info(`      - timestamp: ${sampleGPS.timestamp}`);
    }

    // Ejemplo 3: Validación de datos Mobile
    logger.info('\n📱 Ejemplo 3: Validación de datos Mobile');
    
    const mobileValidation = await dataSeparator.validateMobileData(separatedData.mobile.data);
    logger.info(`   ✅ Mobile válidos: ${mobileValidation.validData.length}/${mobileValidation.stats.total}`);
    logger.info(`   📊 Tasa de validación: ${mobileValidation.stats.validationRate}%`);

    // Mostrar estructura de datos Mobile validados
    if (mobileValidation.validData.length > 0) {
      logger.info('   📋 Estructura Mobile validada:');
      const sampleMobile = mobileValidation.validData[0];
      logger.info(`      - userId: ${sampleMobile.userId}`);
      logger.info(`      - lat: ${sampleMobile.lat}`);
      logger.info(`      - lng: ${sampleMobile.lng}`);
      logger.info(`      - timestamp: ${sampleMobile.timestamp}`);
      logger.info(`      - name: ${sampleMobile.name}`);
      logger.info(`      - email: ${sampleMobile.email}`);
    }

    // Ejemplo 4: Formateo para GCS
    logger.info('\n📦 Ejemplo 4: Formateo para GCS');
    
    const gcsGpsData = dataSeparator.formatForGCS(gpsValidation.validData, 'gps');
    const gcsMobileData = dataSeparator.formatForGCS(mobileValidation.validData, 'mobile');

    logger.info(`   📍 GPS formateado para GCS:`);
    logger.info(`      - Tipo: ${gcsGpsData.metadata.type}`);
    logger.info(`      - Registros: ${gcsGpsData.metadata.recordCount}`);
    logger.info(`      - Fuente: ${gcsGpsData.metadata.source}`);
    logger.info(`      - Processing ID: ${gcsGpsData.metadata.processingId}`);

    logger.info(`   📱 Mobile formateado para GCS:`);
    logger.info(`      - Tipo: ${gcsMobileData.metadata.type}`);
    logger.info(`      - Registros: ${gcsMobileData.metadata.recordCount}`);
    logger.info(`      - Fuente: ${gcsMobileData.metadata.source}`);
    logger.info(`      - Processing ID: ${gcsMobileData.metadata.processingId}`);

    // Ejemplo 5: Estadísticas del separador
    logger.info('\n📊 Ejemplo 5: Estadísticas del separador');
    
    const stats = dataSeparator.getStats();
    logger.info(`   🔑 Claves Redis:`);
    logger.info(`      - GPS: ${stats.redisKeys.gps}`);
    logger.info(`      - Mobile: ${stats.redisKeys.mobile}`);
    
    logger.info(`   ✅ Campos requeridos GPS: ${stats.validators.gps.requiredFields.join(', ')}`);
    logger.info(`   ✅ Campos requeridos Mobile: ${stats.validators.mobile.requiredFields.join(', ')}`);

    logger.info('\n🎉 Demostración completada exitosamente!');

  } catch (error) {
    logger.error('❌ Error en la demostración:', error.message);
  } finally {
    // Limpiar recursos
    await dataSeparator.cleanup();
  }
}

// Ejecutar la demostración si es llamado directamente
import { fileURLToPath } from 'url';

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  demonstrateDataSeparator().catch(error => {
    logger.error('❌ Error fatal en la demostración:', error.message);
    process.exit(1);
  });
}

export { demonstrateDataSeparator };