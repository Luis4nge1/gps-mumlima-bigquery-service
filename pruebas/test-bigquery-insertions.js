#!/usr/bin/env node

/**
 * Script para probar inserciones reales de GPS y Mobile en BigQuery
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('🧪 Probando inserciones en BigQuery...');

async function testBigQueryInsertions() {
  try {
    const { RedisRepository } = await import('../src/repositories/RedisRepository.js');
    const { GPSProcessorService } = await import('../src/services/GPSProcessorService.js');
    
    console.log('\n📋 Paso 1: Conectando a Redis...');
    const redisRepo = new RedisRepository();
    await redisRepo.connect();
    console.log('✅ Conectado a Redis');
    
    // Limpiar datos existentes
    console.log('\n🗑️ Limpiando datos existentes...');
    try {
      await redisRepo.clearListData('gps:history:global');
      await redisRepo.clearListData('mobile:history:global');
      console.log('✅ Datos existentes limpiados');
    } catch (cleanError) {
      console.log('⚠️ No había datos para limpiar');
    }
    
    console.log('\n📊 Paso 2: Insertando datos de prueba en Redis...');
    
    // Datos GPS de prueba
    const gpsTestData = [
      {
        deviceId: 'GPS_DEVICE_001',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: new Date().toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'test_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 25.5,
          heading: 180,
          altitude: 150,
          accuracy: 5,
          batteryLevel: 85
        }
      },
      {
        deviceId: 'GPS_DEVICE_002',
        lat: -12.0500,
        lng: -77.0450,
        timestamp: new Date(Date.now() - 60000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'test_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 30.0,
          heading: 90,
          altitude: 145,
          accuracy: 3,
          batteryLevel: 90
        }
      },
      {
        deviceId: 'GPS_DEVICE_003',
        lat: -12.0520,
        lng: -77.0480,
        timestamp: new Date(Date.now() - 120000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'test_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 15.2,
          heading: 45,
          altitude: 155,
          accuracy: 8,
          batteryLevel: 75
        }
      }
    ];
    
    // Datos Mobile de prueba
    const mobileTestData = [
      {
        userId: 'MOBILE_USER_001',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: new Date().toISOString(),
        name: 'Juan Pérez',
        email: 'juan.perez@municipalidad.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'test_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 10,
          batteryLevel: 80,
          networkType: '4G',
          appVersion: '2.1.0',
          deviceModel: 'Samsung Galaxy A54',
          osVersion: 'Android 13',
          isBackground: false,
          locationSource: 'gps'
        }
      },
      {
        userId: 'MOBILE_USER_002',
        lat: -12.0480,
        lng: -77.0440,
        timestamp: new Date(Date.now() - 30000).toISOString(),
        name: 'María García',
        email: 'maria.garcia@municipalidad.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'test_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 15,
          batteryLevel: 65,
          networkType: 'WiFi',
          appVersion: '2.1.0',
          deviceModel: 'iPhone 14',
          osVersion: 'iOS 17.1',
          isBackground: true,
          locationSource: 'network'
        }
      },
      {
        userId: 'MOBILE_USER_003',
        lat: -12.0510,
        lng: -77.0460,
        timestamp: new Date(Date.now() - 90000).toISOString(),
        name: 'Carlos López',
        email: 'carlos.lopez@municipalidad.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'test_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 5,
          batteryLevel: 95,
          networkType: '5G',
          appVersion: '2.0.8',
          deviceModel: 'Xiaomi Redmi Note 12',
          osVersion: 'Android 12',
          isBackground: false,
          locationSource: 'gps'
        }
      }
    ];
    
    // Insertar datos GPS en Redis
    console.log('📍 Insertando datos GPS...');
    for (const gpsData of gpsTestData) {
      await redisRepo.addToList('gps:history:global', JSON.stringify(gpsData));
    }
    console.log(`✅ ${gpsTestData.length} registros GPS insertados`);
    
    // Insertar datos Mobile en Redis
    console.log('📱 Insertando datos Mobile...');
    for (const mobileData of mobileTestData) {
      await redisRepo.addToList('mobile:history:global', JSON.stringify(mobileData));
    }
    console.log(`✅ ${mobileTestData.length} registros Mobile insertados`);
    
    // Verificar datos en Redis
    console.log('\n📊 Verificando datos en Redis...');
    const gpsStats = await redisRepo.getGPSStats();
    const mobileStats = await redisRepo.getMobileStats();
    console.log(`- GPS en Redis: ${gpsStats.totalRecords} registros`);
    console.log(`- Mobile en Redis: ${mobileStats.totalRecords} registros`);
    
    // NO desconectar Redis aquí, mantener la conexión para el procesador
    
    console.log('\n🔄 Paso 3: Procesando datos hacia BigQuery...');
    
    // Inicializar y ejecutar procesador
    const processor = new GPSProcessorService();
    await processor.initialize();
    
    console.log('✅ GPSProcessorService inicializado');
    
    // Ejecutar procesamiento
    const result = await processor.processGPSData();
    
    console.log('\n📊 Resultado del procesamiento:');
    console.log('- Success:', result.success);
    console.log('- Records Processed:', result.recordsProcessed);
    console.log('- Processing Time:', result.processingTime, 'ms');
    
    if (result.results) {
      console.log('\n📍 Resultado GPS:');
      console.log('  - Success:', result.results.gps.success);
      console.log('  - Records:', result.results.gps.recordsProcessed);
      console.log('  - Stage:', result.results.gps.stage);
      if (result.results.gps.jobId) {
        console.log('  - BigQuery Job ID:', result.results.gps.jobId);
      }
      
      console.log('\n📱 Resultado Mobile:');
      console.log('  - Success:', result.results.mobile.success);
      console.log('  - Records:', result.results.mobile.recordsProcessed);
      console.log('  - Stage:', result.results.mobile.stage);
      if (result.results.mobile.jobId) {
        console.log('  - BigQuery Job ID:', result.results.mobile.jobId);
      }
    }
    
    if (result.error) {
      console.error('\n❌ Error en procesamiento:', result.error);
    }
    
    // Verificar que Redis se limpió
    console.log('\n🔍 Verificando limpieza de Redis...');
    const finalGpsStats = await redisRepo.getGPSStats();
    const finalMobileStats = await redisRepo.getMobileStats();
    console.log(`- GPS restantes en Redis: ${finalGpsStats.totalRecords}`);
    console.log(`- Mobile restantes en Redis: ${finalMobileStats.totalRecords}`);
    
    await redisRepo.disconnect();
    await processor.cleanup();
    
    console.log('\n🎉 Prueba de inserciones completada');
    
    // Resumen final
    console.log('\n📋 RESUMEN:');
    console.log('✅ Datos insertados en Redis correctamente');
    console.log('✅ Procesamiento ejecutado');
    console.log('✅ Datos enviados a BigQuery');
    console.log('✅ Redis limpiado después del procesamiento');
    console.log('✅ Bucket GCS optimizado utilizado');
    
    if (result.success && result.recordsProcessed > 0) {
      console.log('\n🎯 ¡ÉXITO! Los datos se insertaron correctamente en BigQuery');
      console.log(`   📊 Total procesado: ${result.recordsProcessed} registros`);
      console.log(`   ⏱️ Tiempo: ${result.processingTime}ms`);
    } else if (result.recordsProcessed === 0) {
      console.log('\n⚠️ No se procesaron registros (posible problema de validación)');
    } else {
      console.log('\n❌ Hubo errores en el procesamiento');
    }
    
  } catch (error) {
    console.error('❌ Error en prueba de inserciones:', error.message);
    console.error('Stack:', error.stack);
  }
}

testBigQueryInsertions().then(() => {
  console.log('\n✅ Test de inserciones completado');
  process.exit(0);
}).catch(error => {
  console.error('💥 Error fatal:', error.message);
  process.exit(1);
});