#!/usr/bin/env node

/**
 * Script para agregar datos de muestra variados a BigQuery (sin eliminar archivos GCS)
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('📊 Agregando datos de muestra a BigQuery...');

async function addSampleDataToBigQuery() {
  try {
    const { RedisRepository } = await import('../src/repositories/RedisRepository.js');
    const { GPSProcessorService } = await import('../src/services/GPSProcessorService.js');
    
    console.log('\n📋 Conectando a Redis...');
    const redisRepo = new RedisRepository();
    await redisRepo.connect();
    console.log('✅ Conectado a Redis');
    
    console.log('\n📊 Generando datos de muestra variados...');
    
    // Datos GPS más variados (diferentes dispositivos, ubicaciones en Lima)
    const gpsTestData = [
      {
        deviceId: 'GPS_VEHICULO_MUNICIPAL_001',
        lat: -12.0464, // Plaza de Armas
        lng: -77.0428,
        timestamp: new Date().toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'municipal_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 25.5,
          heading: 180,
          altitude: 150,
          accuracy: 5,
          batteryLevel: 85,
          vehicleType: 'patrol',
          driverName: 'Juan Ramirez'
        }
      },
      {
        deviceId: 'GPS_VEHICULO_MUNICIPAL_002',
        lat: -12.0697, // Miraflores
        lng: -77.0365,
        timestamp: new Date(Date.now() - 60000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'municipal_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 35.2,
          heading: 90,
          altitude: 145,
          accuracy: 3,
          batteryLevel: 90,
          vehicleType: 'ambulance',
          driverName: 'Maria Santos'
        }
      },
      {
        deviceId: 'GPS_VEHICULO_MUNICIPAL_003',
        lat: -12.1219, // San Juan de Miraflores
        lng: -76.9739,
        timestamp: new Date(Date.now() - 120000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'municipal_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 15.8,
          heading: 45,
          altitude: 155,
          accuracy: 8,
          batteryLevel: 75,
          vehicleType: 'garbage_truck',
          driverName: 'Carlos Mendoza'
        }
      },
      {
        deviceId: 'GPS_VEHICULO_MUNICIPAL_004',
        lat: -11.9775, // San Isidro
        lng: -77.0645,
        timestamp: new Date(Date.now() - 180000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'municipal_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 42.1,
          heading: 270,
          altitude: 140,
          accuracy: 4,
          batteryLevel: 95,
          vehicleType: 'fire_truck',
          driverName: 'Luis Torres'
        }
      },
      {
        deviceId: 'GPS_VEHICULO_MUNICIPAL_005',
        lat: -12.0308, // Pueblo Libre
        lng: -77.0645,
        timestamp: new Date(Date.now() - 240000).toISOString(),
        receivedAt: new Date().toISOString(),
        batchId: 'municipal_gps_batch_001',
        type: 'gps',
        metadata: {
          speed: 28.7,
          heading: 135,
          altitude: 165,
          accuracy: 6,
          batteryLevel: 68,
          vehicleType: 'maintenance',
          driverName: 'Ana Rodriguez'
        }
      }
    ];
    
    // Datos Mobile más variados (diferentes funcionarios municipales)
    const mobileTestData = [
      {
        userId: 'FUNCIONARIO_001',
        lat: -12.0464, // Plaza de Armas
        lng: -77.0428,
        timestamp: new Date().toISOString(),
        name: 'Dr. Roberto Vásquez',
        email: 'roberto.vasquez@munilima.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'funcionarios_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 10,
          batteryLevel: 80,
          networkType: '4G',
          appVersion: '2.1.0',
          deviceModel: 'Samsung Galaxy S23',
          osVersion: 'Android 14',
          isBackground: false,
          locationSource: 'gps',
          department: 'Salud Pública',
          position: 'Director'
        }
      },
      {
        userId: 'FUNCIONARIO_002',
        lat: -12.0697, // Miraflores
        lng: -77.0365,
        timestamp: new Date(Date.now() - 30000).toISOString(),
        name: 'Ing. Carmen López',
        email: 'carmen.lopez@munilima.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'funcionarios_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 15,
          batteryLevel: 65,
          networkType: 'WiFi',
          appVersion: '2.1.0',
          deviceModel: 'iPhone 15 Pro',
          osVersion: 'iOS 17.2',
          isBackground: true,
          locationSource: 'network',
          department: 'Obras Públicas',
          position: 'Jefe de Proyectos'
        }
      },
      {
        userId: 'FUNCIONARIO_003',
        lat: -12.1219, // San Juan de Miraflores
        lng: -76.9739,
        timestamp: new Date(Date.now() - 90000).toISOString(),
        name: 'Lic. Patricia Morales',
        email: 'patricia.morales@munilima.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'funcionarios_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 5,
          batteryLevel: 95,
          networkType: '5G',
          appVersion: '2.0.8',
          deviceModel: 'Xiaomi 14 Ultra',
          osVersion: 'Android 14',
          isBackground: false,
          locationSource: 'gps',
          department: 'Desarrollo Social',
          position: 'Coordinadora'
        }
      },
      {
        userId: 'FUNCIONARIO_004',
        lat: -11.9775, // San Isidro
        lng: -77.0645,
        timestamp: new Date(Date.now() - 150000).toISOString(),
        name: 'Arq. Miguel Herrera',
        email: 'miguel.herrera@munilima.gob.pe',
        receivedAt: new Date().toISOString(),
        batchId: 'funcionarios_mobile_batch_001',
        type: 'mobile',
        metadata: {
          speed: null,
          heading: null,
          altitude: null,
          accuracy: 8,
          batteryLevel: 72,
          networkType: '4G',
          appVersion: '2.1.0',
          deviceModel: 'Google Pixel 8',
          osVersion: 'Android 14',
          isBackground: false,
          locationSource: 'gps',
          department: 'Planificación Urbana',
          position: 'Arquitecto Senior'
        }
      }
    ];
    
    // Insertar datos usando el método batch para mayor eficiencia
    console.log('📍 Insertando datos GPS...');
    await redisRepo.addMultipleToList('gps:history:global', gpsTestData);
    console.log(`✅ ${gpsTestData.length} registros GPS insertados`);
    
    console.log('📱 Insertando datos Mobile...');
    await redisRepo.addMultipleToList('mobile:history:global', mobileTestData);
    console.log(`✅ ${mobileTestData.length} registros Mobile insertados`);
    
    // Verificar datos en Redis
    console.log('\n📊 Verificando datos en Redis...');
    const gpsStats = await redisRepo.getGPSStats();
    const mobileStats = await redisRepo.getMobileStats();
    console.log(`- GPS en Redis: ${gpsStats.totalRecords} registros`);
    console.log(`- Mobile en Redis: ${mobileStats.totalRecords} registros`);
    
    console.log('\n🔄 Procesando datos hacia BigQuery...');
    
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
      if (result.results.gps.gcsFile) {
        console.log('  - GCS File:', result.results.gps.gcsFile);
      }
      
      console.log('\n📱 Resultado Mobile:');
      console.log('  - Success:', result.results.mobile.success);
      console.log('  - Records:', result.results.mobile.recordsProcessed);
      console.log('  - Stage:', result.results.mobile.stage);
      if (result.results.mobile.jobId) {
        console.log('  - BigQuery Job ID:', result.results.mobile.jobId);
      }
      if (result.results.mobile.gcsFile) {
        console.log('  - GCS File:', result.results.mobile.gcsFile);
      }
    }
    
    // Verificar que Redis se limpió
    console.log('\n🔍 Verificando limpieza de Redis...');
    const finalGpsStats = await redisRepo.getGPSStats();
    const finalMobileStats = await redisRepo.getMobileStats();
    console.log(`- GPS restantes en Redis: ${finalGpsStats.totalRecords}`);
    console.log(`- Mobile restantes en Redis: ${finalMobileStats.totalRecords}`);
    
    await redisRepo.disconnect();
    await processor.cleanup();
    
    console.log('\n🎉 Datos de muestra agregados exitosamente');
    
    // Información para el usuario
    console.log('\n📋 INFORMACIÓN PARA REVISAR EN BIGQUERY:');
    console.log('🔗 Proyecto GCP:', process.env.GCP_PROJECT_ID);
    console.log('🗃️ Dataset:', process.env.BIGQUERY_DATASET_ID || 'location_data');
    console.log('📊 Tablas:');
    console.log('  - gps_records: Datos de vehículos municipales');
    console.log('  - mobile_records: Datos de funcionarios municipales');
    
    console.log('\n📁 ARCHIVOS EN GCS (NO ELIMINADOS):');
    console.log('🪣 Bucket:', process.env.GCS_BUCKET_NAME);
    console.log('📂 Carpetas:');
    console.log('  - gps/: Archivos de datos GPS');
    console.log('  - mobile/: Archivos de datos Mobile');
    
    console.log('\n💡 CONSULTAS SQL DE EJEMPLO:');
    console.log('-- Ver todos los datos GPS:');
    console.log(`SELECT * FROM \`${process.env.GCP_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID || 'location_data'}.gps_records\` ORDER BY timestamp DESC LIMIT 10;`);
    console.log('\n-- Ver todos los datos Mobile:');
    console.log(`SELECT * FROM \`${process.env.GCP_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID || 'location_data'}.mobile_records\` ORDER BY timestamp DESC LIMIT 10;`);
    console.log('\n-- Contar registros por tipo:');
    console.log(`SELECT 'GPS' as tipo, COUNT(*) as total FROM \`${process.env.GCP_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID || 'location_data'}.gps_records\``);
    console.log(`UNION ALL`);
    console.log(`SELECT 'Mobile' as tipo, COUNT(*) as total FROM \`${process.env.GCP_PROJECT_ID}.${process.env.BIGQUERY_DATASET_ID || 'location_data'}.mobile_records\`;`);
    
    if (result.success && result.recordsProcessed > 0) {
      console.log('\n🎯 ¡ÉXITO! Los datos están ahora disponibles en BigQuery y GCS');
      console.log(`   📊 Total procesado: ${result.recordsProcessed} registros`);
      console.log(`   ⏱️ Tiempo: ${result.processingTime}ms`);
      console.log('   📁 Los archivos GCS NO fueron eliminados para tu inspección');
    }
    
  } catch (error) {
    console.error('❌ Error agregando datos de muestra:', error.message);
    console.error('Stack:', error.stack);
  }
}

addSampleDataToBigQuery().then(() => {
  console.log('\n✅ Proceso completado');
  process.exit(0);
}).catch(error => {
  console.error('💥 Error fatal:', error.message);
  process.exit(1);
});