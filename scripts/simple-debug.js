#!/usr/bin/env node

/**
 * Script de diagnóstico simple para el microservicio GPS-BigQuery
 */

console.log('🚀 Diagnóstico Simple GPS BigQuery Microservice');
console.log('='.repeat(50));

// Función para manejar errores de forma segura
function safeLog(message) {
  try {
    console.log(message);
  } catch (error) {
    console.error('Error en log:', error.message);
  }
}

// Paso 1: Verificar Node.js
safeLog('\n1️⃣ Verificando Node.js...');
safeLog(`   Versión Node.js: ${process.version}`);
safeLog(`   Plataforma: ${process.platform}`);

// Paso 2: Verificar archivos básicos
safeLog('\n2️⃣ Verificando archivos...');
try {
  const fs = await import('fs/promises');
  
  // Verificar .env
  try {
    await fs.access('.env');
    safeLog('   ✅ Archivo .env existe');
  } catch {
    safeLog('   ❌ Archivo .env no existe');
    safeLog('   💡 Ejecuta: cp .env.example .env');
  }
  
  // Verificar directorio tmp
  try {
    await fs.access('tmp');
    safeLog('   ✅ Directorio tmp/ existe');
  } catch {
    safeLog('   ❌ Directorio tmp/ no existe');
    safeLog('   💡 Ejecuta: mkdir -p tmp/backup');
  }
  
  // Verificar src/
  try {
    await fs.access('src');
    safeLog('   ✅ Directorio src/ existe');
  } catch {
    safeLog('   ❌ Directorio src/ no existe');
  }
  
} catch (error) {
  safeLog(`   ❌ Error verificando archivos: ${error.message}`);
}

// Paso 3: Verificar configuración básica
safeLog('\n3️⃣ Verificando configuración...');
try {
  // Cargar variables de entorno manualmente
  const dotenv = await import('dotenv');
  dotenv.config();
  
  const redisHost = process.env.REDIS_HOST || 'localhost';
  const redisPort = process.env.REDIS_PORT || '6379';
  const gpsKey = process.env.GPS_LIST_KEY || 'gps:history:global';
  
  safeLog(`   Redis Host: ${redisHost}:${redisPort}`);
  safeLog(`   GPS Key: ${gpsKey}`);
  safeLog(`   Scheduler habilitado: ${process.env.SCHEDULER_ENABLED || 'true'}`);
  
} catch (error) {
  safeLog(`   ❌ Error cargando configuración: ${error.message}`);
}

// Paso 4: Test Redis básico
safeLog('\n4️⃣ Verificando Redis...');
try {
  const Redis = await import('ioredis');
  
  const redis = new Redis.default({
    host: process.env.REDIS_HOST || 'localhost',
    port: parseInt(process.env.REDIS_PORT) || 6379,
    password: process.env.REDIS_PASSWORD || null,
    db: parseInt(process.env.REDIS_DB) || 0,
    lazyConnect: true,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3
  });
  
  // Test conexión
  await redis.connect();
  const pong = await redis.ping();
  
  if (pong === 'PONG') {
    safeLog('   ✅ Conexión Redis exitosa');
    
    // Verificar datos
    const gpsKey = process.env.GPS_LIST_KEY || 'gps:history:global';
    const count = await redis.llen(gpsKey);
    safeLog(`   📊 Registros en Redis: ${count}`);
    
    if (count === 0) {
      safeLog('   ⚠️ No hay datos GPS en Redis');
      safeLog('   💡 Agrega datos de prueba con el comando:');
      safeLog(`   redis-cli LPUSH ${gpsKey} '{"latitude": -12.0464, "longitude": -77.0428, "device_id": "test"}'`);
    } else {
      // Mostrar algunos datos
      const samples = await redis.lrange(gpsKey, 0, 1);
      safeLog('   📍 Datos de ejemplo:');
      samples.forEach((sample, i) => {
        safeLog(`      ${i + 1}. ${sample.substring(0, 60)}...`);
      });
    }
  } else {
    safeLog('   ❌ Redis respondió pero no con PONG');
  }
  
  await redis.quit();
  
} catch (error) {
  safeLog(`   ❌ Error conectando a Redis: ${error.message}`);
  safeLog('   💡 Verifica que Redis esté ejecutándose:');
  safeLog('   redis-server');
  safeLog('   💡 O verifica la configuración en .env');
}

// Paso 5: Test de procesamiento simple
safeLog('\n5️⃣ Test de procesamiento...');
try {
  // Intentar importar el servicio principal
  const { GPSBigQueryService } = await import('../src/index.js');
  safeLog('   ✅ Módulo principal se puede importar');
  
  // Crear instancia del servicio
  const service = new GPSBigQueryService();
  safeLog('   ✅ Servicio se puede instanciar');
  
} catch (error) {
  safeLog(`   ❌ Error importando servicio: ${error.message}`);
  safeLog('   💡 Revisa los errores en el código fuente');
}

// Resumen final
safeLog('\n📋 Resumen:');
safeLog('   Para agregar datos de prueba:');
safeLog('   npm run add-test-data');
safeLog('');
safeLog('   Para ejecutar una vez:');
safeLog('   npm run start:once');
safeLog('');
safeLog('   Para ejecutar continuamente:');
safeLog('   npm start');
safeLog('');
safeLog('   Para desarrollo:');
safeLog('   npm run dev');

safeLog('\n✅ Diagnóstico simple completado');