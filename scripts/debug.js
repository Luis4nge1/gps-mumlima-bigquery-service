#!/usr/bin/env node

/**
 * Script de diagnóstico para el microservicio GPS-BigQuery
 */

console.log('🚀 Iniciando diagnóstico...');

// Variables globales para los módulos
let config, logger, RedisRepository, GPSProcessorService, fs;

async function loadModules() {
  try {
    console.log('📦 Cargando configuración...');
    const configModule = await import('../src/config/env.js');
    config = configModule.config;

    console.log('📦 Cargando logger...');
    const loggerModule = await import('../src/utils/logger.js');
    logger = loggerModule.logger;

    console.log('📦 Cargando RedisRepository...');
    const redisModule = await import('../src/repositories/RedisRepository.js');
    RedisRepository = redisModule.RedisRepository;

    console.log('📦 Cargando GPSProcessorService...');
    const processorModule = await import('../src/services/GPSProcessorService.js');
    GPSProcessorService = processorModule.GPSProcessorService;

    console.log('📦 Cargando fs...');
    fs = await import('fs/promises');

    console.log('✅ Todos los módulos cargados correctamente\n');
    return true;
  } catch (error) {
    console.error('❌ Error cargando módulos:', error.message);
    console.error('Stack:', error.stack);
    return false;
  }
}

async function diagnosticRedis() {
  console.log('🔍 Diagnóstico Redis...');

  try {
    const repo = new RedisRepository();

    // Test conexión
    const connected = await repo.ping();
    console.log(`   Conexión Redis: ${connected ? '✅ OK' : '❌ Error'}`);

    if (!connected) {
      console.log('   💡 Verifica que Redis esté ejecutándose y la configuración en .env');
      return false;
    }

    // Verificar datos
    const stats = await repo.getGPSStats();
    console.log(`   Registros en Redis: ${stats.totalRecords}`);
    console.log(`   Clave Redis: ${stats.key}`);

    if (stats.totalRecords === 0) {
      console.log('   ⚠️ No hay datos GPS en Redis');
      console.log('   💡 Agrega datos de prueba con:');
      console.log('   redis-cli LPUSH gps:history:global \'{"latitude": -12.0464, "longitude": -77.0428, "device_id": "test"}\'');
      return false;
    }

    // Mostrar algunos datos
    const data = await repo.getGPSDataBatch(0, 2);
    console.log('   📍 Datos de ejemplo:');
    data.forEach((record, i) => {
      console.log(`      ${i + 1}. ${JSON.stringify(record).substring(0, 80)}...`);
    });

    await repo.disconnect();
    return true;

  } catch (error) {
    console.log(`   ❌ Error: ${error.message}`);
    return false;
  }
}

async function diagnosticConfig() {
  console.log('⚙️ Diagnóstico Configuración...');

  console.log(`   Redis Host: ${config.redis.host}:${config.redis.port}`);
  console.log(`   GPS Key: ${config.gps.listKey}`);
  console.log(`   Archivo salida: ${config.gps.outputFilePath}`);
  console.log(`   Scheduler habilitado: ${config.scheduler.enabled}`);
  console.log(`   Intervalo: ${config.scheduler.intervalMinutes} minutos`);

  // Verificar directorio de salida
  try {
    const outputDir = config.gps.outputFilePath.split('/').slice(0, -1).join('/');
    await fs.access(outputDir);
    console.log(`   Directorio salida: ✅ ${outputDir} existe`);
  } catch (error) {
    console.log(`   Directorio salida: ❌ No existe, creando...`);
    const outputDir = config.gps.outputFilePath.split('/').slice(0, -1).join('/');
    await fs.mkdir(outputDir, { recursive: true });
    console.log(`   Directorio salida: ✅ Creado ${outputDir}`);
  }
}

async function diagnosticProcessing() {
  console.log('🔄 Diagnóstico Procesamiento...');

  try {
    const processor = new GPSProcessorService();

    // Health check
    const health = await processor.healthCheck();
    console.log(`   Health check: ${health.healthy ? '✅ Healthy' : '❌ Unhealthy'}`);

    if (!health.healthy) {
      console.log('   Servicios:', health.services);
      return false;
    }

    // Ejecutar procesamiento de prueba
    console.log('   🔄 Ejecutando procesamiento de prueba...');
    const result = await processor.processGPSData();

    console.log(`   Resultado: ${result.success ? '✅ Exitoso' : '❌ Error'}`);
    console.log(`   Registros procesados: ${result.recordsProcessed || 0}`);

    if (result.success && result.recordsProcessed > 0) {
      console.log(`   Archivo generado: ${result.outputFile}`);
      console.log(`   Tamaño archivo: ${result.fileSize} bytes`);

      // Verificar que el archivo existe
      try {
        await fs.access(result.outputFile);
        console.log('   ✅ Archivo TXT creado exitosamente');

        // Mostrar primeras líneas del archivo
        const content = await fs.readFile(result.outputFile, 'utf8');
        const lines = content.split('\n').slice(0, 10);
        console.log('   📄 Contenido del archivo (primeras líneas):');
        lines.forEach((line, i) => {
          if (line.trim()) {
            console.log(`      ${i + 1}. ${line.substring(0, 80)}${line.length > 80 ? '...' : ''}`);
          }
        });

      } catch (error) {
        console.log(`   ❌ Error verificando archivo: ${error.message}`);
      }
    } else if (result.error) {
      console.log(`   Error: ${result.error}`);
    }

    await processor.cleanup();
    return result.success;

  } catch (error) {
    console.log(`   ❌ Error en procesamiento: ${error.message}`);
    return false;
  }
}

async function addTestData() {
  console.log('📍 Agregando datos de prueba...');

  try {
    const repo = new RedisRepository();
    await repo.connect();

    const testData = [
      {
        latitude: -12.0464,
        longitude: -77.0428,
        timestamp: new Date().toISOString(),
        device_id: 'test_device_1',
        speed: 45,
        heading: 180
      },
      {
        latitude: -12.0500,
        longitude: -77.0500,
        timestamp: new Date().toISOString(),
        device_id: 'test_device_2',
        speed: 30,
        heading: 90
      },
      {
        latitude: -12.0600,
        longitude: -77.0600,
        timestamp: new Date().toISOString(),
        device_id: 'test_device_3',
        speed: 60,
        heading: 270
      }
    ];

    // Agregar datos a Redis
    const client = await repo.connect();
    for (const data of testData) {
      await client.lpush(config.gps.listKey, JSON.stringify(data));
    }

    console.log(`   ✅ Agregados ${testData.length} registros de prueba`);
    await repo.disconnect();

  } catch (error) {
    console.log(`   ❌ Error agregando datos: ${error.message}`);
  }
}

async function main() {
  console.log('🚀 Diagnóstico GPS BigQuery Microservice\n');

  // Cargar módulos primero
  const modulesLoaded = await loadModules();
  if (!modulesLoaded) {
    console.log('❌ No se pudieron cargar los módulos. Revisa los errores arriba.');
    return;
  }

  const args = process.argv.slice(2);

  if (args.includes('--add-test-data')) {
    await addTestData();
    console.log('\n✅ Datos de prueba agregados. Ahora ejecuta: npm run start:once\n');
    return;
  }

  // Diagnóstico completo
  await diagnosticConfig();
  console.log();

  const redisOk = await diagnosticRedis();
  console.log();

  if (redisOk) {
    const processingOk = await diagnosticProcessing();
    console.log();

    if (processingOk) {
      console.log('🎉 ¡Todo funciona correctamente!');
      console.log('💡 Para ejecutar continuamente: npm start');
      console.log('💡 Para desarrollo: npm run dev');
    } else {
      console.log('❌ Hay problemas con el procesamiento');
    }
  } else {
    console.log('❌ Hay problemas con Redis o no hay datos');
    console.log('💡 Para agregar datos de prueba: npm run add-test-data');
  }
}

// Ejecutar si es llamado directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(error => {
    console.error('❌ Error en diagnóstico:', error.message);
    process.exit(1);
  });
}