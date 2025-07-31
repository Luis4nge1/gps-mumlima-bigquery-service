#!/usr/bin/env node

/**
 * Script para probar el procesamiento GPS paso a paso
 */

console.log('🔄 Test de Procesamiento GPS');
console.log('='.repeat(40));

try {
  // Cargar configuración
  console.log('📦 Cargando configuración...');
  const { config } = await import('../src/config/env.js');
  console.log(`   ✅ Configuración cargada`);
  console.log(`   📊 Redis: ${config.redis.host}:${config.redis.port}`);
  console.log(`   📍 GPS Key: ${config.gps.listKey}`);
  console.log(`   📁 Archivo salida: ${config.gps.outputFilePath}`);

  // Test Redis Repository
  console.log('\n🔗 Probando RedisRepository...');
  const { RedisRepository } = await import('../src/repositories/RedisRepository.js');
  const repo = new RedisRepository();
  
  // Conectar y obtener estadísticas
  const stats = await repo.getGPSStats();
  console.log(`   ✅ Conectado a Redis`);
  console.log(`   📊 Registros disponibles: ${stats.totalRecords}`);
  
  if (stats.totalRecords === 0) {
    console.log('   ❌ No hay datos para procesar');
    process.exit(1);
  }
  
  // Obtener algunos datos de muestra
  console.log('\n📍 Obteniendo datos GPS...');
  const gpsData = await repo.getAllGPSData();
  console.log(`   ✅ Obtenidos ${gpsData.length} registros`);
  console.log(`   📄 Primer registro: ${JSON.stringify(gpsData[0]).substring(0, 100)}...`);
  
  // Test BigQuery Adapter
  console.log('\n📤 Probando BigQueryAdapter...');
  const { BigQueryAdapter } = await import('../src/adapters/BigQueryAdapter.js');
  const adapter = new BigQueryAdapter();
  
  // Inicializar adapter
  await adapter.initialize();
  console.log('   ✅ BigQueryAdapter inicializado');
  
  // Procesar solo los primeros 3 registros para prueba
  const testData = gpsData.slice(0, 3);
  console.log(`   🔄 Procesando ${testData.length} registros de prueba...`);
  
  const result = await adapter.uploadData(testData);
  console.log(`   📊 Resultado: ${result.success ? '✅ Exitoso' : '❌ Error'}`);
  
  if (result.success) {
    console.log(`   📁 Archivo: ${result.outputFile}`);
    console.log(`   📏 Tamaño: ${result.fileSize} bytes`);
    console.log(`   📊 Registros: ${result.recordsProcessed}`);
    
    // Verificar que el archivo existe
    const fs = await import('fs/promises');
    try {
      await fs.access(result.outputFile);
      console.log('   ✅ Archivo creado exitosamente');
      
      // Mostrar contenido
      const content = await fs.readFile(result.outputFile, 'utf8');
      const lines = content.split('\n').slice(0, 15);
      console.log('\n📄 Contenido del archivo:');
      lines.forEach((line, i) => {
        if (line.trim()) {
          console.log(`   ${i + 1}. ${line.substring(0, 80)}${line.length > 80 ? '...' : ''}`);
        }
      });
      
    } catch (error) {
      console.log(`   ❌ Error verificando archivo: ${error.message}`);
    }
    
  } else {
    console.log(`   ❌ Error: ${result.error}`);
  }
  
  // Limpiar conexión
  await repo.disconnect();
  
  console.log('\n✅ Test de procesamiento completado');
  
} catch (error) {
  console.error('❌ Error en test:', error.message);
  console.error('Stack:', error.stack);
  process.exit(1);
}