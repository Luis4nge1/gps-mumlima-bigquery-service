#!/usr/bin/env node

/**
 * Test completo del procesamiento GPS
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('🔄 Iniciando test completo de procesamiento...');

async function testFullProcessing() {
  try {
    const { GPSProcessorService } = await import('../src/services/GPSProcessorService.js');
    
    console.log('🔧 Inicializando GPSProcessorService...');
    const processor = new GPSProcessorService();
    
    await processor.initialize();
    console.log('✅ GPSProcessorService inicializado');
    
    // Obtener estadísticas iniciales
    console.log('\n📊 Estadísticas iniciales:');
    const initialStats = await processor.getProcessorStats();
    console.log('Redis:', initialStats.redis);
    console.log('GCS:', initialStats.gcs);
    console.log('BigQuery:', initialStats.bigQuery);
    
    // Ejecutar procesamiento
    console.log('\n🔄 Ejecutando procesamiento...');
    const result = await processor.processGPSData();
    
    console.log('\n✅ Resultado del procesamiento:');
    console.log('Success:', result.success);
    console.log('Records Processed:', result.recordsProcessed);
    console.log('Processing Time:', result.processingTime, 'ms');
    
    if (result.results) {
      console.log('GPS Result:', result.results.gps);
      console.log('Mobile Result:', result.results.mobile);
    }
    
    if (result.error) {
      console.error('Error:', result.error);
    }
    
    // Estadísticas finales
    console.log('\n📊 Estadísticas finales:');
    const finalStats = await processor.getProcessorStats();
    console.log('Redis:', finalStats.redis);
    
    await processor.cleanup();
    
  } catch (error) {
    console.error('❌ Error en test completo:', error.message);
    console.error('Stack:', error.stack);
  }
}

testFullProcessing().then(() => {
  console.log('\n🎉 Test completado');
}).catch(console.error);