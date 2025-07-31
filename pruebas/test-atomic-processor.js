#!/usr/bin/env node

/**
 * Script de prueba para AtomicRedisProcessor
 * Verifica que el procesador funcione correctamente con Redis real
 */

import { AtomicRedisProcessor } from '../src/services/AtomicRedisProcessor.js';
import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { logger } from '../src/utils/logger.js';

async function testAtomicProcessor() {
  const processor = new AtomicRedisProcessor();
  const redisRepo = new RedisRepository();
  
  try {
    logger.info('🧪 Iniciando prueba del AtomicRedisProcessor...');
    
    // Paso 1: Inicializar
    await processor.initialize();
    await redisRepo.connect();
    
    // Paso 2: Agregar datos de prueba
    logger.info('📝 Agregando datos de prueba...');
    
    const testGPSData = [
      { deviceId: 'test-device-1', lat: -12.0464, lng: -77.0428, timestamp: new Date().toISOString() },
      { deviceId: 'test-device-2', lat: -12.0465, lng: -77.0429, timestamp: new Date().toISOString() }
    ];
    
    const testMobileData = [
      { userId: 'test-user-1', name: 'Test User 1', email: 'test1@example.com', timestamp: new Date().toISOString() },
      { userId: 'test-user-2', name: 'Test User 2', email: 'test2@example.com', timestamp: new Date().toISOString() }
    ];
    
    // Agregar datos a Redis
    await redisRepo.addMultipleToList('gps:history:global', testGPSData);
    await redisRepo.addMultipleToList('mobile:history:global', testMobileData);
    
    // Paso 3: Verificar datos iniciales
    const initialStats = await processor.getStats();
    logger.info('📊 Estadísticas iniciales:', JSON.stringify(initialStats.redis, null, 2));
    
    // Paso 4: Probar extracción atómica
    logger.info('🔄 Probando extracción atómica...');
    const result = await processor.extractAllData();
    
    logger.info('✅ Resultado de extracción:', {
      success: result.success,
      totalRecords: result.totalRecords,
      gpsRecords: result.gps.recordCount,
      mobileRecords: result.mobile.recordCount,
      allCleared: result.allCleared,
      extractionTime: result.extractionTime
    });
    
    // Paso 5: Verificar que Redis está limpio
    const finalStats = await processor.getStats();
    logger.info('📊 Estadísticas finales:', JSON.stringify(finalStats.redis, null, 2));
    
    // Paso 6: Verificar que nuevos datos pueden agregarse
    logger.info('🔄 Probando que nuevos datos pueden agregarse...');
    await redisRepo.addToList('gps:history:global', { deviceId: 'new-device', lat: -12.0466, lng: -77.0430 });
    
    const newStats = await processor.getStats();
    logger.info('📊 Estadísticas después de agregar nuevo dato:', JSON.stringify(newStats.redis, null, 2));
    
    // Limpiar datos de prueba
    await redisRepo.clearListData('gps:history:global');
    await redisRepo.clearListData('mobile:history:global');
    
    logger.info('✅ Prueba del AtomicRedisProcessor completada exitosamente');
    
    return {
      success: true,
      extractedRecords: result.totalRecords,
      processingTime: result.extractionTime
    };
    
  } catch (error) {
    logger.error('❌ Error en prueba del AtomicRedisProcessor:', error.message);
    return {
      success: false,
      error: error.message
    };
  } finally {
    // Limpiar recursos
    await processor.cleanup();
    await redisRepo.disconnect();
  }
}

// Ejecutar prueba si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  testAtomicProcessor()
    .then(result => {
      console.log('\n🎯 Resultado final:', JSON.stringify(result, null, 2));
      process.exit(result.success ? 0 : 1);
    })
    .catch(error => {
      console.error('💥 Error fatal:', error.message);
      process.exit(1);
    });
}

export { testAtomicProcessor };