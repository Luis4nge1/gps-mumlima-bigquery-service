#!/usr/bin/env node

/**
 * Script de prueba para verificar la lógica de deduplicación en GCS
 */

import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import { logger } from '../src/utils/logger.js';

async function testGCSDeduplication() {
  const processor = new GPSProcessorService();
  
  try {
    logger.info('🧪 Iniciando prueba de deduplicación GCS...');
    
    // Inicializar el procesador
    await processor.initialize();
    
    // Obtener estadísticas iniciales
    const initialStats = await processor.getProcessorStats();
    logger.info('📊 Estadísticas iniciales:', {
      redis: initialStats.redis,
      gcs: initialStats.gcs,
      recovery: initialStats.recovery
    });
    
    // Ejecutar procesamiento (primera vez)
    logger.info('🔄 Ejecutando primer procesamiento...');
    const firstResult = await processor.processGPSData();
    logger.info('✅ Primer procesamiento completado:', {
      success: firstResult.success,
      recordsProcessed: firstResult.recordsProcessed,
      results: firstResult.results
    });
    
    // Esperar un momento
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Ejecutar procesamiento nuevamente (debería detectar archivos existentes)
    logger.info('🔄 Ejecutando segundo procesamiento (debería detectar duplicados)...');
    const secondResult = await processor.processGPSData();
    logger.info('✅ Segundo procesamiento completado:', {
      success: secondResult.success,
      recordsProcessed: secondResult.recordsProcessed,
      results: secondResult.results
    });
    
    // Obtener estadísticas finales
    const finalStats = await processor.getProcessorStats();
    logger.info('📊 Estadísticas finales:', {
      redis: finalStats.redis,
      gcs: finalStats.gcs,
      recovery: finalStats.recovery
    });
    
    // Verificar health check
    const health = await processor.healthCheck();
    logger.info('🏥 Health check:', health);
    
    logger.info('✅ Prueba de deduplicación completada exitosamente');
    
  } catch (error) {
    logger.error('❌ Error en prueba de deduplicación:', error.message);
    console.error(error);
  } finally {
    // Limpiar recursos
    await processor.cleanup();
  }
}

// Ejecutar prueba si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  testGCSDeduplication()
    .then(() => {
      logger.info('🎉 Prueba completada');
      process.exit(0);
    })
    .catch((error) => {
      logger.error('💥 Error fatal en prueba:', error.message);
      process.exit(1);
    });
}

export { testGCSDeduplication };