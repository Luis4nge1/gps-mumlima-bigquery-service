#!/usr/bin/env node

/**
 * Prueba básica del AtomicRedisProcessor sin conexión Redis real
 */

import { AtomicRedisProcessor } from '../src/services/AtomicRedisProcessor.js';
import { logger } from '../src/utils/logger.js';

async function testBasicFunctionality() {
  logger.info('🧪 Iniciando prueba básica del AtomicRedisProcessor...');
  
  const processor = new AtomicRedisProcessor();
  
  try {
    // Probar estado inicial
    logger.info('📊 Verificando estado inicial...');
    const initialStats = await processor.getStats();
    logger.info('✅ Estado inicial:', {
      initialized: initialStats.initialized,
      hasError: !!initialStats.error
    });
    
    // Probar health check sin inicializar
    logger.info('🏥 Verificando health check sin inicializar...');
    const healthBefore = await processor.healthCheck();
    logger.info('✅ Health check antes:', {
      healthy: healthBefore.healthy,
      initialized: healthBefore.initialized
    });
    
    // Probar cleanup
    logger.info('🧹 Probando cleanup...');
    await processor.cleanup();
    logger.info('✅ Cleanup completado');
    
    // Verificar que la clase tiene los métodos requeridos
    logger.info('🔍 Verificando métodos requeridos...');
    const requiredMethods = [
      'extractAndClearGPSData',
      'extractAndClearMobileData', 
      'extractAllData',
      'initialize',
      'getStats',
      'healthCheck',
      'cleanup'
    ];
    
    const missingMethods = requiredMethods.filter(method => 
      typeof processor[method] !== 'function'
    );
    
    if (missingMethods.length > 0) {
      throw new Error(`Métodos faltantes: ${missingMethods.join(', ')}`);
    }
    
    logger.info('✅ Todos los métodos requeridos están presentes');
    
    // Verificar propiedades iniciales
    logger.info('🔍 Verificando propiedades iniciales...');
    if (typeof processor.isInitialized !== 'boolean') {
      throw new Error('Propiedad isInitialized debe ser boolean');
    }
    
    if (!processor.redisRepo) {
      throw new Error('Propiedad redisRepo debe estar presente');
    }
    
    logger.info('✅ Propiedades iniciales correctas');
    
    logger.info('🎉 Prueba básica completada exitosamente');
    
    return {
      success: true,
      message: 'AtomicRedisProcessor implementado correctamente',
      methodsVerified: requiredMethods.length,
      propertiesVerified: 2
    };
    
  } catch (error) {
    logger.error('❌ Error en prueba básica:', error.message);
    return {
      success: false,
      error: error.message
    };
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  testBasicFunctionality()
    .then(result => {
      console.log('\n🎯 Resultado de prueba básica:');
      console.log(JSON.stringify(result, null, 2));
      process.exit(result.success ? 0 : 1);
    })
    .catch(error => {
      console.error('💥 Error fatal:', error.message);
      process.exit(1);
    });
}

export { testBasicFunctionality };