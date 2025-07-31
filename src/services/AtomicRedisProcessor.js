import { RedisRepository } from '../repositories/RedisRepository.js';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';

/**
 * Procesador atómico de Redis que extrae todos los datos y limpia inmediatamente
 * para evitar pérdida de datos durante el procesamiento.
 * 
 * Flujo atómico:
 * 1. Extraer TODOS los datos de Redis
 * 2. Limpiar Redis inmediatamente 
 * 3. Procesar datos extraídos
 * 4. Nuevos datos van a Redis limpio
 */
export class AtomicRedisProcessor {
  constructor() {
    this.redisRepo = new RedisRepository();
    this.isInitialized = false;
    this.atomicProcessingEnabled = config.backup.atomicProcessingEnabled;
  }

  /**
   * Inicializa el procesador conectando a Redis
   */
  async initialize() {
    try {
      logger.info('🔧 Inicializando AtomicRedisProcessor...', {
        atomicProcessingEnabled: this.atomicProcessingEnabled,
        featureFlag: 'ATOMIC_PROCESSING_ENABLED'
      });
      
      if (!this.atomicProcessingEnabled) {
        logger.warn('⚠️ ADVERTENCIA: Procesamiento atómico DESHABILITADO por feature flag');
        logger.warn('   Para habilitar, configurar ATOMIC_PROCESSING_ENABLED=true');
        logger.warn('   El sistema usará procesamiento legacy con riesgo de pérdida de datos');
      }
      
      await this.redisRepo.connect();
      this.isInitialized = true;
      
      logger.info('✅ AtomicRedisProcessor inicializado exitosamente', {
        atomicProcessingEnabled: this.atomicProcessingEnabled,
        mode: this.atomicProcessingEnabled ? 'atomic' : 'legacy'
      });
    } catch (error) {
      logger.error('❌ Error inicializando AtomicRedisProcessor:', {
        error: error.message,
        stack: error.stack,
        atomicProcessingEnabled: this.atomicProcessingEnabled
      });
      throw error;
    }
  }

  /**
   * Verifica si el procesamiento atómico está habilitado
   * @returns {boolean} True si está habilitado
   */
  isAtomicProcessingEnabled() {
    return this.atomicProcessingEnabled;
  }

  /**
   * Obtiene el modo de procesamiento actual
   * @returns {string} 'atomic' o 'legacy'
   */
  getProcessingMode() {
    return this.atomicProcessingEnabled ? 'atomic' : 'legacy';
  }

  /**
   * Habilita o deshabilita el procesamiento atómico en tiempo de ejecución
   * @param {boolean} enabled - True para habilitar, false para deshabilitar
   */
  setAtomicProcessingEnabled(enabled) {
    const previousState = this.atomicProcessingEnabled;
    this.atomicProcessingEnabled = enabled;
    
    logger.info(`🔧 Feature flag de procesamiento atómico cambiado: ${previousState} → ${enabled}`, {
      previousMode: previousState ? 'atomic' : 'legacy',
      newMode: enabled ? 'atomic' : 'legacy',
      runtimeChange: true
    });
    
    if (!enabled) {
      logger.warn('⚠️ ADVERTENCIA: Procesamiento atómico DESHABILITADO - riesgo de pérdida de datos');
    }
  }

  /**
   * Extrae todos los datos GPS de forma atómica y limpia la key inmediatamente
   * @returns {Object} Resultado con datos extraídos y estadísticas
   */
  async extractAndClearGPSData() {
    const startTime = Date.now();
    const gpsKey = config.gps.listKey; // 'gps:history:global'
    
    try {
      if (!this.atomicProcessingEnabled) {
        const error = 'Procesamiento atómico deshabilitado por feature flag ATOMIC_PROCESSING_ENABLED=false';
        logger.error(`❌ ${error}`);
        return {
          success: false,
          error: error,
          data: [],
          recordCount: 0,
          extractionTime: 0,
          totalTime: Date.now() - startTime,
          key: gpsKey,
          cleared: false,
          featureFlagDisabled: true
        };
      }

      logger.info(`🔄 Iniciando extracción atómica de datos GPS desde ${gpsKey}...`, {
        mode: 'atomic',
        featureFlag: 'enabled'
      });
      
      if (!this.isInitialized) {
        await this.initialize();
      }

      // Paso 1: Obtener estadísticas iniciales para logging
      const initialStats = await this.redisRepo.getGPSStats();
      logger.info(`📊 Estadísticas GPS iniciales: ${initialStats.totalRecords} registros, ${initialStats.memoryUsage} bytes`);

      if (initialStats.totalRecords === 0) {
        logger.info('📍 No hay datos GPS para extraer');
        return {
          success: true,
          data: [],
          recordCount: 0,
          extractionTime: Date.now() - startTime,
          key: gpsKey,
          cleared: false
        };
      }

      // Paso 2: Extracción atómica - obtener TODOS los datos
      logger.info(`📥 Extrayendo TODOS los ${initialStats.totalRecords} registros GPS...`);
      const extractedData = await this.redisRepo.getListData(gpsKey);
      
      const extractionTime = Date.now() - startTime;
      logger.info(`✅ Extracción GPS completada: ${extractedData.length} registros en ${extractionTime}ms`);

      // Paso 3: Limpieza inmediata de Redis
      logger.info(`🗑️ Limpiando key GPS ${gpsKey} inmediatamente...`);
      const clearStartTime = Date.now();
      
      const cleared = await this.redisRepo.clearListData(gpsKey);
      const clearTime = Date.now() - clearStartTime;
      
      if (cleared) {
        logger.info(`✅ Key GPS ${gpsKey} limpiada exitosamente en ${clearTime}ms`);
        logger.info(`🔄 Redis GPS ahora disponible para nuevos datos`);
      } else {
        logger.warn(`⚠️ No se pudo limpiar key GPS ${gpsKey} (posiblemente ya estaba vacía)`);
      }

      // Paso 4: Verificar que Redis está limpio
      const finalStats = await this.redisRepo.getGPSStats();
      logger.info(`📊 Estadísticas GPS finales: ${finalStats.totalRecords} registros (debe ser 0)`);

      if (finalStats.totalRecords > 0) {
        logger.warn(`⚠️ ADVERTENCIA: Aún quedan ${finalStats.totalRecords} registros GPS en Redis después de la limpieza`);
      }

      const totalTime = Date.now() - startTime;
      logger.info(`✅ Extracción atómica GPS completada: ${extractedData.length} registros procesados en ${totalTime}ms total`);

      return {
        success: true,
        data: extractedData,
        recordCount: extractedData.length,
        extractionTime: extractionTime,
        clearTime: clearTime,
        totalTime: totalTime,
        key: gpsKey,
        cleared: cleared,
        initialRecords: initialStats.totalRecords,
        finalRecords: finalStats.totalRecords
      };

    } catch (error) {
      const totalTime = Date.now() - startTime;
      logger.error(`❌ Error en extracción atómica GPS (${totalTime}ms):`, error.message);
      
      // Intentar obtener estadísticas para debugging
      try {
        const errorStats = await this.redisRepo.getGPSStats();
        logger.error(`📊 Estadísticas GPS en error: ${errorStats.totalRecords} registros`);
      } catch (statsError) {
        logger.error('❌ No se pudieron obtener estadísticas GPS en error:', statsError.message);
      }

      return {
        success: false,
        error: error.message,
        data: [],
        recordCount: 0,
        extractionTime: 0,
        totalTime: totalTime,
        key: gpsKey,
        cleared: false
      };
    }
  }

  /**
   * Extrae todos los datos Mobile de forma atómica y limpia la key inmediatamente
   * @returns {Object} Resultado con datos extraídos y estadísticas
   */
  async extractAndClearMobileData() {
    const startTime = Date.now();
    const mobileKey = 'mobile:history:global';
    
    try {
      if (!this.atomicProcessingEnabled) {
        const error = 'Procesamiento atómico deshabilitado por feature flag ATOMIC_PROCESSING_ENABLED=false';
        logger.error(`❌ ${error}`);
        return {
          success: false,
          error: error,
          data: [],
          recordCount: 0,
          extractionTime: 0,
          totalTime: Date.now() - startTime,
          key: mobileKey,
          cleared: false,
          featureFlagDisabled: true
        };
      }

      logger.info(`🔄 Iniciando extracción atómica de datos Mobile desde ${mobileKey}...`, {
        mode: 'atomic',
        featureFlag: 'enabled'
      });
      
      if (!this.isInitialized) {
        await this.initialize();
      }

      // Paso 1: Obtener estadísticas iniciales para logging
      const initialStats = await this.redisRepo.getMobileStats();
      logger.info(`📊 Estadísticas Mobile iniciales: ${initialStats.totalRecords} registros, ${initialStats.memoryUsage} bytes`);

      if (initialStats.totalRecords === 0) {
        logger.info('📱 No hay datos Mobile para extraer');
        return {
          success: true,
          data: [],
          recordCount: 0,
          extractionTime: Date.now() - startTime,
          key: mobileKey,
          cleared: false
        };
      }

      // Paso 2: Extracción atómica - obtener TODOS los datos
      logger.info(`📥 Extrayendo TODOS los ${initialStats.totalRecords} registros Mobile...`);
      const extractedData = await this.redisRepo.getListData(mobileKey);
      
      const extractionTime = Date.now() - startTime;
      logger.info(`✅ Extracción Mobile completada: ${extractedData.length} registros en ${extractionTime}ms`);

      // Paso 3: Limpieza inmediata de Redis
      logger.info(`🗑️ Limpiando key Mobile ${mobileKey} inmediatamente...`);
      const clearStartTime = Date.now();
      
      const cleared = await this.redisRepo.clearListData(mobileKey);
      const clearTime = Date.now() - clearStartTime;
      
      if (cleared) {
        logger.info(`✅ Key Mobile ${mobileKey} limpiada exitosamente en ${clearTime}ms`);
        logger.info(`🔄 Redis Mobile ahora disponible para nuevos datos`);
      } else {
        logger.warn(`⚠️ No se pudo limpiar key Mobile ${mobileKey} (posiblemente ya estaba vacía)`);
      }

      // Paso 4: Verificar que Redis está limpio
      const finalStats = await this.redisRepo.getMobileStats();
      logger.info(`📊 Estadísticas Mobile finales: ${finalStats.totalRecords} registros (debe ser 0)`);

      if (finalStats.totalRecords > 0) {
        logger.warn(`⚠️ ADVERTENCIA: Aún quedan ${finalStats.totalRecords} registros Mobile en Redis después de la limpieza`);
      }

      const totalTime = Date.now() - startTime;
      logger.info(`✅ Extracción atómica Mobile completada: ${extractedData.length} registros procesados en ${totalTime}ms total`);

      return {
        success: true,
        data: extractedData,
        recordCount: extractedData.length,
        extractionTime: extractionTime,
        clearTime: clearTime,
        totalTime: totalTime,
        key: mobileKey,
        cleared: cleared,
        initialRecords: initialStats.totalRecords,
        finalRecords: finalStats.totalRecords
      };

    } catch (error) {
      const totalTime = Date.now() - startTime;
      logger.error(`❌ Error en extracción atómica Mobile (${totalTime}ms):`, error.message);
      
      // Intentar obtener estadísticas para debugging
      try {
        const errorStats = await this.redisRepo.getMobileStats();
        logger.error(`📊 Estadísticas Mobile en error: ${errorStats.totalRecords} registros`);
      } catch (statsError) {
        logger.error('❌ No se pudieron obtener estadísticas Mobile en error:', statsError.message);
      }

      return {
        success: false,
        error: error.message,
        data: [],
        recordCount: 0,
        extractionTime: 0,
        totalTime: totalTime,
        key: mobileKey,
        cleared: false
      };
    }
  }

  /**
   * Coordina la extracción atómica de ambos tipos de datos (GPS y Mobile)
   * Extrae ambos tipos y limpia Redis inmediatamente para cada uno
   * @returns {Object} Resultado combinado con datos de ambos tipos
   */
  async extractAllData() {
    const startTime = Date.now();
    
    try {
      if (!this.atomicProcessingEnabled) {
        const error = 'Procesamiento atómico deshabilitado por feature flag ATOMIC_PROCESSING_ENABLED=false';
        logger.error(`❌ ${error}`);
        return {
          success: false,
          error: error,
          gps: { data: [], recordCount: 0, success: false },
          mobile: { data: [], recordCount: 0, success: false },
          totalRecords: 0,
          extractionTime: Date.now() - startTime,
          featureFlagDisabled: true
        };
      }

      logger.info('🚀 Iniciando extracción atómica coordinada de TODOS los datos (GPS + Mobile)...', {
        mode: 'atomic',
        featureFlag: 'enabled'
      });
      
      if (!this.isInitialized) {
        await this.initialize();
      }

      // Obtener estadísticas iniciales combinadas
      const [initialGpsStats, initialMobileStats] = await Promise.all([
        this.redisRepo.getGPSStats(),
        this.redisRepo.getMobileStats()
      ]);

      const totalInitialRecords = initialGpsStats.totalRecords + initialMobileStats.totalRecords;
      logger.info(`📊 Estadísticas iniciales combinadas: ${initialGpsStats.totalRecords} GPS + ${initialMobileStats.totalRecords} Mobile = ${totalInitialRecords} total`);

      if (totalInitialRecords === 0) {
        logger.info('📍 No hay datos para extraer (GPS y Mobile vacíos)');
        return {
          success: true,
          gps: {
            data: [],
            recordCount: 0,
            success: true
          },
          mobile: {
            data: [],
            recordCount: 0,
            success: true
          },
          totalRecords: 0,
          extractionTime: Date.now() - startTime,
          allCleared: true
        };
      }

      // Paso 1: Extraer GPS atómicamente
      logger.info('🔄 Paso 1/2: Extracción atómica GPS...');
      const gpsResult = await this.extractAndClearGPSData();
      
      if (!gpsResult.success) {
        logger.error('❌ Fallo en extracción GPS, abortando extracción coordinada');
        return {
          success: false,
          error: `GPS extraction failed: ${gpsResult.error}`,
          gps: gpsResult,
          mobile: { data: [], recordCount: 0, success: false },
          totalRecords: 0,
          extractionTime: Date.now() - startTime
        };
      }

      // Paso 2: Extraer Mobile atómicamente
      logger.info('🔄 Paso 2/2: Extracción atómica Mobile...');
      const mobileResult = await this.extractAndClearMobileData();
      
      if (!mobileResult.success) {
        logger.error('❌ Fallo en extracción Mobile, pero GPS ya fue extraído exitosamente');
        // GPS ya fue extraído exitosamente, no fallar toda la operación
      }

      // Paso 3: Verificar limpieza completa
      const [finalGpsStats, finalMobileStats] = await Promise.all([
        this.redisRepo.getGPSStats(),
        this.redisRepo.getMobileStats()
      ]);

      const totalFinalRecords = finalGpsStats.totalRecords + finalMobileStats.totalRecords;
      const allCleared = totalFinalRecords === 0;

      if (allCleared) {
        logger.info('✅ Redis completamente limpio: 0 registros GPS + 0 registros Mobile');
      } else {
        logger.warn(`⚠️ Redis no completamente limpio: ${finalGpsStats.totalRecords} GPS + ${finalMobileStats.totalRecords} Mobile = ${totalFinalRecords} restantes`);
      }

      // Calcular estadísticas finales
      const totalExtractedRecords = gpsResult.recordCount + mobileResult.recordCount;
      const totalTime = Date.now() - startTime;
      const overallSuccess = gpsResult.success && mobileResult.success;

      logger.info(`✅ Extracción atómica coordinada completada:`);
      logger.info(`   📊 GPS: ${gpsResult.recordCount} registros extraídos`);
      logger.info(`   📊 Mobile: ${mobileResult.recordCount} registros extraídos`);
      logger.info(`   📊 Total: ${totalExtractedRecords} registros en ${totalTime}ms`);
      logger.info(`   🔄 Redis limpio: ${allCleared ? 'SÍ' : 'NO'}`);

      return {
        success: overallSuccess,
        gps: {
          data: gpsResult.data,
          recordCount: gpsResult.recordCount,
          success: gpsResult.success,
          extractionTime: gpsResult.extractionTime,
          clearTime: gpsResult.clearTime,
          cleared: gpsResult.cleared
        },
        mobile: {
          data: mobileResult.data,
          recordCount: mobileResult.recordCount,
          success: mobileResult.success,
          extractionTime: mobileResult.extractionTime,
          clearTime: mobileResult.clearTime,
          cleared: mobileResult.cleared
        },
        totalRecords: totalExtractedRecords,
        extractionTime: totalTime,
        allCleared: allCleared,
        initialStats: {
          gps: initialGpsStats.totalRecords,
          mobile: initialMobileStats.totalRecords,
          total: totalInitialRecords
        },
        finalStats: {
          gps: finalGpsStats.totalRecords,
          mobile: finalMobileStats.totalRecords,
          total: totalFinalRecords
        }
      };

    } catch (error) {
      const totalTime = Date.now() - startTime;
      logger.error(`❌ Error en extracción atómica coordinada (${totalTime}ms):`, error.message);

      return {
        success: false,
        error: error.message,
        gps: { data: [], recordCount: 0, success: false },
        mobile: { data: [], recordCount: 0, success: false },
        totalRecords: 0,
        extractionTime: totalTime
      };
    }
  }

  /**
   * Obtiene estadísticas del procesador atómico
   * @returns {Object} Estadísticas actuales
   */
  async getStats() {
    try {
      if (!this.isInitialized) {
        return {
          initialized: false,
          error: 'Processor not initialized'
        };
      }

      const [gpsStats, mobileStats] = await Promise.all([
        this.redisRepo.getGPSStats(),
        this.redisRepo.getMobileStats()
      ]);

      return {
        initialized: this.isInitialized,
        atomicProcessingEnabled: this.atomicProcessingEnabled,
        processingMode: this.getProcessingMode(),
        redis: {
          gps: gpsStats,
          mobile: mobileStats,
          total: gpsStats.totalRecords + mobileStats.totalRecords
        },
        keys: {
          gps: config.gps.listKey,
          mobile: 'mobile:history:global'
        },
        featureFlags: {
          atomicProcessingEnabled: this.atomicProcessingEnabled,
          configValue: config.backup.atomicProcessingEnabled
        },
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas del procesador atómico:', error.message);
      return {
        initialized: this.isInitialized,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Verifica la salud del procesador atómico
   * @returns {Object} Estado de salud
   */
  async healthCheck() {
    try {
      const redisHealth = await this.redisRepo.ping();
      
      return {
        healthy: this.isInitialized && redisHealth,
        initialized: this.isInitialized,
        atomicProcessingEnabled: this.atomicProcessingEnabled,
        processingMode: this.getProcessingMode(),
        redis: redisHealth ? 'healthy' : 'unhealthy',
        featureFlags: {
          atomicProcessingEnabled: this.atomicProcessingEnabled,
          configValue: config.backup.atomicProcessingEnabled
        },
        warnings: this.atomicProcessingEnabled ? [] : [
          'Procesamiento atómico deshabilitado - riesgo de pérdida de datos',
          'Para habilitar: ATOMIC_PROCESSING_ENABLED=true'
        ],
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en health check del procesador atómico:', error.message);
      return {
        healthy: false,
        initialized: this.isInitialized,
        atomicProcessingEnabled: this.atomicProcessingEnabled,
        processingMode: this.getProcessingMode(),
        redis: 'unhealthy',
        error: error.message,
        featureFlags: {
          atomicProcessingEnabled: this.atomicProcessingEnabled,
          configValue: config.backup.atomicProcessingEnabled
        },
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Limpia recursos del procesador atómico
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del procesador atómico...');
      
      if (this.redisRepo) {
        await this.redisRepo.disconnect();
      }
      
      this.isInitialized = false;
      
      logger.info('✅ Recursos del procesador atómico limpiados exitosamente');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del procesador atómico:', error.message);
    }
  }
}