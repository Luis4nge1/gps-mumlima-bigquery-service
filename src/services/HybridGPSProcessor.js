import { GPSProcessorService } from './GPSProcessorService.js';
import { LegacyGPSProcessor } from './LegacyGPSProcessor.js';
import { logger } from '../utils/logger.js';
import { migrationConfig } from '../config/migrationConfig.js';
import { migrationMetrics } from '../utils/MigrationMetrics.js';
import { rollbackManager } from '../utils/RollbackManager.js';

/**
 * Procesador GPS híbrido que soporta migración gradual
 * Permite ejecutar flujo legacy, nuevo flujo, o ambos para comparación
 */
export class HybridGPSProcessor {
  constructor() {
    this.newFlowProcessor = new GPSProcessorService();
    this.legacyProcessor = null; // Se inicializará si es necesario
    this.isProcessing = false;
  }

  /**
   * Inicializa el procesador híbrido
   */
  async initialize() {
    try {
      logger.info('🔧 Inicializando Hybrid GPS Processor...');
      
      // Siempre inicializar el nuevo procesador
      await this.newFlowProcessor.initialize();
      
      // Inicializar procesador legacy si es necesario
      if (this.shouldUseLegacyFlow() || migrationConfig.shouldCompare()) {
        await this.initializeLegacyProcessor();
      }
      
      logger.info('✅ Hybrid GPS Processor inicializado exitosamente');
    } catch (error) {
      logger.error('❌ Error inicializando Hybrid GPS Processor:', error.message);
      throw error;
    }
  }

  /**
   * Inicializa el procesador legacy real
   */
  async initializeLegacyProcessor() {
    try {
      logger.info('🔧 Inicializando procesador legacy real...');
      
      this.legacyProcessor = new LegacyGPSProcessor();
      await this.legacyProcessor.initialize();
      
      logger.info('✅ Procesador legacy real inicializado');
    } catch (error) {
      logger.error('❌ Error inicializando procesador legacy:', error.message);
      throw error;
    }
  }

  /**
   * Procesa datos GPS usando la estrategia configurada
   */
  async processGPSData() {
    if (this.isProcessing) {
      logger.warn('⚠️ Procesamiento ya en curso, saltando ejecución');
      return { success: false, error: 'Processing already in progress' };
    }

    this.isProcessing = true;

    try {
      // Verificar rollback automático antes de procesar
      const rollbackCheck = await rollbackManager.checkAndExecuteRollback();
      if (rollbackCheck.executed) {
        logger.warn(`🔄 Rollback ejecutado: ${rollbackCheck.reason}`);
      }

      const migrationStatus = migrationConfig.getStatus();
      logger.info(`🔄 Procesando con configuración: ${migrationStatus.currentPhase} (nuevo flujo: ${migrationStatus.shouldUseNewFlow})`);

      let result;

      if (migrationConfig.shouldCompare()) {
        // Modo comparación: ejecutar ambos flujos
        result = await this.processWithComparison();
      } else if (migrationConfig.shouldUseNewFlow()) {
        // Solo nuevo flujo
        result = await this.processWithNewFlow();
      } else {
        // Solo flujo legacy
        result = await this.processWithLegacyFlow();
      }

      return result;

    } catch (error) {
      logger.error('❌ Error en procesamiento híbrido:', error.message);
      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        flowType: 'hybrid_error'
      };
    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Procesa usando solo el nuevo flujo
   */
  async processWithNewFlow() {
    try {
      logger.info('🚀 Ejecutando nuevo flujo GCS-BigQuery...');
      const startTime = Date.now();
      
      const result = await this.newFlowProcessor.processGPSData();
      
      // Registrar métricas
      migrationMetrics.recordExecution('newFlow', {
        success: result.success,
        processingTime: result.processingTime || (Date.now() - startTime),
        recordsProcessed: result.recordsProcessed || 0,
        error: result.error,
        details: {
          results: result.results,
          separationStats: result.separationStats
        }
      });

      return {
        ...result,
        flowType: 'newFlow',
        migrationPhase: migrationConfig.getConfig().migrationPhase
      };

    } catch (error) {
      logger.error('❌ Error en nuevo flujo:', error.message);
      
      migrationMetrics.recordExecution('newFlow', {
        success: false,
        processingTime: 0,
        recordsProcessed: 0,
        error: error.message
      });

      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        flowType: 'newFlow'
      };
    }
  }

  /**
   * Procesa usando solo el flujo legacy
   */
  async processWithLegacyFlow() {
    try {
      logger.info('🔄 Ejecutando flujo legacy...');
      
      if (!this.legacyProcessor) {
        await this.initializeLegacyProcessor();
      }

      const result = await this.legacyProcessor.processGPSData();
      
      // Registrar métricas
      migrationMetrics.recordExecution('legacy', {
        success: result.success,
        processingTime: result.processingTime,
        recordsProcessed: result.recordsProcessed,
        error: result.error
      });

      return {
        ...result,
        flowType: 'legacy',
        migrationPhase: migrationConfig.getConfig().migrationPhase
      };

    } catch (error) {
      logger.error('❌ Error en flujo legacy:', error.message);
      
      migrationMetrics.recordExecution('legacy', {
        success: false,
        processingTime: 0,
        recordsProcessed: 0,
        error: error.message
      });

      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        flowType: 'legacy'
      };
    }
  }

  /**
   * Procesa con ambos flujos para comparación
   */
  async processWithComparison() {
    try {
      logger.info('📊 Ejecutando ambos flujos para comparación...');
      const comparisonStartTime = Date.now();

      // Ejecutar ambos flujos en paralelo
      const [newFlowResult, legacyResult] = await Promise.allSettled([
        this.processWithNewFlow(),
        this.processWithLegacyFlow()
      ]);

      // Procesar resultados
      const newFlow = newFlowResult.status === 'fulfilled' ? newFlowResult.value : {
        success: false,
        error: newFlowResult.reason?.message || 'Error desconocido',
        recordsProcessed: 0,
        processingTime: 0,
        flowType: 'newFlow'
      };

      const legacy = legacyResult.status === 'fulfilled' ? legacyResult.value : {
        success: false,
        error: legacyResult.reason?.message || 'Error desconocido',
        recordsProcessed: 0,
        processingTime: 0,
        flowType: 'legacy'
      };

      // Realizar comparación detallada
      const comparison = migrationMetrics.compareFlows();
      const comparisonTime = Date.now() - comparisonStartTime;

      // Log detallado de comparación si está habilitado
      if (migrationConfig.getPerformanceConfig().comparisonLogging) {
        this.logDetailedComparison(newFlow, legacy, comparison, comparisonTime);
      }

      // Determinar qué resultado usar como principal
      const primaryResult = migrationConfig.shouldUseNewFlow() ? newFlow : legacy;
      const secondaryResult = migrationConfig.shouldUseNewFlow() ? legacy : newFlow;

      // Verificar consistencia de datos
      const consistencyCheck = this.checkDataConsistency(newFlow, legacy);

      logger.info('📊 Comparación completada:');
      logger.info(`   🎯 Flujo principal (${primaryResult.flowType}): ${primaryResult.success ? '✅' : '❌'} - ${primaryResult.recordsProcessed} registros en ${primaryResult.processingTime}ms`);
      logger.info(`   📋 Flujo secundario (${secondaryResult.flowType}): ${secondaryResult.success ? '✅' : '❌'} - ${secondaryResult.recordsProcessed} registros en ${secondaryResult.processingTime}ms`);
      
      if (consistencyCheck.hasIssues) {
        logger.warn(`   ⚠️ Inconsistencias detectadas: ${consistencyCheck.issues.join(', ')}`);
      } else {
        logger.info('   ✅ Resultados consistentes entre flujos');
      }

      return {
        ...primaryResult,
        flowType: 'comparison',
        comparison: {
          primary: primaryResult,
          secondary: secondaryResult,
          analysis: comparison,
          consistency: consistencyCheck,
          totalComparisonTime: comparisonTime
        },
        migrationPhase: migrationConfig.getConfig().migrationPhase
      };

    } catch (error) {
      logger.error('❌ Error en comparación de flujos:', error.message);
      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        flowType: 'comparison_error'
      };
    }
  }

  /**
   * Log detallado de comparación entre flujos
   */
  logDetailedComparison(newFlow, legacy, comparison, comparisonTime) {
    logger.info('📊 === COMPARACIÓN DETALLADA DE FLUJOS ===');
    
    // Métricas de rendimiento
    const performanceRatio = legacy.processingTime > 0 ? 
      (newFlow.processingTime / legacy.processingTime) : 1;
    
    logger.info(`📈 Rendimiento:`);
    logger.info(`   Legacy: ${legacy.processingTime}ms`);
    logger.info(`   Nuevo: ${newFlow.processingTime}ms`);
    logger.info(`   Ratio: ${performanceRatio.toFixed(2)}x ${performanceRatio > 1 ? '(más lento)' : '(más rápido)'}`);
    
    // Métricas de confiabilidad
    logger.info(`🎯 Confiabilidad:`);
    logger.info(`   Legacy: ${legacy.success ? 'Éxito' : 'Fallo'}`);
    logger.info(`   Nuevo: ${newFlow.success ? 'Éxito' : 'Fallo'}`);
    
    // Métricas de datos
    logger.info(`📊 Procesamiento de datos:`);
    logger.info(`   Legacy: ${legacy.recordsProcessed} registros`);
    logger.info(`   Nuevo: ${newFlow.recordsProcessed} registros`);
    
    const recordDifference = Math.abs(legacy.recordsProcessed - newFlow.recordsProcessed);
    if (recordDifference > 0) {
      logger.warn(`   ⚠️ Diferencia: ${recordDifference} registros`);
    }
    
    // Errores si los hay
    if (!legacy.success && legacy.error) {
      logger.warn(`   ❌ Error Legacy: ${legacy.error}`);
    }
    if (!newFlow.success && newFlow.error) {
      logger.warn(`   ❌ Error Nuevo: ${newFlow.error}`);
    }
    
    // Tiempo total de comparación
    logger.info(`⏱️ Tiempo total de comparación: ${comparisonTime}ms`);
    
    // Recomendaciones si las hay
    if (comparison && comparison.recommendations && comparison.recommendations.length > 0) {
      logger.info(`💡 Recomendaciones:`);
      comparison.recommendations.forEach(rec => {
        logger.info(`   - ${rec}`);
      });
    }
    
    logger.info('📊 === FIN COMPARACIÓN DETALLADA ===');
  }

  /**
   * Verifica consistencia de datos entre flujos
   */
  checkDataConsistency(newFlow, legacy) {
    const issues = [];
    const tolerance = migrationConfig.getComparisonConfig().tolerance || 0.05;
    
    // Verificar diferencia en registros procesados
    const recordDifference = Math.abs(newFlow.recordsProcessed - legacy.recordsProcessed);
    const maxRecords = Math.max(newFlow.recordsProcessed, legacy.recordsProcessed);
    
    if (maxRecords > 0) {
      const recordDifferenceRatio = recordDifference / maxRecords;
      if (recordDifferenceRatio > tolerance) {
        issues.push(`Diferencia significativa en registros: ${recordDifference} (${(recordDifferenceRatio * 100).toFixed(1)}%)`);
      }
    }
    
    // Verificar estados de éxito diferentes
    if (newFlow.success !== legacy.success) {
      issues.push(`Estados de éxito diferentes: Legacy=${legacy.success}, Nuevo=${newFlow.success}`);
    }
    
    // Verificar si ambos fallaron pero con errores diferentes
    if (!newFlow.success && !legacy.success && newFlow.error !== legacy.error) {
      issues.push('Ambos flujos fallaron con errores diferentes');
    }
    
    return {
      hasIssues: issues.length > 0,
      issues,
      recordDifference,
      successMatch: newFlow.success === legacy.success
    };
  }

  /**
   * Determina si debe usar el flujo legacy
   */
  shouldUseLegacyFlow() {
    return !migrationConfig.shouldUseNewFlow();
  }

  /**
   * Obtiene estadísticas del procesador híbrido
   */
  async getProcessorStats() {
    try {
      const migrationStatus = migrationConfig.getStatus();
      const migrationStats = migrationMetrics.getStats();
      const rollbackStatus = rollbackManager.getStatus();

      // Obtener estadísticas del procesador activo
      let processorStats = {};
      if (migrationConfig.shouldUseNewFlow()) {
        processorStats = await this.newFlowProcessor.getProcessorStats();
      }

      return {
        migration: migrationStatus,
        metrics: migrationStats,
        rollback: rollbackStatus,
        processor: {
          ...processorStats,
          isProcessing: this.isProcessing,
          hybridMode: true,
          activeFlow: migrationConfig.shouldUseNewFlow() ? 'newFlow' : 'legacy',
          comparisonEnabled: migrationConfig.shouldCompare()
        },
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas híbridas:', error.message);
      return {
        error: error.message,
        processor: {
          isProcessing: this.isProcessing,
          hybridMode: true
        },
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Verifica la salud del procesador híbrido
   */
  async healthCheck() {
    try {
      const migrationStatus = migrationConfig.getStatus();
      const rollbackStatus = rollbackManager.getStatus();

      // Health check del procesador activo
      let processorHealth = { healthy: false };
      if (migrationConfig.shouldUseNewFlow()) {
        processorHealth = await this.newFlowProcessor.healthCheck();
      } else if (this.legacyProcessor) {
        // Health check del procesador legacy real
        processorHealth = await this.legacyProcessor.healthCheck();
      }

      const isHealthy = processorHealth.healthy && migrationStatus.migrationEnabled;

      return {
        healthy: isHealthy,
        migration: migrationStatus,
        rollback: {
          enabled: rollbackStatus.enabled,
          inCooldown: rollbackStatus.inCooldown,
          recentRollbacks: rollbackStatus.rollbackCount
        },
        processor: processorHealth,
        activeFlow: migrationConfig.shouldUseNewFlow() ? 'newFlow' : 'legacy',
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en health check híbrido:', error.message);
      return {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Fuerza un cambio de fase de migración
   */
  async setMigrationPhase(phase) {
    try {
      logger.info(`🔄 Cambiando fase de migración a: ${phase}`);
      
      migrationConfig.setMigrationPhase(phase);
      
      // Reinicializar procesadores si es necesario
      if (phase === 'legacy' || migrationConfig.shouldCompare()) {
        if (!this.legacyProcessor) {
          await this.initializeLegacyProcessor();
        }
      }

      logger.info(`✅ Fase de migración actualizada a: ${phase}`);
      return { success: true, newPhase: phase };

    } catch (error) {
      logger.error('❌ Error cambiando fase de migración:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Ejecuta rollback manual
   */
  async executeManualRollback(reason = 'Rollback manual solicitado') {
    try {
      const result = await rollbackManager.forceRollback(reason);
      
      if (result.success) {
        // Reinicializar procesador legacy si es necesario
        if (!this.legacyProcessor) {
          await this.initializeLegacyProcessor();
        }
      }

      return result;
    } catch (error) {
      logger.error('❌ Error ejecutando rollback manual:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Limpia recursos del procesador híbrido
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del procesador híbrido...');
      
      await this.newFlowProcessor.cleanup();
      
      // Limpiar procesador legacy si existe
      if (this.legacyProcessor && typeof this.legacyProcessor.cleanup === 'function') {
        await this.legacyProcessor.cleanup();
      }
      
      logger.info('✅ Recursos del procesador híbrido limpiados exitosamente');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del procesador híbrido:', error.message);
    }
  }
}