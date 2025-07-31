import { logger } from './logger.js';
import { migrationConfig } from '../config/migrationConfig.js';

/**
 * Utilidad para recopilar y analizar métricas de migración
 * Permite comparar rendimiento entre flujo legacy y nuevo flujo
 */
export class MigrationMetrics {
  constructor() {
    this.metrics = {
      legacy: {
        executions: [],
        totalExecutions: 0,
        successfulExecutions: 0,
        failedExecutions: 0,
        averageProcessingTime: 0,
        averageRecordsProcessed: 0,
        errorRate: 0,
        lastExecution: null
      },
      newFlow: {
        executions: [],
        totalExecutions: 0,
        successfulExecutions: 0,
        failedExecutions: 0,
        averageProcessingTime: 0,
        averageRecordsProcessed: 0,
        errorRate: 0,
        lastExecution: null
      },
      comparison: {
        performanceRatio: 1.0,
        reliabilityRatio: 1.0,
        lastComparison: null,
        recommendations: []
      }
    };
    
    this.retentionDays = migrationConfig.getConfig().metrics.retentionDays;
    this.cleanupOldMetrics();
  }

  /**
   * Registra métricas de ejecución para un flujo específico
   */
  recordExecution(flowType, executionData) {
    try {
      const timestamp = new Date().toISOString();
      const execution = {
        timestamp,
        success: executionData.success,
        processingTime: executionData.processingTime || 0,
        recordsProcessed: executionData.recordsProcessed || 0,
        error: executionData.error || null,
        stage: executionData.stage || 'completed',
        details: executionData.details || {}
      };

      if (flowType === 'legacy' || flowType === 'newFlow') {
        this.metrics[flowType].executions.push(execution);
        this.metrics[flowType].totalExecutions++;
        this.metrics[flowType].lastExecution = execution;

        if (execution.success) {
          this.metrics[flowType].successfulExecutions++;
        } else {
          this.metrics[flowType].failedExecutions++;
        }

        this.updateAverages(flowType);
        this.cleanupOldMetrics();

        if (migrationConfig.getPerformanceConfig().loggingEnabled) {
          this.logExecutionMetrics(flowType, execution);
        }
      }
    } catch (error) {
      logger.error('❌ Error registrando métricas de ejecución:', error.message);
    }
  }

  /**
   * Actualiza promedios para un flujo específico
   */
  updateAverages(flowType) {
    const flow = this.metrics[flowType];
    const successfulExecutions = flow.executions.filter(e => e.success);

    if (successfulExecutions.length > 0) {
      flow.averageProcessingTime = successfulExecutions.reduce((sum, e) => sum + e.processingTime, 0) / successfulExecutions.length;
      flow.averageRecordsProcessed = successfulExecutions.reduce((sum, e) => sum + e.recordsProcessed, 0) / successfulExecutions.length;
    }

    flow.errorRate = flow.totalExecutions > 0 ? flow.failedExecutions / flow.totalExecutions : 0;
  }

  /**
   * Compara rendimiento entre flujos
   */
  compareFlows() {
    try {
      const legacy = this.metrics.legacy;
      const newFlow = this.metrics.newFlow;

      if (legacy.totalExecutions === 0 || newFlow.totalExecutions === 0) {
        return {
          canCompare: false,
          reason: 'Insuficientes datos para comparación'
        };
      }

      // Calcular ratios de rendimiento
      const performanceRatio = legacy.averageProcessingTime > 0 ? 
        newFlow.averageProcessingTime / legacy.averageProcessingTime : 1.0;
      
      const reliabilityRatio = legacy.errorRate > 0 ? 
        newFlow.errorRate / legacy.errorRate : 
        (newFlow.errorRate === 0 && legacy.errorRate === 0 ? 1.0 : 0.0);

      // Generar recomendaciones
      const recommendations = this.generateRecommendations(performanceRatio, reliabilityRatio);

      const comparison = {
        canCompare: true,
        timestamp: new Date().toISOString(),
        performanceRatio,
        reliabilityRatio,
        legacy: {
          avgProcessingTime: legacy.averageProcessingTime,
          errorRate: legacy.errorRate,
          totalExecutions: legacy.totalExecutions,
          avgRecordsProcessed: legacy.averageRecordsProcessed
        },
        newFlow: {
          avgProcessingTime: newFlow.averageProcessingTime,
          errorRate: newFlow.errorRate,
          totalExecutions: newFlow.totalExecutions,
          avgRecordsProcessed: newFlow.averageRecordsProcessed
        },
        recommendations
      };

      this.metrics.comparison = {
        performanceRatio,
        reliabilityRatio,
        lastComparison: comparison,
        recommendations
      };

      if (migrationConfig.getPerformanceConfig().comparisonLogging) {
        this.logComparison(comparison);
      }

      return comparison;
    } catch (error) {
      logger.error('❌ Error comparando flujos:', error.message);
      return {
        canCompare: false,
        reason: `Error en comparación: ${error.message}`
      };
    }
  }

  /**
   * Genera recomendaciones basadas en métricas
   */
  generateRecommendations(performanceRatio, reliabilityRatio) {
    const recommendations = [];
    const config = migrationConfig.getComparisonConfig();

    // Recomendaciones de rendimiento
    if (performanceRatio > (1 + config.tolerance)) {
      recommendations.push({
        type: 'performance',
        severity: 'warning',
        message: `Nuevo flujo es ${((performanceRatio - 1) * 100).toFixed(1)}% más lento`,
        action: 'Considerar optimizaciones o mantener flujo legacy'
      });
    } else if (performanceRatio < (1 - config.tolerance)) {
      recommendations.push({
        type: 'performance',
        severity: 'info',
        message: `Nuevo flujo es ${((1 - performanceRatio) * 100).toFixed(1)}% más rápido`,
        action: 'Continuar migración al nuevo flujo'
      });
    }

    // Recomendaciones de confiabilidad
    if (reliabilityRatio > (1 + config.tolerance)) {
      recommendations.push({
        type: 'reliability',
        severity: 'error',
        message: `Nuevo flujo tiene ${((reliabilityRatio - 1) * 100).toFixed(1)}% más errores`,
        action: 'Investigar errores antes de continuar migración'
      });
    } else if (reliabilityRatio < (1 - config.tolerance)) {
      recommendations.push({
        type: 'reliability',
        severity: 'info',
        message: `Nuevo flujo es ${((1 - reliabilityRatio) * 100).toFixed(1)}% más confiable`,
        action: 'Acelerar migración al nuevo flujo'
      });
    }

    // Recomendación general
    if (recommendations.length === 0) {
      recommendations.push({
        type: 'general',
        severity: 'info',
        message: 'Ambos flujos tienen rendimiento similar',
        action: 'Continuar migración gradual según plan'
      });
    }

    return recommendations;
  }

  /**
   * Verifica si se debe activar rollback automático
   */
  shouldTriggerRollback() {
    const rollbackConfig = migrationConfig.getRollbackConfig();
    
    if (!rollbackConfig.enabled) {
      return { shouldRollback: false, reason: 'Rollback deshabilitado' };
    }

    const newFlow = this.metrics.newFlow;
    
    // Verificar fallos consecutivos
    const recentExecutions = newFlow.executions.slice(-rollbackConfig.consecutiveFailures);
    const consecutiveFailures = recentExecutions.length === rollbackConfig.consecutiveFailures &&
                               recentExecutions.every(e => !e.success);

    if (consecutiveFailures) {
      return {
        shouldRollback: true,
        reason: `${rollbackConfig.consecutiveFailures} fallos consecutivos detectados`,
        trigger: 'consecutive_failures'
      };
    }

    // Verificar tasa de error
    if (newFlow.totalExecutions >= rollbackConfig.threshold && 
        newFlow.errorRate > rollbackConfig.errorRate) {
      return {
        shouldRollback: true,
        reason: `Tasa de error ${(newFlow.errorRate * 100).toFixed(1)}% excede límite ${(rollbackConfig.errorRate * 100).toFixed(1)}%`,
        trigger: 'error_rate'
      };
    }

    // Verificar rendimiento
    const comparison = this.compareFlows();
    if (comparison.canCompare && 
        comparison.performanceRatio > rollbackConfig.performanceThreshold) {
      return {
        shouldRollback: true,
        reason: `Rendimiento ${(comparison.performanceRatio * 100).toFixed(1)}% peor que flujo legacy`,
        trigger: 'performance'
      };
    }

    return { shouldRollback: false, reason: 'Métricas dentro de límites aceptables' };
  }

  /**
   * Obtiene estadísticas completas
   */
  getStats() {
    return {
      legacy: { ...this.metrics.legacy },
      newFlow: { ...this.metrics.newFlow },
      comparison: { ...this.metrics.comparison },
      rollbackCheck: this.shouldTriggerRollback(),
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Limpia métricas antiguas basándose en retención configurada
   */
  cleanupOldMetrics() {
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - this.retentionDays);

    ['legacy', 'newFlow'].forEach(flowType => {
      const originalLength = this.metrics[flowType].executions.length;
      this.metrics[flowType].executions = this.metrics[flowType].executions.filter(
        execution => new Date(execution.timestamp) > cutoffDate
      );
      
      const removedCount = originalLength - this.metrics[flowType].executions.length;
      if (removedCount > 0) {
        logger.debug(`🧹 Limpiadas ${removedCount} métricas antiguas de ${flowType}`);
        this.updateAverages(flowType);
      }
    });
  }

  /**
   * Registra métricas de ejecución en logs
   */
  logExecutionMetrics(flowType, execution) {
    const status = execution.success ? '✅' : '❌';
    const flowLabel = flowType === 'legacy' ? 'LEGACY' : 'NEW';
    
    logger.info(`${status} [${flowLabel}] Procesamiento completado:`);
    logger.info(`   📊 Registros: ${execution.recordsProcessed}`);
    logger.info(`   ⏱️  Tiempo: ${execution.processingTime}ms`);
    
    if (!execution.success && execution.error) {
      logger.error(`   ❌ Error: ${execution.error}`);
    }

    if (migrationConfig.getPerformanceConfig().detailedMetrics && execution.details) {
      logger.debug(`   📋 Detalles: ${JSON.stringify(execution.details)}`);
    }
  }

  /**
   * Registra comparación en logs
   */
  logComparison(comparison) {
    logger.info('📊 Comparación de flujos:');
    logger.info(`   ⚡ Rendimiento: Nuevo flujo es ${comparison.performanceRatio > 1 ? 'más lento' : 'más rápido'} (${(comparison.performanceRatio * 100).toFixed(1)}%)`);
    logger.info(`   🛡️  Confiabilidad: Nuevo flujo es ${comparison.reliabilityRatio > 1 ? 'menos confiable' : 'más confiable'} (${(comparison.reliabilityRatio * 100).toFixed(1)}%)`);
    
    if (comparison.recommendations.length > 0) {
      logger.info('   💡 Recomendaciones:');
      comparison.recommendations.forEach(rec => {
        const icon = rec.severity === 'error' ? '🚨' : rec.severity === 'warning' ? '⚠️' : 'ℹ️';
        logger.info(`     ${icon} ${rec.message}`);
      });
    }
  }

  /**
   * Reinicia todas las métricas
   */
  reset() {
    this.metrics = {
      legacy: {
        executions: [],
        totalExecutions: 0,
        successfulExecutions: 0,
        failedExecutions: 0,
        averageProcessingTime: 0,
        averageRecordsProcessed: 0,
        errorRate: 0,
        lastExecution: null
      },
      newFlow: {
        executions: [],
        totalExecutions: 0,
        successfulExecutions: 0,
        failedExecutions: 0,
        averageProcessingTime: 0,
        averageRecordsProcessed: 0,
        errorRate: 0,
        lastExecution: null
      },
      comparison: {
        performanceRatio: 1.0,
        reliabilityRatio: 1.0,
        lastComparison: null,
        recommendations: []
      }
    };
    
    logger.info('🔄 Métricas de migración reiniciadas');
  }
}

// Instancia singleton
export const migrationMetrics = new MigrationMetrics();