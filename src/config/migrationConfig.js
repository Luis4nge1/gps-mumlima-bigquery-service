import dotenv from 'dotenv';
import { logger } from '../utils/logger.js';

dotenv.config();

/**
 * Configuración para migración gradual del sistema GPS
 * Permite transición controlada entre el flujo legacy y el nuevo flujo GCS-BigQuery
 */
export class MigrationConfig {
  constructor() {
    this.config = this.loadConfig();
    this.validateConfig();
  }

  /**
   * Carga la configuración de migración desde variables de entorno
   */
  loadConfig() {
    return {
      // Control general de migración
      migrationEnabled: process.env.MIGRATION_ENABLED === 'true',
      newFlowEnabled: process.env.NEW_FLOW_ENABLED === 'true',
      hybridMode: process.env.HYBRID_MODE === 'true',
      
      // Fases de migración: 'legacy', 'hybrid', 'new', 'rollback'
      migrationPhase: process.env.MIGRATION_PHASE || 'legacy',
      
      // Configuración de rollback automático
      rollback: {
        enabled: process.env.ROLLBACK_ENABLED === 'true',
        threshold: parseInt(process.env.ROLLBACK_THRESHOLD) || 3,
        consecutiveFailures: parseInt(process.env.ROLLBACK_CONSECUTIVE_FAILURES) || 3,
        errorRate: parseFloat(process.env.ROLLBACK_ERROR_RATE) || 0.5,
        performanceThreshold: parseFloat(process.env.ROLLBACK_PERFORMANCE_THRESHOLD) || 2.0,
        cooldownMinutes: parseInt(process.env.ROLLBACK_COOLDOWN_MINUTES) || 30
      },
      
      // Configuración de logging y métricas
      performance: {
        loggingEnabled: process.env.PERFORMANCE_LOGGING_ENABLED === 'true',
        detailedMetrics: process.env.DETAILED_PERFORMANCE_METRICS === 'true',
        comparisonLogging: process.env.COMPARISON_LOGGING === 'true'
      },
      
      // Configuración de comparación entre flujos
      comparison: {
        enabled: process.env.COMPARISON_ENABLED === 'true',
        sampleRate: parseFloat(process.env.COMPARISON_SAMPLE_RATE) || 1.0,
        tolerance: parseFloat(process.env.COMPARISON_TOLERANCE) || 0.05
      },
      
      // Configuración de métricas
      metrics: {
        retentionDays: parseInt(process.env.METRICS_RETENTION_DAYS) || 7
      }
    };
  }

  /**
   * Valida la configuración de migración
   */
  validateConfig() {
    const validPhases = ['legacy', 'hybrid', 'new', 'rollback'];
    if (!validPhases.includes(this.config.migrationPhase)) {
      throw new Error(`Fase de migración inválida: ${this.config.migrationPhase}. Válidas: ${validPhases.join(', ')}`);
    }

    if (this.config.rollback.errorRate < 0 || this.config.rollback.errorRate > 1) {
      throw new Error('ROLLBACK_ERROR_RATE debe estar entre 0 y 1');
    }

    if (this.config.comparison.sampleRate < 0 || this.config.comparison.sampleRate > 1) {
      throw new Error('COMPARISON_SAMPLE_RATE debe estar entre 0 y 1');
    }

    logger.info('✅ Configuración de migración validada exitosamente');
  }

  /**
   * Determina si debe usar el nuevo flujo basándose en la configuración actual
   */
  shouldUseNewFlow() {
    switch (this.config.migrationPhase) {
      case 'legacy':
        return false;
      case 'hybrid':
        return this.config.newFlowEnabled;
      case 'new':
        return true;
      case 'rollback':
        return false;
      default:
        return false;
    }
  }

  /**
   * Determina si está en modo híbrido
   */
  isHybridMode() {
    return this.config.migrationPhase === 'hybrid' && this.config.hybridMode;
  }

  /**
   * Determina si debe ejecutar comparación entre flujos
   */
  shouldCompare() {
    return this.config.comparison.enabled && 
           this.config.migrationPhase === 'hybrid' &&
           Math.random() < this.config.comparison.sampleRate;
  }

  /**
   * Obtiene la configuración de rollback
   */
  getRollbackConfig() {
    return this.config.rollback;
  }

  /**
   * Obtiene la configuración de performance
   */
  getPerformanceConfig() {
    return this.config.performance;
  }

  /**
   * Obtiene la configuración de comparación
   */
  getComparisonConfig() {
    return this.config.comparison;
  }

  /**
   * Actualiza la fase de migración
   */
  setMigrationPhase(phase) {
    const validPhases = ['legacy', 'hybrid', 'new', 'rollback'];
    if (!validPhases.includes(phase)) {
      throw new Error(`Fase de migración inválida: ${phase}`);
    }
    
    this.config.migrationPhase = phase;
    logger.info(`🔄 Fase de migración actualizada a: ${phase}`);
  }

  /**
   * Habilita o deshabilita el nuevo flujo
   */
  setNewFlowEnabled(enabled) {
    this.config.newFlowEnabled = enabled;
    logger.info(`🔄 Nuevo flujo ${enabled ? 'habilitado' : 'deshabilitado'}`);
  }

  /**
   * Obtiene toda la configuración actual
   */
  getConfig() {
    return { ...this.config };
  }

  /**
   * Obtiene un resumen del estado actual de migración
   */
  getStatus() {
    return {
      migrationEnabled: this.config.migrationEnabled,
      currentPhase: this.config.migrationPhase,
      newFlowEnabled: this.config.newFlowEnabled,
      hybridMode: this.config.hybridMode,
      shouldUseNewFlow: this.shouldUseNewFlow(),
      isHybridMode: this.isHybridMode(),
      rollbackEnabled: this.config.rollback.enabled,
      comparisonEnabled: this.config.comparison.enabled,
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Reinicia la configuración a valores por defecto
   */
  reset() {
    this.config = this.loadConfig();
    this.validateConfig();
    logger.info('🔄 Configuración de migración reiniciada');
  }
}

// Instancia singleton
export const migrationConfig = new MigrationConfig();