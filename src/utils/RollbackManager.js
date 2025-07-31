import { logger } from './logger.js';
import { migrationConfig } from '../config/migrationConfig.js';
import { migrationMetrics } from './MigrationMetrics.js';

/**
 * Gestor de rollback automático para migración
 * Monitorea métricas y ejecuta rollback cuando se detectan problemas críticos
 */
export class RollbackManager {
  constructor() {
    this.rollbackHistory = [];
    this.lastRollbackTime = null;
    this.isInCooldown = false;
    this.rollbackReasons = [];
  }

  /**
   * Verifica si se debe ejecutar rollback y lo ejecuta si es necesario
   */
  async checkAndExecuteRollback() {
    try {
      // Verificar si está en período de cooldown
      if (this.isInCooldown) {
        const cooldownRemaining = this.getCooldownRemaining();
        if (cooldownRemaining > 0) {
          logger.debug(`🕐 Rollback en cooldown por ${cooldownRemaining} minutos más`);
          return { executed: false, reason: 'En período de cooldown' };
        } else {
          this.isInCooldown = false;
          logger.info('✅ Período de cooldown terminado');
        }
      }

      // Verificar condiciones de rollback
      const rollbackCheck = migrationMetrics.shouldTriggerRollback();
      
      if (!rollbackCheck.shouldRollback) {
        return { executed: false, reason: rollbackCheck.reason };
      }

      // Ejecutar rollback
      logger.warn(`🚨 Iniciando rollback automático: ${rollbackCheck.reason}`);
      logger.warn(`📊 Métricas que activaron rollback:`);
      logger.warn(`   - Trigger: ${rollbackCheck.trigger}`);
      logger.warn(`   - Detalles: ${JSON.stringify(rollbackCheck.details || {})}`);
      
      const rollbackResult = await this.executeRollback(rollbackCheck);
      
      if (rollbackResult.success) {
        this.recordRollback(rollbackCheck);
        this.startCooldown();
        
        logger.error(`🔄 Rollback ejecutado exitosamente: ${rollbackCheck.reason}`);
        logger.error(`📋 Nueva configuración:`);
        logger.error(`   - Fase: ${rollbackResult.newPhase}`);
        logger.error(`   - Nuevo flujo habilitado: ${rollbackResult.newFlowEnabled}`);
        logger.error(`   - Cooldown: ${migrationConfig.getRollbackConfig().cooldownMinutes} minutos`);
        
        return {
          executed: true,
          reason: rollbackCheck.reason,
          trigger: rollbackCheck.trigger,
          timestamp: new Date().toISOString()
        };
      } else {
        logger.error(`❌ Error ejecutando rollback: ${rollbackResult.error}`);
        return {
          executed: false,
          reason: `Error en rollback: ${rollbackResult.error}`
        };
      }

    } catch (error) {
      logger.error('❌ Error en verificación de rollback:', error.message);
      return {
        executed: false,
        reason: `Error en verificación: ${error.message}`
      };
    }
  }

  /**
   * Ejecuta el rollback cambiando la configuración
   */
  async executeRollback(rollbackCheck) {
    try {
      const currentPhase = migrationConfig.getConfig().migrationPhase;
      
      // Determinar nueva fase basándose en la fase actual
      let newPhase;
      switch (currentPhase) {
        case 'new':
          newPhase = 'hybrid';
          break;
        case 'hybrid':
          newPhase = 'legacy';
          break;
        default:
          newPhase = 'rollback';
      }

      // Actualizar configuración
      migrationConfig.setMigrationPhase(newPhase);
      migrationConfig.setNewFlowEnabled(false);

      // Registrar evento de rollback
      const rollbackEvent = {
        timestamp: new Date().toISOString(),
        fromPhase: currentPhase,
        toPhase: newPhase,
        trigger: rollbackCheck.trigger,
        reason: rollbackCheck.reason,
        success: true
      };

      logger.info(`🔄 Rollback ejecutado: ${currentPhase} → ${newPhase}`);
      logger.info(`   🎯 Trigger: ${rollbackCheck.trigger}`);
      logger.info(`   📝 Razón: ${rollbackCheck.reason}`);

      return { success: true, event: rollbackEvent };

    } catch (error) {
      logger.error('❌ Error ejecutando rollback:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Registra un evento de rollback en el historial
   */
  recordRollback(rollbackCheck) {
    const rollbackEvent = {
      timestamp: new Date().toISOString(),
      trigger: rollbackCheck.trigger,
      reason: rollbackCheck.reason,
      phase: migrationConfig.getConfig().migrationPhase
    };

    this.rollbackHistory.push(rollbackEvent);
    this.rollbackReasons.push(rollbackCheck.reason);
    this.lastRollbackTime = new Date();

    // Mantener solo los últimos 10 rollbacks
    if (this.rollbackHistory.length > 10) {
      this.rollbackHistory = this.rollbackHistory.slice(-10);
    }

    if (this.rollbackReasons.length > 10) {
      this.rollbackReasons = this.rollbackReasons.slice(-10);
    }
  }

  /**
   * Inicia el período de cooldown
   */
  startCooldown() {
    this.isInCooldown = true;
    this.lastRollbackTime = new Date();
    
    const cooldownMinutes = migrationConfig.getRollbackConfig().cooldownMinutes;
    logger.info(`🕐 Iniciando período de cooldown de ${cooldownMinutes} minutos`);
    
    // Programar fin de cooldown
    setTimeout(() => {
      this.isInCooldown = false;
      logger.info('✅ Período de cooldown terminado');
    }, cooldownMinutes * 60 * 1000);
  }

  /**
   * Obtiene el tiempo restante de cooldown en minutos
   */
  getCooldownRemaining() {
    if (!this.lastRollbackTime || !this.isInCooldown) {
      return 0;
    }

    const cooldownMinutes = migrationConfig.getRollbackConfig().cooldownMinutes;
    const elapsedMinutes = (Date.now() - this.lastRollbackTime.getTime()) / (1000 * 60);
    const remaining = Math.max(0, cooldownMinutes - elapsedMinutes);
    
    return Math.ceil(remaining);
  }

  /**
   * Verifica si el rollback está habilitado
   */
  isRollbackEnabled() {
    return migrationConfig.getRollbackConfig().enabled;
  }

  /**
   * Fuerza un rollback manual
   */
  async forceRollback(reason = 'Rollback manual') {
    try {
      logger.warn(`🚨 Ejecutando rollback manual: ${reason}`);
      
      const rollbackCheck = {
        shouldRollback: true,
        reason,
        trigger: 'manual'
      };

      const result = await this.executeRollback(rollbackCheck);
      
      if (result.success) {
        this.recordRollback(rollbackCheck);
        this.startCooldown();
        
        logger.info(`✅ Rollback manual ejecutado exitosamente`);
        return { success: true, reason };
      } else {
        logger.error(`❌ Error en rollback manual: ${result.error}`);
        return { success: false, error: result.error };
      }

    } catch (error) {
      logger.error('❌ Error ejecutando rollback manual:', error.message);
      return { success: false, error: error.message };
    }
  }

  /**
   * Obtiene el estado actual del rollback manager
   */
  getStatus() {
    return {
      enabled: this.isRollbackEnabled(),
      inCooldown: this.isInCooldown,
      cooldownRemaining: this.getCooldownRemaining(),
      lastRollbackTime: this.lastRollbackTime,
      rollbackCount: this.rollbackHistory.length,
      recentReasons: this.rollbackReasons.slice(-3),
      config: migrationConfig.getRollbackConfig(),
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Obtiene el historial de rollbacks
   */
  getRollbackHistory() {
    return {
      history: [...this.rollbackHistory],
      totalRollbacks: this.rollbackHistory.length,
      lastRollback: this.rollbackHistory[this.rollbackHistory.length - 1] || null,
      commonReasons: this.getCommonReasons(),
      timestamp: new Date().toISOString()
    };
  }

  /**
   * Analiza las razones más comunes de rollback
   */
  getCommonReasons() {
    const reasonCounts = {};
    
    this.rollbackReasons.forEach(reason => {
      reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
    });

    return Object.entries(reasonCounts)
      .sort(([,a], [,b]) => b - a)
      .slice(0, 5)
      .map(([reason, count]) => ({ reason, count }));
  }

  /**
   * Reinicia el estado del rollback manager
   */
  reset() {
    this.rollbackHistory = [];
    this.lastRollbackTime = null;
    this.isInCooldown = false;
    this.rollbackReasons = [];
    
    logger.info('🔄 Estado de rollback manager reiniciado');
  }

  /**
   * Valida la configuración de rollback
   */
  validateRollbackConfig() {
    const config = migrationConfig.getRollbackConfig();
    const issues = [];

    if (config.threshold < 1) {
      issues.push('Threshold debe ser mayor a 0');
    }

    if (config.consecutiveFailures < 1) {
      issues.push('ConsecutiveFailures debe ser mayor a 0');
    }

    if (config.errorRate < 0 || config.errorRate > 1) {
      issues.push('ErrorRate debe estar entre 0 y 1');
    }

    if (config.performanceThreshold < 1) {
      issues.push('PerformanceThreshold debe ser mayor a 1');
    }

    if (config.cooldownMinutes < 1) {
      issues.push('CooldownMinutes debe ser mayor a 0');
    }

    if (issues.length > 0) {
      throw new Error(`Configuración de rollback inválida: ${issues.join(', ')}`);
    }

    return true;
  }
}

// Instancia singleton
export const rollbackManager = new RollbackManager();