#!/usr/bin/env node

/**
 * Script de monitoreo continuo para producción
 * Ejecuta verificaciones de salud, alertas y optimizaciones automáticas
 */

import { logger } from '../src/utils/logger.js';
import { MetricsCollector } from '../src/utils/MetricsCollector.js';
import { AlertManager } from '../src/utils/AlertManager.js';
import { CostMonitor } from '../src/utils/CostMonitor.js';
import { AutoCleanup } from '../src/utils/AutoCleanup.js';
import { gcpConfig } from '../src/config/gcpConfig.js';
import fs from 'fs/promises';

class ProductionMonitor {
  constructor() {
    this.metricsCollector = MetricsCollector.getInstance();
    this.alertManager = new AlertManager();
    this.costMonitor = new CostMonitor();
    this.autoCleanup = new AutoCleanup();
    
    this.monitoringInterval = parseInt(process.env.MONITORING_INTERVAL_SECONDS) || 300; // 5 minutos
    this.healthCheckInterval = parseInt(process.env.HEALTH_CHECK_INTERVAL) || 30; // 30 segundos
    
    this.isRunning = false;
    this.stats = {
      startTime: new Date(),
      totalChecks: 0,
      healthyChecks: 0,
      alerts: 0,
      optimizations: 0,
      cleanups: 0
    };
  }

  /**
   * Inicia el monitoreo continuo
   */
  async start() {
    if (this.isRunning) {
      logger.warn('⚠️ Monitor ya está ejecutándose');
      return;
    }

    logger.info('🚀 Iniciando monitor de producción...');
    this.isRunning = true;

    try {
      // Verificación inicial
      await this.runInitialChecks();

      // Programar monitoreo continuo
      this.scheduleMonitoring();

      logger.info('✅ Monitor de producción iniciado exitosamente');

    } catch (error) {
      logger.error('❌ Error iniciando monitor:', error.message);
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Detiene el monitoreo
   */
  stop() {
    logger.info('🛑 Deteniendo monitor de producción...');
    this.isRunning = false;
    
    if (this.monitoringTimer) {
      clearInterval(this.monitoringTimer);
    }
    
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    logger.info('✅ Monitor detenido');
  }

  /**
   * Ejecuta verificaciones iniciales
   */
  async runInitialChecks() {
    logger.info('🔍 Ejecutando verificaciones iniciales...');

    const checks = [
      { name: 'Health Check', fn: () => this.performHealthCheck() },
      { name: 'GCP Status', fn: () => this.checkGCPStatus() },
      { name: 'Metrics Collection', fn: () => this.checkMetricsCollection() },
      { name: 'Alert System', fn: () => this.checkAlertSystem() },
      { name: 'Cost Monitoring', fn: () => this.checkCostMonitoring() },
      { name: 'Cleanup System', fn: () => this.checkCleanupSystem() }
    ];

    const results = await Promise.allSettled(
      checks.map(async check => {
        try {
          await check.fn();
          return { name: check.name, status: 'OK' };
        } catch (error) {
          return { name: check.name, status: 'ERROR', error: error.message };
        }
      })
    );

    const successful = results.filter(r => r.status === 'fulfilled' && r.value.status === 'OK').length;
    
    logger.info(`📊 Verificaciones iniciales: ${successful}/${checks.length} exitosas`);

    if (successful < checks.length) {
      const failures = results
        .filter(r => r.status === 'fulfilled' && r.value.status === 'ERROR')
        .map(r => `${r.value.name}: ${r.value.error}`);
      
      logger.warn('⚠️ Verificaciones fallidas:', failures);
    }
  }

  /**
   * Programa monitoreo continuo
   */
  scheduleMonitoring() {
    // Health checks frecuentes
    this.healthCheckTimer = setInterval(() => {
      if (this.isRunning) {
        this.performHealthCheck().catch(error => {
          logger.error('❌ Error en health check:', error.message);
        });
      }
    }, this.healthCheckInterval * 1000);

    // Monitoreo completo menos frecuente
    this.monitoringTimer = setInterval(() => {
      if (this.isRunning) {
        this.runFullMonitoring().catch(error => {
          logger.error('❌ Error en monitoreo completo:', error.message);
        });
      }
    }, this.monitoringInterval * 1000);

    logger.info(`📅 Monitoreo programado: health checks cada ${this.healthCheckInterval}s, monitoreo completo cada ${this.monitoringInterval}s`);
  }

  /**
   * Ejecuta monitoreo completo
   */
  async runFullMonitoring() {
    logger.debug('🔍 Ejecutando monitoreo completo...');
    this.stats.totalChecks++;

    try {
      const results = await Promise.allSettled([
        this.performHealthCheck(),
        this.monitorMetrics(),
        this.monitorCosts(),
        this.monitorAlerts(),
        this.monitorCleanup(),
        this.monitorResources()
      ]);

      const successful = results.filter(r => r.status === 'fulfilled').length;
      
      if (successful === results.length) {
        this.stats.healthyChecks++;
        logger.debug('✅ Monitoreo completo exitoso');
      } else {
        logger.warn(`⚠️ Monitoreo parcialmente exitoso: ${successful}/${results.length}`);
      }

      // Generar reporte periódico
      if (this.stats.totalChecks % 12 === 0) { // Cada hora (12 * 5 min)
        await this.generatePeriodicReport();
      }

    } catch (error) {
      logger.error('❌ Error en monitoreo completo:', error.message);
    }
  }

  /**
   * Realiza health check
   */
  async performHealthCheck() {
    const health = await this.getSystemHealth();
    
    if (health.overall !== 'healthy') {
      logger.warn(`⚠️ Sistema no saludable: ${health.overall} (${health.score}%)`);
      
      // Alertar si está crítico
      if (health.overall === 'unhealthy') {
        await this.alertManager.alertHighResourceUsage(
          'system_health',
          health.score,
          80
        );
      }
    }

    return health;
  }

  /**
   * Verifica estado de GCP
   */
  async checkGCPStatus() {
    const status = gcpConfig.getStatus();
    
    if (!status.credentialsValid && !status.simulationMode) {
      throw new Error(`GCP credentials invalid: ${status.credentialsMessage}`);
    }

    return status;
  }

  /**
   * Verifica recolección de métricas
   */
  async checkMetricsCollection() {
    const metrics = await this.metricsCollector.getMetrics();
    
    if (!metrics || !metrics.system) {
      throw new Error('Metrics collection not working');
    }

    return metrics;
  }

  /**
   * Verifica sistema de alertas
   */
  async checkAlertSystem() {
    const status = this.alertManager.getStatus();
    
    if (!status.enabled) {
      logger.warn('⚠️ Sistema de alertas deshabilitado');
    }

    return status;
  }

  /**
   * Verifica monitoreo de costos
   */
  async checkCostMonitoring() {
    const status = this.costMonitor.getStatus();
    
    if (!status.enabled) {
      logger.warn('⚠️ Monitoreo de costos deshabilitado');
    }

    return status;
  }

  /**
   * Verifica sistema de limpieza
   */
  async checkCleanupSystem() {
    const stats = this.autoCleanup.getStats();
    
    if (!stats.configuration?.enabled) {
      logger.warn('⚠️ Sistema de limpieza deshabilitado');
    }

    return stats;
  }

  /**
   * Monitorea métricas
   */
  async monitorMetrics() {
    await this.metricsCollector.updateSystemMetrics();
    
    // Verificar métricas críticas
    const metrics = await this.metricsCollector.getMetrics();
    
    if (metrics.system?.memoryUsage) {
      const memUsage = (metrics.system.memoryUsage.heapUsed / metrics.system.memoryUsage.heapTotal) * 100;
      
      if (memUsage > 85) {
        await this.alertManager.alertHighResourceUsage('memory', memUsage, 85);
      }
    }
  }

  /**
   * Monitorea costos
   */
  async monitorCosts() {
    await this.costMonitor.monitorQuotas();
    
    const report = await this.costMonitor.getCostReport();
    
    if (report.summary?.isOverThreshold) {
      this.stats.alerts++;
      logger.warn(`💰 Umbral de costos superado: $${report.summary.totalCost}`);
    }
  }

  /**
   * Monitorea alertas
   */
  async monitorAlerts() {
    await this.alertManager.monitorAndAlert();
    this.alertManager.cleanupCooldowns();
  }

  /**
   * Monitorea limpieza
   */
  async monitorCleanup() {
    // Verificar si es necesaria limpieza de emergencia
    await this.autoCleanup.checkDiskSpace();
  }

  /**
   * Monitorea recursos del sistema
   */
  async monitorResources() {
    if (process.env.RESOURCE_USAGE_TRACKING !== 'true') return;

    const usage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();
    
    // Log de recursos si está habilitado
    if (process.env.DETAILED_PERFORMANCE_METRICS === 'true') {
      logger.debug('📊 Uso de recursos:', {
        memory: {
          rss: Math.round(usage.rss / 1024 / 1024),
          heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
          heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
          external: Math.round(usage.external / 1024 / 1024)
        },
        cpu: {
          user: cpuUsage.user,
          system: cpuUsage.system
        }
      });
    }
  }

  /**
   * Obtiene salud del sistema
   */
  async getSystemHealth() {
    try {
      const checks = [];

      // Check GCP
      const gcpStatus = gcpConfig.getStatus();
      checks.push({
        name: 'GCP Connection',
        healthy: gcpStatus.credentialsValid || gcpStatus.simulationMode,
        message: gcpStatus.credentialsMessage
      });

      // Check métricas
      const metrics = await this.metricsCollector.getMetrics();
      checks.push({
        name: 'Metrics Collection',
        healthy: !!metrics?.system,
        message: metrics?.system ? 'OK' : 'No system metrics'
      });

      // Check memoria
      if (metrics?.system?.memoryUsage) {
        const memUsage = (metrics.system.memoryUsage.heapUsed / metrics.system.memoryUsage.heapTotal) * 100;
        checks.push({
          name: 'Memory Usage',
          healthy: memUsage < 80,
          message: `${Math.round(memUsage)}% used`
        });
      }

      // Check alertas
      const alertStatus = this.alertManager.getStatus();
      checks.push({
        name: 'Alert System',
        healthy: alertStatus.enabled,
        message: alertStatus.enabled ? 'Active' : 'Disabled'
      });

      const healthyCount = checks.filter(c => c.healthy).length;
      const score = Math.round((healthyCount / checks.length) * 100);
      
      let overall = 'healthy';
      if (score < 50) overall = 'unhealthy';
      else if (score < 80) overall = 'degraded';

      return {
        overall,
        score,
        checks,
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      return {
        overall: 'unhealthy',
        score: 0,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Genera reporte periódico
   */
  async generatePeriodicReport() {
    try {
      const report = {
        timestamp: new Date().toISOString(),
        uptime: Date.now() - this.stats.startTime.getTime(),
        stats: { ...this.stats },
        health: await this.getSystemHealth(),
        metrics: await this.metricsCollector.getMetrics(),
        costs: await this.costMonitor.getCostReport(),
        cleanup: this.autoCleanup.getStats()
      };

      // Guardar reporte
      const reportPath = `tmp/monitoring-report-${Date.now()}.json`;
      await fs.writeFile(reportPath, JSON.stringify(report, null, 2));

      logger.info('📋 Reporte periódico generado:', {
        uptime: Math.round(report.uptime / 1000 / 60),
        health: report.health.overall,
        totalChecks: report.stats.totalChecks,
        path: reportPath
      });

    } catch (error) {
      logger.error('❌ Error generando reporte periódico:', error.message);
    }
  }

  /**
   * Obtiene estadísticas del monitor
   */
  getStats() {
    return {
      ...this.stats,
      isRunning: this.isRunning,
      uptime: Date.now() - this.stats.startTime.getTime(),
      healthRate: this.stats.totalChecks > 0 ? 
        Math.round((this.stats.healthyChecks / this.stats.totalChecks) * 100) : 0
    };
  }
}

// Manejo de señales para shutdown graceful
let monitor;

process.on('SIGINT', () => {
  logger.info('📡 Recibida señal SIGINT, deteniendo monitor...');
  if (monitor) {
    monitor.stop();
  }
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('📡 Recibida señal SIGTERM, deteniendo monitor...');
  if (monitor) {
    monitor.stop();
  }
  process.exit(0);
});

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  monitor = new ProductionMonitor();
  
  monitor.start()
    .then(() => {
      logger.info('🎯 Monitor de producción ejecutándose...');
      
      // Mantener el proceso vivo
      setInterval(() => {
        const stats = monitor.getStats();
        logger.info('📊 Stats del monitor:', {
          uptime: Math.round(stats.uptime / 1000 / 60),
          checks: stats.totalChecks,
          healthRate: stats.healthRate
        });
      }, 10 * 60 * 1000); // Cada 10 minutos
    })
    .catch(error => {
      logger.error('❌ Error iniciando monitor:', error.message);
      process.exit(1);
    });
}

export { ProductionMonitor };