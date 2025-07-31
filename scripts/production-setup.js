#!/usr/bin/env node

/**
 * Script de configuración para producción
 * Configura monitoreo, alertas y limpieza automática
 */

import { logger } from '../src/utils/logger.js';
import { validateConfig } from '../src/config/env.js';
import { validateGCPConfig } from '../src/config/gcpConfig.js';
import { MetricsCollector } from '../src/utils/MetricsCollector.js';
import { AlertManager } from '../src/utils/AlertManager.js';
import { CostMonitor } from '../src/utils/CostMonitor.js';
import { AutoCleanup } from '../src/utils/AutoCleanup.js';
import fs from 'fs/promises';
import path from 'path';

class ProductionSetup {
  constructor() {
    this.metricsCollector = MetricsCollector.getInstance();
    this.alertManager = new AlertManager();
    this.costMonitor = new CostMonitor();
    this.autoCleanup = new AutoCleanup();
  }

  /**
   * Ejecuta configuración completa de producción
   */
  async run() {
    try {
      logger.info('🚀 Iniciando configuración de producción...');

      // Validar configuración
      await this.validateConfiguration();

      // Crear directorios necesarios
      await this.createDirectories();

      // Configurar logging de producción
      await this.setupProductionLogging();

      // Verificar servicios GCP
      await this.verifyGCPServices();

      // Configurar monitoreo
      await this.setupMonitoring();

      // Configurar alertas
      await this.setupAlerts();

      // Configurar limpieza automática
      await this.setupAutoCleanup();

      // Configurar optimización de costos
      await this.setupCostOptimization();

      // Configurar monitoreo avanzado
      await this.setupAdvancedMonitoring();

      // Generar reporte inicial
      await this.generateInitialReport();

      logger.info('✅ Configuración de producción completada exitosamente');
      
      return {
        success: true,
        message: 'Producción configurada correctamente',
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en configuración de producción:', error.message);
      throw error;
    }
  }

  /**
   * Valida configuración de producción
   */
  async validateConfiguration() {
    logger.info('🔍 Validando configuración...');

    // Validar variables de entorno básicas
    validateConfig();

    // Validar configuración GCP
    validateGCPConfig();

    // Validar variables específicas de producción
    const requiredProdVars = [
      'NODE_ENV',
      'GCP_PROJECT_ID',
      'GCS_BUCKET_NAME',
      'BIGQUERY_DATASET_ID'
    ];

    const missing = requiredProdVars.filter(key => !process.env[key]);
    if (missing.length > 0) {
      throw new Error(`Variables de producción faltantes: ${missing.join(', ')}`);
    }

    // Verificar que estamos en modo producción
    if (process.env.NODE_ENV !== 'production') {
      logger.warn('⚠️ NODE_ENV no está configurado como "production"');
    }

    logger.info('✅ Configuración validada');
  }

  /**
   * Crea directorios necesarios
   */
  async createDirectories() {
    logger.info('📁 Creando directorios...');

    const directories = [
      'logs',
      'tmp/backup',
      'tmp/gcs-simulation',
      'tmp/bigquery-simulation',
      'tmp/metrics-history'
    ];

    for (const dir of directories) {
      try {
        await fs.mkdir(dir, { recursive: true });
        logger.debug(`📁 Directorio creado: ${dir}`);
      } catch (error) {
        if (error.code !== 'EEXIST') {
          logger.warn(`⚠️ Error creando directorio ${dir}:`, error.message);
        }
      }
    }

    logger.info('✅ Directorios configurados');
  }

  /**
   * Configura logging de producción
   */
  async setupProductionLogging() {
    logger.info('📝 Configurando logging de producción...');

    // Verificar configuración de logs
    const logConfig = {
      level: process.env.LOG_LEVEL || 'info',
      format: process.env.LOG_FORMAT || 'json',
      file: process.env.LOG_FILE || 'logs/app.log',
      maxSize: process.env.LOG_MAX_SIZE || '50m',
      maxFiles: parseInt(process.env.LOG_MAX_FILES) || 10
    };

    logger.info('📝 Configuración de logs:', logConfig);
    logger.info('✅ Logging configurado');
  }

  /**
   * Verifica servicios GCP
   */
  async verifyGCPServices() {
    logger.info('☁️ Verificando servicios GCP...');

    try {
      // Verificar credenciales
      const gcpStatus = await import('../src/config/gcpConfig.js');
      const status = gcpStatus.gcpConfig.getStatus();

      if (!status.credentialsValid && !status.simulationMode) {
        throw new Error(`Credenciales GCP inválidas: ${status.credentialsMessage}`);
      }

      logger.info('✅ Servicios GCP verificados:', {
        mode: status.credentialsMode,
        project: status.projectId,
        bucket: status.gcs.bucketName,
        dataset: status.bigQuery.datasetId
      });

    } catch (error) {
      logger.error('❌ Error verificando GCP:', error.message);
      throw error;
    }
  }

  /**
   * Configura monitoreo
   */
  async setupMonitoring() {
    logger.info('📊 Configurando monitoreo...');

    // Inicializar métricas
    await this.metricsCollector.updateSystemMetrics();

    // Configurar monitoreo de costos
    const costStatus = this.costMonitor.getStatus();
    logger.info('💰 Monitor de costos:', {
      enabled: costStatus.enabled,
      dailyReport: costStatus.dailyReportEnabled,
      quotas: costStatus.quotas
    });

    logger.info('✅ Monitoreo configurado');
  }

  /**
   * Configura sistema de alertas
   */
  async setupAlerts() {
    logger.info('🚨 Configurando alertas...');

    const alertStatus = this.alertManager.getStatus();
    
    if (!alertStatus.enabled) {
      logger.warn('⚠️ Sistema de alertas deshabilitado');
      return;
    }

    logger.info('🚨 Configuración de alertas:', {
      enabled: alertStatus.enabled,
      webhook: alertStatus.webhookConfigured,
      email: alertStatus.emailEnabled,
      thresholds: alertStatus.thresholds
    });

    // Enviar alerta de prueba si está configurado
    if (process.env.SEND_TEST_ALERT === 'true') {
      try {
        await this.alertManager.alertGCSFailure(
          new Error('Alerta de prueba - configuración de producción'),
          'production_setup',
          'test',
          0
        );
        logger.info('📧 Alerta de prueba enviada');
      } catch (error) {
        logger.warn('⚠️ Error enviando alerta de prueba:', error.message);
      }
    }

    logger.info('✅ Alertas configuradas');
  }

  /**
   * Configura limpieza automática
   */
  async setupAutoCleanup() {
    logger.info('🧹 Configurando limpieza automática...');

    const cleanupStats = this.autoCleanup.getStats();
    
    if (!cleanupStats.configuration.enabled) {
      logger.warn('⚠️ Limpieza automática deshabilitada');
      return;
    }

    logger.info('🧹 Configuración de limpieza:', {
      enabled: cleanupStats.configuration.enabled,
      schedule: cleanupStats.configuration.schedule,
      retention: cleanupStats.configuration.retention
    });

    // Obtener estimación de limpieza
    const estimate = await this.autoCleanup.getCleanupEstimate();
    logger.info('📊 Estimación de limpieza:', {
      files: estimate.estimatedFiles,
      size: estimate.formattedSize
    });

    // Programar limpieza automática
    this.autoCleanup.scheduleCleanup();

    logger.info('✅ Limpieza automática configurada');
  }

  /**
   * Configura optimización de costos
   */
  async setupCostOptimization() {
    logger.info('💰 Configurando optimización de costos...');

    const costStatus = this.costMonitor.getStatus();
    
    if (!costStatus.optimizationEnabled) {
      logger.warn('⚠️ Optimización de costos deshabilitada');
      return;
    }

    logger.info('💰 Configuración de optimización:', {
      enabled: costStatus.optimizationEnabled,
      features: costStatus.features
    });

    // Programar optimización automática
    this.costMonitor.scheduleOptimization();

    // Generar reporte inicial de optimización
    const optimizationReport = await this.costMonitor.generateOptimizationReport();
    logger.info('📊 Oportunidades de optimización:', {
      totalPotentialSaving: optimizationReport.totalPotentialSaving,
      opportunities: optimizationReport.opportunities?.length || 0
    });

    logger.info('✅ Optimización de costos configurada');
  }

  /**
   * Configura monitoreo avanzado
   */
  async setupAdvancedMonitoring() {
    if (process.env.ADVANCED_MONITORING_ENABLED !== 'true') {
      logger.info('📊 Monitoreo avanzado deshabilitado');
      return;
    }

    logger.info('📊 Configurando monitoreo avanzado...');

    // Configurar profiling de performance
    if (process.env.PERFORMANCE_PROFILING === 'true') {
      logger.info('🔍 Performance profiling habilitado');
    }

    // Configurar tracking de recursos
    if (process.env.RESOURCE_USAGE_TRACKING === 'true') {
      logger.info('📈 Resource usage tracking habilitado');
    }

    // Configurar monitoreo de quotas GCP
    if (process.env.GCP_QUOTA_MONITORING === 'true') {
      logger.info('☁️ GCP quota monitoring habilitado');
      
      // Programar monitoreo de quotas
      setInterval(() => {
        this.costMonitor.monitorQuotas().catch(error => {
          logger.error('❌ Error monitoreando quotas:', error.message);
        });
      }, 60 * 60 * 1000); // Cada hora
    }

    logger.info('✅ Monitoreo avanzado configurado');
  }

  /**
   * Genera reporte inicial de estado
   */
  async generateInitialReport() {
    logger.info('📋 Generando reporte inicial...');

    try {
      // Obtener métricas actuales
      const metrics = await this.metricsCollector.getMetrics();
      
      // Obtener reporte de costos
      const costReport = await this.costMonitor.getCostReport();
      
      // Obtener estado de alertas
      const alertStatus = this.alertManager.getStatus();
      
      // Obtener estadísticas de limpieza
      const cleanupStats = this.autoCleanup.getStats();

      const report = {
        timestamp: new Date().toISOString(),
        environment: process.env.NODE_ENV,
        version: '2.0.0',
        system: {
          uptime: metrics.system?.uptime || 0,
          memory: metrics.system?.memoryUsage,
          startTime: metrics.system?.startTime
        },
        services: {
          gcs: {
            configured: true,
            bucket: process.env.GCS_BUCKET_NAME,
            uploads: metrics.gcs?.uploads || {}
          },
          bigQuery: {
            configured: true,
            dataset: process.env.BIGQUERY_DATASET_ID,
            batchJobs: metrics.bigquery?.batchJobs || {}
          }
        },
        monitoring: {
          alerts: {
            enabled: alertStatus.enabled,
            webhook: alertStatus.webhookConfigured,
            email: alertStatus.emailEnabled
          },
          costs: {
            enabled: costReport.enabled !== false,
            totalCost: costReport.summary?.totalCost || 0,
            threshold: costReport.summary?.threshold || 0
          },
          cleanup: {
            enabled: cleanupStats.configuration?.enabled || false,
            lastRun: cleanupStats.lastRun,
            totalFilesDeleted: cleanupStats.totalFilesDeleted || 0
          }
        }
      };

      // Guardar reporte
      const reportPath = 'tmp/production-setup-report.json';
      await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
      
      logger.info('📋 Reporte inicial guardado en:', reportPath);
      logger.info('✅ Reporte inicial generado');

      return report;

    } catch (error) {
      logger.error('❌ Error generando reporte inicial:', error.message);
      throw error;
    }
  }

  /**
   * Verifica estado de salud post-configuración
   */
  async healthCheck() {
    logger.info('🏥 Verificando estado de salud...');

    const checks = [];

    try {
      // Check configuración
      validateConfig();
      checks.push({ name: 'Configuration', status: 'healthy' });
    } catch (error) {
      checks.push({ name: 'Configuration', status: 'unhealthy', error: error.message });
    }

    try {
      // Check GCP
      validateGCPConfig();
      checks.push({ name: 'GCP Services', status: 'healthy' });
    } catch (error) {
      checks.push({ name: 'GCP Services', status: 'unhealthy', error: error.message });
    }

    try {
      // Check métricas
      await this.metricsCollector.getMetrics();
      checks.push({ name: 'Metrics', status: 'healthy' });
    } catch (error) {
      checks.push({ name: 'Metrics', status: 'unhealthy', error: error.message });
    }

    const healthyChecks = checks.filter(check => check.status === 'healthy').length;
    const overallHealth = healthyChecks === checks.length ? 'healthy' : 'degraded';

    const healthReport = {
      overall: overallHealth,
      score: Math.round((healthyChecks / checks.length) * 100),
      checks,
      timestamp: new Date().toISOString()
    };

    logger.info('🏥 Estado de salud:', healthReport);
    return healthReport;
  }
}

// Ejecutar si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  const setup = new ProductionSetup();
  
  setup.run()
    .then(result => {
      console.log('✅ Configuración completada:', result);
      
      // Ejecutar health check final
      return setup.healthCheck();
    })
    .then(health => {
      console.log('🏥 Health check final:', health);
      process.exit(health.overall === 'healthy' ? 0 : 1);
    })
    .catch(error => {
      console.error('❌ Error en configuración:', error.message);
      process.exit(1);
    });
}

export { ProductionSetup };