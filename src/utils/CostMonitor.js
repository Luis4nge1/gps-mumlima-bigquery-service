import { logger } from './logger.js';
import { MetricsCollector } from './MetricsCollector.js';
import { AlertManager } from './AlertManager.js';

/**
 * Monitor de costos y uso de recursos GCP
 */
export class CostMonitor {
  constructor() {
    this.enabled = process.env.COST_MONITORING_ENABLED === 'true';
    this.dailyReportEnabled = process.env.COST_DAILY_REPORT === 'true';
    
    // Quotas y límites
    this.quotas = {
      gcsStorageGB: parseFloat(process.env.GCS_STORAGE_QUOTA_GB) || 1000,
      bigQueryQueryTB: parseFloat(process.env.BIGQUERY_QUERY_QUOTA_TB) || 10
    };

    // Precios GCP (USD) - Actualizar según región y tipo de servicio
    this.pricing = {
      gcs: {
        storagePerGBMonth: 0.020, // Standard Storage
        operationsPer1000: 0.005, // Class A operations
        networkEgressPerGB: 0.12  // Network egress
      },
      bigQuery: {
        queryPerTB: 5.00,         // On-demand queries
        storagePerGBMonth: 0.020, // Active storage
        streamingPerGB: 0.010     // Streaming inserts
      }
    };

    this.metricsCollector = MetricsCollector.getInstance();
    this.alertManager = new AlertManager();
    
    // Cache para evitar cálculos repetitivos
    this.costCache = {
      lastUpdate: null,
      data: null,
      ttl: 5 * 60 * 1000 // 5 minutos
    };
  }

  /**
   * Calcula costos estimados de GCS
   */
  async calculateGCSCosts() {
    try {
      const gcsMetrics = await this.metricsCollector.getGCSMetrics();
      
      // Costo de almacenamiento (por mes)
      const storageGB = gcsMetrics.storage.totalSize / (1024 * 1024 * 1024); // Convertir a GB
      const storageCost = storageGB * this.pricing.gcs.storagePerGBMonth;

      // Costo de operaciones (uploads)
      const totalOperations = gcsMetrics.summary.totalUploads;
      const operationsCost = (totalOperations / 1000) * this.pricing.gcs.operationsPer1000;

      // Estimación de transferencia de datos (asumiendo promedio)
      const avgTransferGB = (gcsMetrics.summary.totalStorageSize || 0) / (1024 * 1024 * 1024);
      const transferCost = avgTransferGB * this.pricing.gcs.networkEgressPerGB;

      return {
        storage: {
          sizeGB: Math.round(storageGB * 100) / 100,
          cost: Math.round(storageCost * 100) / 100,
          quotaUsage: (storageGB / this.quotas.gcsStorageGB) * 100
        },
        operations: {
          count: totalOperations,
          cost: Math.round(operationsCost * 100) / 100
        },
        transfer: {
          sizeGB: Math.round(avgTransferGB * 100) / 100,
          cost: Math.round(transferCost * 100) / 100
        },
        total: Math.round((storageCost + operationsCost + transferCost) * 100) / 100
      };

    } catch (error) {
      logger.error('❌ Error calculando costos GCS:', error.message);
      return this.getEmptyGCSCosts();
    }
  }

  /**
   * Calcula costos estimados de BigQuery
   */
  async calculateBigQueryCosts() {
    try {
      const bqMetrics = await this.metricsCollector.getBigQueryMetrics();
      
      // Costo de queries (por TB procesado)
      const totalRecords = bqMetrics.summary.totalRecordsProcessed || 0;
      const estimatedDataTB = (totalRecords * 0.5) / (1024 * 1024 * 1024 * 1024); // Estimación: 0.5KB por record
      const queryCost = estimatedDataTB * this.pricing.bigQuery.queryPerTB;

      // Costo de almacenamiento (estimado)
      const storageTB = estimatedDataTB * 0.1; // Asumiendo compresión
      const storageCost = storageTB * 1024 * this.pricing.bigQuery.storagePerGBMonth;

      // Costo de streaming (si aplica)
      const streamingCost = (totalRecords / (1024 * 1024)) * this.pricing.bigQuery.streamingPerGB;

      return {
        queries: {
          dataTB: Math.round(estimatedDataTB * 1000) / 1000,
          cost: Math.round(queryCost * 100) / 100,
          quotaUsage: (estimatedDataTB / this.quotas.bigQueryQueryTB) * 100
        },
        storage: {
          dataTB: Math.round(storageTB * 1000) / 1000,
          cost: Math.round(storageCost * 100) / 100
        },
        streaming: {
          records: totalRecords,
          cost: Math.round(streamingCost * 100) / 100
        },
        total: Math.round((queryCost + storageCost + streamingCost) * 100) / 100
      };

    } catch (error) {
      logger.error('❌ Error calculando costos BigQuery:', error.message);
      return this.getEmptyBigQueryCosts();
    }
  }

  /**
   * Obtiene reporte completo de costos
   */
  async getCostReport() {
    if (!this.enabled) {
      return { enabled: false, message: 'Monitoreo de costos deshabilitado' };
    }

    // Verificar cache
    if (this.costCache.data && this.costCache.lastUpdate && 
        (Date.now() - this.costCache.lastUpdate) < this.costCache.ttl) {
      return this.costCache.data;
    }

    try {
      const [gcsCosts, bqCosts] = await Promise.all([
        this.calculateGCSCosts(),
        this.calculateBigQueryCosts()
      ]);

      const totalCost = gcsCosts.total + bqCosts.total;
      const costThreshold = parseFloat(process.env.COST_ALERT_THRESHOLD) || 100.0;

      const report = {
        timestamp: new Date().toISOString(),
        gcs: gcsCosts,
        bigQuery: bqCosts,
        summary: {
          totalCost,
          threshold: costThreshold,
          thresholdUsage: (totalCost / costThreshold) * 100,
          isOverThreshold: totalCost > costThreshold
        },
        quotas: {
          gcsStorage: {
            limit: this.quotas.gcsStorageGB,
            used: gcsCosts.storage.sizeGB,
            usage: gcsCosts.storage.quotaUsage
          },
          bigQueryQuery: {
            limit: this.quotas.bigQueryQueryTB,
            used: bqCosts.queries.dataTB,
            usage: bqCosts.queries.quotaUsage
          }
        },
        recommendations: this.generateCostRecommendations(gcsCosts, bqCosts, totalCost)
      };

      // Actualizar cache
      this.costCache.data = report;
      this.costCache.lastUpdate = Date.now();

      // Verificar alertas
      if (report.summary.isOverThreshold) {
        await this.alertManager.alertHighCosts(totalCost, costThreshold);
      }

      return report;

    } catch (error) {
      logger.error('❌ Error generando reporte de costos:', error.message);
      return { error: error.message, timestamp: new Date().toISOString() };
    }
  }

  /**
   * Genera recomendaciones de optimización de costos
   */
  generateCostRecommendations(gcsCosts, bqCosts, totalCost) {
    const recommendations = [];

    // Recomendaciones GCS
    if (gcsCosts.storage.quotaUsage > 80) {
      recommendations.push({
        type: 'GCS_STORAGE',
        priority: 'HIGH',
        message: 'Alto uso de almacenamiento GCS',
        action: 'Configurar limpieza automática de archivos antiguos',
        estimatedSaving: gcsCosts.storage.cost * 0.3
      });
    }

    if (gcsCosts.operations.count > 10000) {
      recommendations.push({
        type: 'GCS_OPERATIONS',
        priority: 'MEDIUM',
        message: 'Alto número de operaciones GCS',
        action: 'Optimizar batch size para reducir número de uploads',
        estimatedSaving: gcsCosts.operations.cost * 0.2
      });
    }

    // Recomendaciones BigQuery
    if (bqCosts.queries.quotaUsage > 70) {
      recommendations.push({
        type: 'BIGQUERY_QUERIES',
        priority: 'HIGH',
        message: 'Alto uso de quota de queries BigQuery',
        action: 'Optimizar queries y usar particionado de tablas',
        estimatedSaving: bqCosts.queries.cost * 0.4
      });
    }

    if (bqCosts.storage.cost > 10) {
      recommendations.push({
        type: 'BIGQUERY_STORAGE',
        priority: 'MEDIUM',
        message: 'Costo de almacenamiento BigQuery elevado',
        action: 'Implementar particionado y clustering de tablas',
        estimatedSaving: bqCosts.storage.cost * 0.25
      });
    }

    // Recomendaciones generales
    if (totalCost > 50) {
      recommendations.push({
        type: 'GENERAL',
        priority: 'MEDIUM',
        message: 'Costos totales elevados',
        action: 'Revisar configuración de retención y batch sizes',
        estimatedSaving: totalCost * 0.15
      });
    }

    return recommendations;
  }

  /**
   * Genera reporte diario de costos
   */
  async generateDailyReport() {
    if (!this.dailyReportEnabled) return;

    try {
      const report = await this.getCostReport();
      
      logger.info('📊 Reporte Diario de Costos GCP', {
        totalCost: report.summary?.totalCost,
        gcsCost: report.gcs?.total,
        bigQueryCost: report.bigQuery?.total,
        recommendations: report.recommendations?.length || 0
      });

      // Actualizar métricas
      await this.metricsCollector.updateGCPCosts(
        report.gcs?.storage?.sizeGB || 0,
        report.bigQuery?.queries?.dataTB || 0
      );

      return report;

    } catch (error) {
      logger.error('❌ Error generando reporte diario:', error.message);
    }
  }

  /**
   * Monitorea quotas y envía alertas
   */
  async monitorQuotas() {
    if (!this.enabled) return;

    try {
      const report = await this.getCostReport();

      // Alertar si se acerca a quotas
      if (report.quotas?.gcsStorage?.usage > 90) {
        await this.alertManager.alertHighResourceUsage(
          'gcs_storage',
          report.quotas.gcsStorage.usage,
          90
        );
      }

      if (report.quotas?.bigQueryQuery?.usage > 90) {
        await this.alertManager.alertHighResourceUsage(
          'bigquery_quota',
          report.quotas.bigQueryQuery.usage,
          90
        );
      }

    } catch (error) {
      logger.error('❌ Error monitoreando quotas:', error.message);
    }
  }

  /**
   * Obtiene costos GCS vacíos (fallback)
   */
  getEmptyGCSCosts() {
    return {
      storage: { sizeGB: 0, cost: 0, quotaUsage: 0 },
      operations: { count: 0, cost: 0 },
      transfer: { sizeGB: 0, cost: 0 },
      total: 0
    };
  }

  /**
   * Obtiene costos BigQuery vacíos (fallback)
   */
  getEmptyBigQueryCosts() {
    return {
      queries: { dataTB: 0, cost: 0, quotaUsage: 0 },
      storage: { dataTB: 0, cost: 0 },
      streaming: { records: 0, cost: 0 },
      total: 0
    };
  }

  /**
   * Limpia cache de costos
   */
  clearCache() {
    this.costCache.data = null;
    this.costCache.lastUpdate = null;
  }

  /**
   * Optimiza costos automáticamente
   */
  async optimizeCosts() {
    if (!process.env.COST_OPTIMIZATION_ENABLED === 'true') return;

    try {
      logger.info('💰 Iniciando optimización automática de costos');
      
      const report = await this.getCostReport();
      const optimizations = [];

      // Optimización de almacenamiento GCS
      if (report.gcs?.storage?.quotaUsage > 70) {
        optimizations.push(await this.optimizeGCSStorage());
      }

      // Optimización de queries BigQuery
      if (report.bigQuery?.queries?.quotaUsage > 60) {
        optimizations.push(await this.optimizeBigQueryQueries());
      }

      // Aplicar optimizaciones
      const results = await Promise.allSettled(optimizations);
      const successful = results.filter(r => r.status === 'fulfilled').length;

      logger.info(`✅ Optimización completada: ${successful}/${optimizations.length} exitosas`);
      
      return {
        totalOptimizations: optimizations.length,
        successful,
        estimatedSavings: this.calculateEstimatedSavings(results)
      };

    } catch (error) {
      logger.error('❌ Error en optimización de costos:', error.message);
      throw error;
    }
  }

  /**
   * Optimiza almacenamiento GCS
   */
  async optimizeGCSStorage() {
    const optimizations = [];

    // Configurar lifecycle management
    if (process.env.LIFECYCLE_MANAGEMENT === 'true') {
      optimizations.push({
        type: 'lifecycle',
        action: 'Configurar reglas de lifecycle para archivos antiguos',
        estimatedSaving: 0.3
      });
    }

    // Habilitar compresión
    if (process.env.COMPRESSION_ENABLED === 'true') {
      optimizations.push({
        type: 'compression',
        action: 'Habilitar compresión de archivos',
        estimatedSaving: 0.4
      });
    }

    // Intelligent tiering
    if (process.env.INTELLIGENT_TIERING === 'true') {
      optimizations.push({
        type: 'tiering',
        action: 'Configurar intelligent tiering',
        estimatedSaving: 0.25
      });
    }

    return optimizations;
  }

  /**
   * Optimiza queries BigQuery
   */
  async optimizeBigQueryQueries() {
    const optimizations = [];

    // Configurar particionado
    optimizations.push({
      type: 'partitioning',
      action: 'Implementar particionado de tablas por fecha',
      estimatedSaving: 0.5
    });

    // Configurar clustering
    optimizations.push({
      type: 'clustering',
      action: 'Implementar clustering en columnas frecuentes',
      estimatedSaving: 0.3
    });

    // Optimizar batch sizes
    optimizations.push({
      type: 'batch_optimization',
      action: 'Optimizar tamaños de batch para reducir costos',
      estimatedSaving: 0.2
    });

    return optimizations;
  }

  /**
   * Calcula ahorros estimados
   */
  calculateEstimatedSavings(results) {
    let totalSavings = 0;
    
    results.forEach(result => {
      if (result.status === 'fulfilled' && result.value) {
        const optimizations = Array.isArray(result.value) ? result.value : [result.value];
        optimizations.forEach(opt => {
          totalSavings += opt.estimatedSaving || 0;
        });
      }
    });

    return Math.round(totalSavings * 100) / 100;
  }

  /**
   * Genera reporte de optimización de costos
   */
  async generateOptimizationReport() {
    try {
      const report = await this.getCostReport();
      const optimizationOpportunities = [];

      // Analizar oportunidades GCS
      if (report.gcs?.storage?.cost > 10) {
        optimizationOpportunities.push({
          service: 'GCS',
          opportunity: 'Implementar lifecycle management',
          currentCost: report.gcs.storage.cost,
          potentialSaving: report.gcs.storage.cost * 0.3,
          priority: 'HIGH',
          implementation: 'Configurar reglas para mover archivos antiguos a storage classes más baratos'
        });
      }

      // Analizar oportunidades BigQuery
      if (report.bigQuery?.queries?.cost > 20) {
        optimizationOpportunities.push({
          service: 'BigQuery',
          opportunity: 'Optimizar queries con particionado',
          currentCost: report.bigQuery.queries.cost,
          potentialSaving: report.bigQuery.queries.cost * 0.4,
          priority: 'HIGH',
          implementation: 'Implementar particionado por fecha en tablas principales'
        });
      }

      // Oportunidades generales
      if (report.summary?.totalCost > 50) {
        optimizationOpportunities.push({
          service: 'General',
          opportunity: 'Revisar configuración de retención',
          currentCost: report.summary.totalCost,
          potentialSaving: report.summary.totalCost * 0.15,
          priority: 'MEDIUM',
          implementation: 'Ajustar períodos de retención y batch sizes'
        });
      }

      return {
        timestamp: new Date().toISOString(),
        totalCurrentCost: report.summary?.totalCost || 0,
        totalPotentialSaving: optimizationOpportunities.reduce((sum, opp) => sum + opp.potentialSaving, 0),
        opportunities: optimizationOpportunities,
        recommendations: this.generateDetailedRecommendations(optimizationOpportunities)
      };

    } catch (error) {
      logger.error('❌ Error generando reporte de optimización:', error.message);
      return { error: error.message };
    }
  }

  /**
   * Genera recomendaciones detalladas
   */
  generateDetailedRecommendations(opportunities) {
    const recommendations = [];

    // Agrupar por prioridad
    const highPriority = opportunities.filter(opp => opp.priority === 'HIGH');
    const mediumPriority = opportunities.filter(opp => opp.priority === 'MEDIUM');

    if (highPriority.length > 0) {
      recommendations.push({
        category: 'Acciones Inmediatas',
        items: highPriority.map(opp => ({
          action: opp.opportunity,
          impact: `Ahorro estimado: $${opp.potentialSaving.toFixed(2)}`,
          steps: opp.implementation
        }))
      });
    }

    if (mediumPriority.length > 0) {
      recommendations.push({
        category: 'Optimizaciones a Mediano Plazo',
        items: mediumPriority.map(opp => ({
          action: opp.opportunity,
          impact: `Ahorro estimado: $${opp.potentialSaving.toFixed(2)}`,
          steps: opp.implementation
        }))
      });
    }

    // Recomendaciones generales
    recommendations.push({
      category: 'Mejores Prácticas',
      items: [
        {
          action: 'Monitoreo continuo de costos',
          impact: 'Prevención de sobrecostos',
          steps: 'Configurar alertas automáticas y revisiones semanales'
        },
        {
          action: 'Optimización de batch sizes',
          impact: 'Reducción de costos operacionales',
          steps: 'Ajustar tamaños según patrones de uso'
        },
        {
          action: 'Limpieza automática regular',
          impact: 'Reducción de costos de almacenamiento',
          steps: 'Configurar limpieza automática de archivos antiguos'
        }
      ]
    });

    return recommendations;
  }

  /**
   * Programa optimización automática
   */
  scheduleOptimization() {
    if (process.env.COST_OPTIMIZATION_ENABLED !== 'true') return;

    // Ejecutar optimización diaria a las 3 AM
    const schedule = process.env.COST_OPTIMIZATION_SCHEDULE || '0 3 * * *';
    
    logger.info(`💰 Optimización de costos programada: ${schedule}`);
    
    // Ejecutar optimización inicial después de 5 minutos
    setTimeout(() => {
      this.optimizeCosts().catch(error => {
        logger.error('❌ Error en optimización programada:', error.message);
      });
    }, 5 * 60 * 1000);

    // Programar ejecución diaria (simplificado)
    setInterval(() => {
      this.optimizeCosts().catch(error => {
        logger.error('❌ Error en optimización programada:', error.message);
      });
    }, 24 * 60 * 60 * 1000); // 24 horas
  }

  /**
   * Obtiene estado del monitor de costos
   */
  getStatus() {
    return {
      enabled: this.enabled,
      dailyReportEnabled: this.dailyReportEnabled,
      optimizationEnabled: process.env.COST_OPTIMIZATION_ENABLED === 'true',
      quotas: this.quotas,
      pricing: this.pricing,
      cacheStatus: {
        hasData: !!this.costCache.data,
        lastUpdate: this.costCache.lastUpdate,
        age: this.costCache.lastUpdate ? Date.now() - this.costCache.lastUpdate : null
      },
      features: {
        lifecycleManagement: process.env.LIFECYCLE_MANAGEMENT === 'true',
        compression: process.env.COMPRESSION_ENABLED === 'true',
        intelligentTiering: process.env.INTELLIGENT_TIERING === 'true',
        deduplication: process.env.DEDUPLICATION_ENABLED === 'true'
      }
    };
  }
}