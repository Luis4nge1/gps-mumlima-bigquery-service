import { logger } from '../utils/logger.js';
import { MetricsCollector } from '../utils/MetricsCollector.js';
import { CostMonitor } from '../utils/CostMonitor.js';
import { AlertManager } from '../utils/AlertManager.js';
import { AutoCleanup } from '../utils/AutoCleanup.js';
import { gcpConfig } from '../config/gcpConfig.js';

/**
 * Controlador para dashboard de métricas GCS/BigQuery
 */
export class DashboardController {
  constructor() {
    this.metricsCollector = MetricsCollector.getInstance();
    this.costMonitor = new CostMonitor();
    this.alertManager = new AlertManager();
    this.autoCleanup = new AutoCleanup();
    
    // Configuración del dashboard
    this.refreshInterval = parseInt(process.env.DASHBOARD_REFRESH_INTERVAL) || 30; // segundos
    this.historyHours = parseInt(process.env.DASHBOARD_HISTORY_HOURS) || 24;
  }

  /**
   * Obtiene datos completos del dashboard
   */
  async getDashboardData() {
    try {
      const [
        metrics,
        gcsMetrics,
        bigQueryMetrics,
        costReport,
        gcpStatus,
        alertStatus,
        cleanupStats
      ] = await Promise.allSettled([
        this.metricsCollector.getMetrics(),
        this.metricsCollector.getGCSMetrics(),
        this.metricsCollector.getBigQueryMetrics(),
        this.costMonitor.getCostReport(),
        gcpConfig.getStatus(),
        this.alertManager.getStatus(),
        this.autoCleanup.getStats()
      ]);

      return {
        timestamp: new Date().toISOString(),
        refreshInterval: this.refreshInterval,
        overview: this.buildOverview(metrics.value),
        gcs: this.buildGCSSection(gcsMetrics.value),
        bigQuery: this.buildBigQuerySection(bigQueryMetrics.value),
        costs: this.buildCostsSection(costReport.value),
        system: this.buildSystemSection(metrics.value, gcpStatus.value),
        alerts: this.buildAlertsSection(alertStatus.value),
        cleanup: this.buildCleanupSection(cleanupStats.value),
        health: await this.getHealthStatus()
      };

    } catch (error) {
      logger.error('❌ Error obteniendo datos del dashboard:', error.message);
      return {
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Construye sección de overview
   */
  buildOverview(metrics) {
    if (!metrics) return { error: 'Métricas no disponibles' };

    const summary = metrics.summary || {};
    
    return {
      title: 'Resumen General',
      cards: [
        {
          title: 'Tasa de Éxito',
          value: `${summary.successRate || 0}%`,
          trend: this.calculateTrend(summary.successRate, 95),
          color: parseFloat(summary.successRate || 0) > 95 ? 'green' : 'orange'
        },
        {
          title: 'Registros Procesados',
          value: this.formatNumber(summary.totalRecordsProcessed || 0),
          subtitle: 'Total acumulado',
          color: 'blue'
        },
        {
          title: 'Tiempo Promedio',
          value: `${summary.averageProcessingTime || 0}ms`,
          subtitle: 'Por procesamiento',
          color: 'purple'
        },
        {
          title: 'Uptime',
          value: summary.uptime || '0s',
          subtitle: 'Tiempo activo',
          color: 'green'
        }
      ],
      stats: {
        totalRuns: metrics.processing?.totalRuns || 0,
        successfulRuns: metrics.processing?.successfulRuns || 0,
        failedRuns: metrics.processing?.failedRuns || 0,
        lastProcessing: metrics.processing?.lastProcessing
      }
    };
  }

  /**
   * Construye sección de GCS
   */
  buildGCSSection(gcsMetrics) {
    if (!gcsMetrics) return { error: 'Métricas GCS no disponibles' };

    const summary = gcsMetrics.summary || {};
    
    return {
      title: 'Google Cloud Storage',
      cards: [
        {
          title: 'Uploads Exitosos',
          value: `${summary.successRate || 0}%`,
          subtitle: `${summary.successfulUploads || 0}/${summary.totalUploads || 0}`,
          color: parseFloat(summary.successRate || 0) > 90 ? 'green' : 'red'
        },
        {
          title: 'Almacenamiento Total',
          value: summary.totalStorageSize || '0 B',
          subtitle: `${gcsMetrics.storage?.totalFiles || 0} archivos`,
          color: 'blue'
        },
        {
          title: 'Tamaño Promedio',
          value: summary.avgUploadSize || '0 B',
          subtitle: 'Por archivo',
          color: 'purple'
        },
        {
          title: 'Último Upload',
          value: this.formatTimeAgo(gcsMetrics.lastUpload),
          color: 'gray'
        }
      ],
      details: {
        byType: {
          gps: {
            total: gcsMetrics.uploads?.gps?.total || 0,
            successful: gcsMetrics.uploads?.gps?.successful || 0,
            failed: gcsMetrics.uploads?.gps?.failed || 0,
            avgSize: this.formatBytes(gcsMetrics.uploads?.gps?.avgSize || 0),
            avgTime: `${gcsMetrics.uploads?.gps?.avgTime || 0}ms`
          },
          mobile: {
            total: gcsMetrics.uploads?.mobile?.total || 0,
            successful: gcsMetrics.uploads?.mobile?.successful || 0,
            failed: gcsMetrics.uploads?.mobile?.failed || 0,
            avgSize: this.formatBytes(gcsMetrics.uploads?.mobile?.avgSize || 0),
            avgTime: `${gcsMetrics.uploads?.mobile?.avgTime || 0}ms`
          }
        },
        storage: gcsMetrics.storage,
        lastError: gcsMetrics.lastError
      }
    };
  }

  /**
   * Construye sección de BigQuery
   */
  buildBigQuerySection(bqMetrics) {
    if (!bqMetrics) return { error: 'Métricas BigQuery no disponibles' };

    const summary = bqMetrics.summary || {};
    
    return {
      title: 'BigQuery',
      cards: [
        {
          title: 'Batch Jobs Exitosos',
          value: `${summary.successRate || 0}%`,
          subtitle: `${summary.successfulBatchJobs || 0}/${summary.totalBatchJobs || 0}`,
          color: parseFloat(summary.successRate || 0) > 90 ? 'green' : 'red'
        },
        {
          title: 'Registros Procesados',
          value: this.formatNumber(summary.totalRecordsProcessed || 0),
          subtitle: 'En batch jobs',
          color: 'blue'
        },
        {
          title: 'Tiempo Promedio',
          value: `${summary.avgProcessingTime || 0}ms`,
          subtitle: 'Por batch job',
          color: 'purple'
        },
        {
          title: 'Último Job',
          value: this.formatTimeAgo(bqMetrics.lastBatchJob?.timestamp),
          subtitle: bqMetrics.lastBatchJob?.jobId || 'N/A',
          color: 'gray'
        }
      ],
      details: {
        batchJobs: {
          gps: {
            total: bqMetrics.batchJobs?.gps?.total || 0,
            successful: bqMetrics.batchJobs?.gps?.successful || 0,
            failed: bqMetrics.batchJobs?.gps?.failed || 0,
            avgRecords: bqMetrics.batchJobs?.gps?.avgRecords || 0,
            avgTime: `${bqMetrics.batchJobs?.gps?.avgTime || 0}ms`
          },
          mobile: {
            total: bqMetrics.batchJobs?.mobile?.total || 0,
            successful: bqMetrics.batchJobs?.mobile?.successful || 0,
            failed: bqMetrics.batchJobs?.mobile?.failed || 0,
            avgRecords: bqMetrics.batchJobs?.mobile?.avgRecords || 0,
            avgTime: `${bqMetrics.batchJobs?.mobile?.avgTime || 0}ms`
          }
        },
        legacy: bqMetrics.legacy,
        lastError: bqMetrics.lastError
      }
    };
  }

  /**
   * Construye sección de costos
   */
  buildCostsSection(costReport) {
    if (!costReport || costReport.error) {
      return { error: 'Reporte de costos no disponible' };
    }

    const summary = costReport.summary || {};
    
    return {
      title: 'Costos GCP',
      cards: [
        {
          title: 'Costo Total',
          value: `$${summary.totalCost || 0}`,
          subtitle: 'Estimado mensual',
          color: summary.isOverThreshold ? 'red' : 'green'
        },
        {
          title: 'GCS Storage',
          value: `$${costReport.gcs?.total || 0}`,
          subtitle: `${costReport.gcs?.storage?.sizeGB || 0} GB`,
          color: 'blue'
        },
        {
          title: 'BigQuery',
          value: `$${costReport.bigQuery?.total || 0}`,
          subtitle: `${costReport.bigQuery?.queries?.dataTB || 0} TB`,
          color: 'purple'
        },
        {
          title: 'Uso de Umbral',
          value: `${Math.round(summary.thresholdUsage || 0)}%`,
          subtitle: `Límite: $${summary.threshold || 0}`,
          color: summary.thresholdUsage > 80 ? 'orange' : 'green'
        }
      ],
      details: {
        breakdown: costReport,
        recommendations: costReport.recommendations || [],
        quotas: costReport.quotas
      }
    };
  }

  /**
   * Construye sección del sistema
   */
  buildSystemSection(metrics, gcpStatus) {
    const system = metrics?.system || {};
    const memUsage = system.memoryUsage || {};
    
    return {
      title: 'Estado del Sistema',
      cards: [
        {
          title: 'Memoria Usada',
          value: `${memUsage.heapUsed || 0} MB`,
          subtitle: `de ${memUsage.heapTotal || 0} MB`,
          color: this.getMemoryColor(memUsage.heapUsed, memUsage.heapTotal)
        },
        {
          title: 'GCP Status',
          value: gcpStatus?.credentialsValid ? 'Conectado' : 'Desconectado',
          subtitle: gcpStatus?.credentialsMode || 'unknown',
          color: gcpStatus?.credentialsValid ? 'green' : 'red'
        },
        {
          title: 'Proyecto GCP',
          value: gcpStatus?.projectId || 'N/A',
          subtitle: gcpStatus?.gcs?.bucketName || 'N/A',
          color: 'blue'
        },
        {
          title: 'Último Health Check',
          value: this.formatTimeAgo(system.lastHealthCheck?.timestamp),
          subtitle: system.lastHealthCheck?.healthy ? 'Saludable' : 'Con problemas',
          color: system.lastHealthCheck?.healthy ? 'green' : 'red'
        }
      ],
      details: {
        environment: process.env.NODE_ENV || 'development',
        uptime: system.uptime,
        startTime: system.startTime,
        memoryDetails: memUsage,
        gcpConfig: gcpStatus
      }
    };
  }

  /**
   * Construye sección de alertas
   */
  buildAlertsSection(alertStatus) {
    return {
      title: 'Sistema de Alertas',
      cards: [
        {
          title: 'Estado',
          value: alertStatus?.enabled ? 'Activo' : 'Inactivo',
          color: alertStatus?.enabled ? 'green' : 'gray'
        },
        {
          title: 'Webhook',
          value: alertStatus?.webhookConfigured ? 'Configurado' : 'No configurado',
          color: alertStatus?.webhookConfigured ? 'green' : 'orange'
        },
        {
          title: 'Email',
          value: alertStatus?.emailEnabled ? 'Activo' : 'Inactivo',
          color: alertStatus?.emailEnabled ? 'green' : 'gray'
        },
        {
          title: 'Cooldowns Activos',
          value: alertStatus?.activeCooldowns?.length || 0,
          subtitle: 'Alertas en espera',
          color: 'blue'
        }
      ],
      details: {
        thresholds: alertStatus?.thresholds,
        activeCooldowns: alertStatus?.activeCooldowns,
        cooldownPeriod: alertStatus?.cooldownPeriod
      }
    };
  }

  /**
   * Construye sección de limpieza
   */
  buildCleanupSection(cleanupStats) {
    return {
      title: 'Limpieza Automática',
      cards: [
        {
          title: 'Estado',
          value: cleanupStats?.configuration?.enabled ? 'Activa' : 'Inactiva',
          color: cleanupStats?.configuration?.enabled ? 'green' : 'gray'
        },
        {
          title: 'Archivos Eliminados',
          value: this.formatNumber(cleanupStats?.totalFilesDeleted || 0),
          subtitle: 'Total acumulado',
          color: 'blue'
        },
        {
          title: 'Espacio Liberado',
          value: this.formatBytes(cleanupStats?.totalSpaceFreed || 0),
          subtitle: 'Total acumulado',
          color: 'purple'
        },
        {
          title: 'Última Ejecución',
          value: this.formatTimeAgo(cleanupStats?.lastRun),
          subtitle: cleanupStats?.errors ? `${cleanupStats.errors} errores` : 'Sin errores',
          color: cleanupStats?.errors > 0 ? 'orange' : 'green'
        }
      ],
      details: {
        configuration: cleanupStats?.configuration,
        lastError: cleanupStats?.lastError
      }
    };
  }

  /**
   * Obtiene estado de salud general
   */
  async getHealthStatus() {
    try {
      const metrics = await this.metricsCollector.getMetrics();
      const gcpStatus = gcpConfig.getStatus();
      
      const checks = [
        {
          name: 'GCP Credentials',
          status: gcpStatus.credentialsValid ? 'healthy' : 'unhealthy',
          message: gcpStatus.credentialsMessage
        },
        {
          name: 'Redis Connection',
          status: metrics.redis?.lastConnection ? 'healthy' : 'unknown',
          message: metrics.redis?.lastError?.message || 'OK'
        },
        {
          name: 'Processing',
          status: metrics.processing?.lastProcessing ? 'healthy' : 'unknown',
          message: metrics.processing?.lastError?.message || 'OK'
        },
        {
          name: 'Memory Usage',
          status: this.getMemoryStatus(metrics.system?.memoryUsage),
          message: `${metrics.system?.memoryUsage?.heapUsed || 0} MB used`
        }
      ];

      const healthyChecks = checks.filter(check => check.status === 'healthy').length;
      const overallHealth = healthyChecks === checks.length ? 'healthy' : 
                           healthyChecks > checks.length / 2 ? 'degraded' : 'unhealthy';

      return {
        overall: overallHealth,
        checks,
        score: Math.round((healthyChecks / checks.length) * 100),
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      return {
        overall: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Calcula tendencia
   */
  calculateTrend(current, target) {
    const diff = current - target;
    if (Math.abs(diff) < 1) return 'stable';
    return diff > 0 ? 'up' : 'down';
  }

  /**
   * Obtiene color basado en uso de memoria
   */
  getMemoryColor(used, total) {
    if (!used || !total) return 'gray';
    const percentage = (used / total) * 100;
    if (percentage > 80) return 'red';
    if (percentage > 60) return 'orange';
    return 'green';
  }

  /**
   * Obtiene estado de memoria
   */
  getMemoryStatus(memUsage) {
    if (!memUsage) return 'unknown';
    const percentage = (memUsage.heapUsed / memUsage.heapTotal) * 100;
    if (percentage > 80) return 'unhealthy';
    if (percentage > 60) return 'degraded';
    return 'healthy';
  }

  /**
   * Formatea número
   */
  formatNumber(num) {
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  }

  /**
   * Formatea bytes
   */
  formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  /**
   * Formatea tiempo relativo
   */
  formatTimeAgo(timestamp) {
    if (!timestamp) return 'Nunca';
    
    const now = new Date();
    const time = new Date(timestamp);
    const diffMs = now - time;
    const diffMins = Math.floor(diffMs / 60000);
    
    if (diffMins < 1) return 'Ahora';
    if (diffMins < 60) return `${diffMins}m`;
    
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h`;
    
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d`;
  }
}