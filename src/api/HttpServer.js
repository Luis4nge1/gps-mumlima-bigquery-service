import http from 'http';
import url from 'url';
import { logger } from '../utils/logger.js';
import { HealthController } from './HealthController.js';
import { DashboardRoutes } from './DashboardRoutes.js';
import { AlertManager } from '../utils/AlertManager.js';
import { CostMonitor } from '../utils/CostMonitor.js';
import { AutoCleanup } from '../utils/AutoCleanup.js';
import { config } from '../config/env.js';

/**
 * Servidor HTTP simple para health checks y API básica
 */
export class HttpServer {
  constructor() {
    this.server = null;
    this.healthController = new HealthController();
    this.dashboardRoutes = new DashboardRoutes();
    this.alertManager = new AlertManager();
    this.costMonitor = new CostMonitor();
    this.autoCleanup = new AutoCleanup();
    this.port = config.server.port;
    this.isRunning = false;
    
    // Configuración de API
    this.apiConfig = {
      baseUrl: process.env.API_BASE_URL || `http://${config.server.host}:${config.server.port}`,
      version: process.env.API_VERSION || 'v3',
      basePath: process.env.API_BASE_PATH || '/api/v3'
    };
    
    // Inicializar monitoreo de producción
    this.initializeProductionMonitoring();
  }

  /**
   * Inicia el servidor HTTP
   */
  start() {
    if (this.isRunning) {
      logger.warn('⚠️ Servidor HTTP ya está ejecutándose');
      return false;
    }

    this.server = http.createServer(this.handleRequest.bind(this));

    this.server.listen(this.port, () => {
      logger.info(`🌐 Servidor HTTP iniciado en puerto ${this.port}`);
      logger.info(`🔗 API Base URL: ${this.apiConfig.baseUrl}${this.apiConfig.basePath}`);
      logger.info(`📊 Health check: ${this.apiConfig.baseUrl}${this.apiConfig.basePath}/health`);
      logger.info(`📈 Métricas: ${this.apiConfig.baseUrl}${this.apiConfig.basePath}/metrics`);
      logger.info(`📋 Documentación: ${this.apiConfig.baseUrl}${this.apiConfig.basePath}`);
      this.isRunning = true;
    });

    this.server.on('error', (error) => {
      logger.error('❌ Error en servidor HTTP:', error.message);
      this.isRunning = false;
    });

    return true;
  }

  /**
   * Normaliza la ruta para usar el prefijo de API configurado
   */
  normalizePath(originalPath) {
    // Si ya tiene el prefijo correcto, devolverlo tal como está
    if (originalPath.startsWith(this.apiConfig.basePath)) {
      return originalPath;
    }
    
    // Si tiene el prefijo legacy, convertirlo al nuevo
    if (originalPath.startsWith('/api/massive-data/')) {
      return originalPath.replace('/api/massive-data/', `${this.apiConfig.basePath}/`);
    }
    
    // Si tiene el prefijo de dashboard, mantenerlo
    if (originalPath.startsWith('/api/dashboard/') || originalPath === '/dashboard') {
      return originalPath;
    }
    
    // Si tiene el prefijo de monitoring, mantenerlo
    if (originalPath.startsWith('/api/monitoring/')) {
      return originalPath;
    }
    
    // Para rutas raíz, agregar el prefijo
    if (originalPath === '/api/massive-data' || originalPath === '/') {
      return this.apiConfig.basePath;
    }
    
    return originalPath;
  }

  /**
   * Maneja las peticiones HTTP
   */
  async handleRequest(req, res) {
    const startTime = Date.now();
    const parsedUrl = url.parse(req.url, true);
    const originalPath = parsedUrl.pathname;
    const path = this.normalizePath(originalPath);
    const method = req.method;

    // Log de petición
    logger.debug(`📡 ${method} ${originalPath} → ${path} - ${req.headers['user-agent'] || 'Unknown'}`);

    // Configurar CORS y headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('X-API-Version', this.apiConfig.version);
    res.setHeader('X-Response-Time', `${Date.now() - startTime}ms`);

    try {
      // Manejar OPTIONS para CORS
      if (method === 'OPTIONS') {
        res.writeHead(200);
        res.end();
        return;
      }

      // Dashboard routes
      if (path === '/dashboard' || path.startsWith('/api/dashboard/')) {
        await this.handleDashboardRoute(req, res, path, method);
        return;
      }

      // Production monitoring routes
      if (path.startsWith('/api/monitoring/')) {
        await this.handleMonitoringRoute(req, res, path, method);
        return;
      }

      // Rutas disponibles con prefijo /api/v3/
      switch (path) {
        // Health endpoints
        case `${this.apiConfig.basePath}/health`:
          await this.handleHealth(req, res);
          break;

        case `${this.apiConfig.basePath}/health/detailed`:
          await this.handleDetailedHealth(req, res);
          break;

        case `${this.apiConfig.basePath}/health/gcs`:
          await this.handleGCSHealth(req, res);
          break;

        case `${this.apiConfig.basePath}/health/bigquery`:
          await this.handleBigQueryHealth(req, res);
          break;

        // Metrics endpoints
        case `${this.apiConfig.basePath}/metrics`:
          await this.handleMetrics(req, res);
          break;

        case `${this.apiConfig.basePath}/metrics/gcs`:
          await this.handleGCSMetrics(req, res);
          break;

        case `${this.apiConfig.basePath}/metrics/bigquery`:
          await this.handleBigQueryMetrics(req, res);
          break;

        case `${this.apiConfig.basePath}/metrics/costs`:
          await this.handleCostMetrics(req, res);
          break;

        // Status and processing endpoints
        case `${this.apiConfig.basePath}/status`:
          await this.handleStatus(req, res);
          break;

        case `${this.apiConfig.basePath}/process`:
          if (method === 'POST') {
            await this.handleManualProcess(req, res);
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;
        
        // Recovery endpoints
        case `${this.apiConfig.basePath}/recovery`:
          if (method === 'POST') {
            await this.handleRecovery(req, res);
          } else if (method === 'GET') {
            await this.handleRecoveryStatus(req, res);
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        case `${this.apiConfig.basePath}/recovery/gcs-status`:
          await this.handleGCSRecoveryStatus(req, res);
          break;

        // GCS endpoints
        case `${this.apiConfig.basePath}/gcs/files`:
          await this.handleGCSFileStats(req, res);
          break;

        // Batch processing endpoints
        case `${this.apiConfig.basePath}/batch-process`:
          if (method === 'POST') {
            await this.handleManualBatchProcess(req, res);
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        // Hybrid/Migration endpoints
        case `${this.apiConfig.basePath}/hybrid/status`:
          await this.handleHybridStatus(req, res);
          break;

        case `${this.apiConfig.basePath}/hybrid/metrics`:
          await this.handleHybridMetrics(req, res);
          break;

        case `${this.apiConfig.basePath}/hybrid/rollbacks`:
          await this.handleHybridRollbacks(req, res);
          break;

        case `${this.apiConfig.basePath}/hybrid/comparisons`:
          await this.handleHybridComparisons(req, res);
          break;

        case `${this.apiConfig.basePath}/migration/phase`:
          if (method === 'POST') {
            await this.handleMigrationPhaseChange(req, res);
          } else {
            await this.handleMigrationPhaseGet(req, res);
          }
          break;

        // Root endpoint
        case this.apiConfig.basePath:
          this.handleRoot(req, res);
          break;

        default:
          this.sendError(res, 404, 'Endpoint not found');
      }

    } catch (error) {
      logger.error('❌ Error procesando petición:', error.message);
      this.sendError(res, 500, 'Internal server error', error.message);
    }
  }

  /**
   * Health check básico
   */
  async handleHealth(req, res) {
    const health = await this.healthController.basicHealth();
    const statusCode = health.status === 'healthy' ? 200 : 503;

    res.writeHead(statusCode);
    res.end(JSON.stringify(health, null, 2));
  }

  /**
   * Health check detallado
   */
  async handleDetailedHealth(req, res) {
    const health = await this.healthController.detailedHealth();
    const statusCode = health.status === 'healthy' ? 200 : 503;

    res.writeHead(statusCode);
    res.end(JSON.stringify(health, null, 2));
  }

  /**
   * Métricas del sistema
   */
  async handleMetrics(req, res) {
    const metrics = await this.healthController.getMetrics();

    res.writeHead(200);
    res.end(JSON.stringify(metrics, null, 2));
  }

  /**
   * Estado del procesador
   */
  async handleStatus(req, res) {
    const status = await this.healthController.getProcessorStatus();

    res.writeHead(200);
    res.end(JSON.stringify(status, null, 2));
  }

  /**
   * Procesamiento manual
   */
  async handleManualProcess(req, res) {
    const result = await this.healthController.triggerManualProcessing();
    const statusCode = result.success ? 200 : 500;

    res.writeHead(statusCode);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Ejecutar recovery manual
   */
  async handleRecovery(req, res) {
    const result = await this.healthController.triggerRecovery();
    const statusCode = result.success ? 200 : 500;
    
    res.writeHead(statusCode);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Estado del sistema de recovery
   */
  async handleRecoveryStatus(req, res) {
    const result = await this.healthController.getRecoveryStatus();
    
    res.writeHead(200);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Health check específico de GCS
   */
  async handleGCSHealth(req, res) {
    const health = await this.healthController.getGCSHealth();
    const statusCode = health.healthy ? 200 : 503;

    res.writeHead(statusCode);
    res.end(JSON.stringify(health, null, 2));
  }

  /**
   * Health check específico de BigQuery
   */
  async handleBigQueryHealth(req, res) {
    const health = await this.healthController.getBigQueryHealth();
    const statusCode = health.healthy ? 200 : 503;

    res.writeHead(statusCode);
    res.end(JSON.stringify(health, null, 2));
  }

  /**
   * Métricas específicas de GCS
   */
  async handleGCSMetrics(req, res) {
    const metrics = await this.healthController.metrics.getGCSMetrics();

    res.writeHead(200);
    res.end(JSON.stringify({
      success: true,
      timestamp: new Date().toISOString(),
      service: 'Google Cloud Storage',
      metrics
    }, null, 2));
  }

  /**
   * Métricas específicas de BigQuery
   */
  async handleBigQueryMetrics(req, res) {
    const metrics = await this.healthController.metrics.getBigQueryMetrics();

    res.writeHead(200);
    res.end(JSON.stringify({
      success: true,
      timestamp: new Date().toISOString(),
      service: 'BigQuery Batch Processor',
      metrics
    }, null, 2));
  }

  /**
   * Métricas de costos GCP
   */
  async handleCostMetrics(req, res) {
    const costs = await this.healthController.metrics.getCostMetrics();

    res.writeHead(200);
    res.end(JSON.stringify({
      success: true,
      timestamp: new Date().toISOString(),
      service: 'GCP Cost Tracking',
      costs
    }, null, 2));
  }

  /**
   * Estado del recovery GCS
   */
  async handleGCSRecoveryStatus(req, res) {
    const result = await this.healthController.getGCSRecoveryStatus();
    
    res.writeHead(200);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Estadísticas de archivos GCS
   */
  async handleGCSFileStats(req, res) {
    const result = await this.healthController.getGCSFileStats();
    
    res.writeHead(200);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Procesamiento batch manual
   */
  async handleManualBatchProcess(req, res) {
    const result = await this.healthController.triggerManualBatchProcessing();
    const statusCode = result.success ? 200 : 500;
    
    res.writeHead(statusCode);
    res.end(JSON.stringify(result, null, 2));
  }

  /**
   * Página de inicio con información de endpoints
   */
  handleRoot(req, res) {
    const basePath = this.apiConfig.basePath;
    const info = {
      service: 'GPS BigQuery Microservice',
      version: '2.0.0',
      apiVersion: this.apiConfig.version,
      status: 'running',
      timestamp: new Date().toISOString(),
      baseUrl: this.apiConfig.baseUrl,
      basePath: basePath,
      endpoints: {
        // Health Checks
        health: `${basePath}/health - Health check básico`,
        detailedHealth: `${basePath}/health/detailed - Health check detallado`,
        gcsHealth: `${basePath}/health/gcs - Health check específico GCS`,
        bigQueryHealth: `${basePath}/health/bigquery - Health check específico BigQuery`,
        
        // Metrics
        metrics: `${basePath}/metrics - Métricas del sistema`,
        gcsMetrics: `${basePath}/metrics/gcs - Métricas específicas GCS`,
        bigQueryMetrics: `${basePath}/metrics/bigquery - Métricas específicas BigQuery`,
        costMetrics: `${basePath}/metrics/costs - Métricas de costos GCP`,
        
        // Status and Processing
        status: `${basePath}/status - Estado del procesador`,
        manualProcess: `POST ${basePath}/process - Ejecutar procesamiento manual`,
        batchProcess: `POST ${basePath}/batch-process - Ejecutar procesamiento batch manual`,
        
        // Recovery
        recovery: `POST ${basePath}/recovery - Ejecutar recovery de backups`,
        recoveryStatus: `GET ${basePath}/recovery - Estado del sistema de recovery`,
        gcsRecoveryStatus: `${basePath}/recovery/gcs-status - Estado recovery GCS específico`,
        
        // GCS File Management
        gcsFileStats: `${basePath}/gcs/files - Estadísticas de archivos GCS`,
        
        // Hybrid/Migration Endpoints
        hybridStatus: `${basePath}/hybrid/status - Estado del sistema híbrido`,
        hybridMetrics: `${basePath}/hybrid/metrics - Métricas del sistema híbrido`,
        hybridRollbacks: `${basePath}/hybrid/rollbacks - Historial de rollbacks`,
        hybridComparisons: `${basePath}/hybrid/comparisons - Comparaciones entre flujos`,
        migrationPhase: `GET/POST ${basePath}/migration/phase - Gestión de fase de migración`,
        
        // Production Dashboard (mantiene rutas originales)
        dashboard: '/dashboard - Dashboard web de métricas y monitoreo',
        dashboardData: '/api/dashboard/data - Datos completos del dashboard',
        
        // Production Monitoring (mantiene rutas originales)
        alertsStatus: '/api/monitoring/alerts/status - Estado del sistema de alertas',
        costReport: '/api/monitoring/costs/report - Reporte de costos GCP',
        cleanupStats: '/api/monitoring/cleanup/stats - Estadísticas de limpieza automática',
        runCleanup: 'POST /api/monitoring/cleanup/run - Ejecutar limpieza manual',
        testAlert: 'POST /api/monitoring/alerts/test - Enviar alerta de prueba'
      },
      features: {
        apiVersioning: `API versionada con prefijo ${basePath}`,
        backwardCompatibility: 'Compatibilidad con rutas legacy /api/massive-data/',
        gcsIntegration: 'Google Cloud Storage para almacenamiento intermedio',
        bigQueryBatch: 'Procesamiento por lotes hacia BigQuery',
        recovery: 'Sistema de recovery para archivos GCS pendientes',
        metrics: 'Métricas detalladas de GCS, BigQuery y costos',
        healthChecks: 'Health checks específicos por servicio',
        hybridMigration: 'Sistema de migración gradual con rollback automático',
        productionDashboard: 'Dashboard web para monitoreo en tiempo real',
        alertSystem: 'Sistema de alertas para fallos de GCS y BigQuery',
        costMonitoring: 'Monitoreo de costos y uso de recursos GCP',
        autoCleanup: 'Limpieza automática de archivos antiguos'
      },
      configuration: {
        environment: config.server.environment,
        port: config.server.port,
        apiVersion: this.apiConfig.version,
        basePath: this.apiConfig.basePath,
        migrationEnabled: process.env.MIGRATION_ENABLED === 'true',
        simulationMode: process.env.GCP_SIMULATION_MODE === 'true'
      },
      documentation: 'Ver README.md para más información sobre el flujo GCS-BigQuery'
    };

    res.writeHead(200);
    res.end(JSON.stringify(info, null, 2));
  }

  /**
   * Maneja estado del sistema híbrido
   */
  async handleHybridStatus(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      const status = await this.healthController.getHybridStatus();
      res.writeHead(200);
      res.end(JSON.stringify(status, null, 2));
    } catch (error) {
      logger.error('❌ Error obteniendo estado híbrido:', error.message);
      this.sendError(res, 500, 'Error getting hybrid status', error.message);
    }
  }

  /**
   * Maneja métricas del sistema híbrido
   */
  async handleHybridMetrics(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      const metrics = await this.healthController.getHybridMetrics();
      res.writeHead(200);
      res.end(JSON.stringify(metrics, null, 2));
    } catch (error) {
      logger.error('❌ Error obteniendo métricas híbridas:', error.message);
      this.sendError(res, 500, 'Error getting hybrid metrics', error.message);
    }
  }

  /**
   * Maneja historial de rollbacks
   */
  async handleHybridRollbacks(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      const rollbacks = await this.healthController.getHybridRollbacks();
      res.writeHead(200);
      res.end(JSON.stringify(rollbacks, null, 2));
    } catch (error) {
      logger.error('❌ Error obteniendo rollbacks:', error.message);
      this.sendError(res, 500, 'Error getting rollbacks', error.message);
    }
  }

  /**
   * Maneja comparaciones entre flujos
   */
  async handleHybridComparisons(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      const comparisons = await this.healthController.getHybridComparisons();
      res.writeHead(200);
      res.end(JSON.stringify(comparisons, null, 2));
    } catch (error) {
      logger.error('❌ Error obteniendo comparaciones:', error.message);
      this.sendError(res, 500, 'Error getting comparisons', error.message);
    }
  }

  /**
   * Maneja cambio de fase de migración
   */
  async handleMigrationPhaseChange(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      let body = '';
      req.on('data', chunk => {
        body += chunk.toString();
      });

      req.on('end', async () => {
        try {
          const { phase } = JSON.parse(body);
          const result = await this.healthController.changeMigrationPhase(phase);
          
          res.writeHead(200);
          res.end(JSON.stringify(result, null, 2));
        } catch (parseError) {
          this.sendError(res, 400, 'Invalid JSON body', parseError.message);
        }
      });
    } catch (error) {
      logger.error('❌ Error cambiando fase de migración:', error.message);
      this.sendError(res, 500, 'Error changing migration phase', error.message);
    }
  }

  /**
   * Maneja obtención de fase de migración actual
   */
  async handleMigrationPhaseGet(req, res) {
    try {
      const migrationEnabled = process.env.MIGRATION_ENABLED === 'true';
      
      if (!migrationEnabled) {
        this.sendError(res, 404, 'Hybrid mode not enabled');
        return;
      }

      const phase = await this.healthController.getMigrationPhase();
      res.writeHead(200);
      res.end(JSON.stringify(phase, null, 2));
    } catch (error) {
      logger.error('❌ Error obteniendo fase de migración:', error.message);
      this.sendError(res, 500, 'Error getting migration phase', error.message);
    }
  }

  /**
   * Inicializa monitoreo de producción
   */
  initializeProductionMonitoring() {
    if (process.env.NODE_ENV !== 'production') return;

    // Programar limpieza automática
    this.autoCleanup.scheduleCleanup();

    // Programar monitoreo de alertas cada 5 minutos
    setInterval(() => {
      this.alertManager.monitorAndAlert().catch(error => {
        logger.error('❌ Error en monitoreo de alertas:', error.message);
      });
    }, 5 * 60 * 1000);

    // Programar monitoreo de costos cada hora
    setInterval(() => {
      this.costMonitor.monitorQuotas().catch(error => {
        logger.error('❌ Error en monitoreo de costos:', error.message);
      });
    }, 60 * 60 * 1000);

    // Generar reporte diario de costos a las 2 AM
    const now = new Date();
    const tomorrow2AM = new Date(now);
    tomorrow2AM.setDate(tomorrow2AM.getDate() + 1);
    tomorrow2AM.setHours(2, 0, 0, 0);
    
    const msUntil2AM = tomorrow2AM.getTime() - now.getTime();
    
    setTimeout(() => {
      this.costMonitor.generateDailyReport().catch(error => {
        logger.error('❌ Error generando reporte diario:', error.message);
      });
      
      // Repetir cada 24 horas
      setInterval(() => {
        this.costMonitor.generateDailyReport().catch(error => {
          logger.error('❌ Error generando reporte diario:', error.message);
        });
      }, 24 * 60 * 60 * 1000);
    }, msUntil2AM);

    logger.info('🔧 Monitoreo de producción inicializado');
  }

  /**
   * Maneja rutas del dashboard
   */
  async handleDashboardRoute(req, res, path, method) {
    try {
      // Simular Express-like app object
      const mockApp = {
        get: (route, handler) => {
          if (path === route && method === 'GET') {
            handler(req, res);
          }
        },
        post: (route, handler) => {
          if (path === route && method === 'POST') {
            handler(req, res);
          }
        }
      };

      // Configurar rutas temporalmente
      this.dashboardRoutes.setupRoutes(mockApp);

      // Si no se manejó la ruta, enviar 404
      if (!res.headersSent) {
        this.sendError(res, 404, 'Dashboard route not found');
      }

    } catch (error) {
      logger.error('❌ Error en ruta del dashboard:', error.message);
      this.sendError(res, 500, 'Dashboard error', error.message);
    }
  }

  /**
   * Maneja rutas de monitoreo
   */
  async handleMonitoringRoute(req, res, path, method) {
    try {
      switch (path) {
        case '/api/monitoring/alerts/status':
          if (method === 'GET') {
            const status = this.alertManager.getStatus();
            res.writeHead(200);
            res.end(JSON.stringify(status, null, 2));
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        case '/api/monitoring/costs/report':
          if (method === 'GET') {
            const report = await this.costMonitor.getCostReport();
            res.writeHead(200);
            res.end(JSON.stringify(report, null, 2));
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        case '/api/monitoring/cleanup/stats':
          if (method === 'GET') {
            const stats = this.autoCleanup.getStats();
            const estimate = await this.autoCleanup.getCleanupEstimate();
            res.writeHead(200);
            res.end(JSON.stringify({ ...stats, estimate }, null, 2));
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        case '/api/monitoring/cleanup/run':
          if (method === 'POST') {
            const result = await this.autoCleanup.runFullCleanup();
            res.writeHead(200);
            res.end(JSON.stringify({
              success: true,
              message: 'Limpieza ejecutada exitosamente',
              result
            }, null, 2));
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        case '/api/monitoring/alerts/test':
          if (method === 'POST') {
            await this.alertManager.alertGCSFailure(
              new Error('Prueba de alerta desde API'),
              'test_operation',
              'test',
              0
            );
            res.writeHead(200);
            res.end(JSON.stringify({
              success: true,
              message: 'Alerta de prueba enviada'
            }, null, 2));
          } else {
            this.sendError(res, 405, 'Method not allowed');
          }
          break;

        default:
          this.sendError(res, 404, 'Monitoring endpoint not found');
      }

    } catch (error) {
      logger.error('❌ Error en ruta de monitoreo:', error.message);
      this.sendError(res, 500, 'Monitoring error', error.message);
    }
  }

  /**
   * Envía respuesta de error
   */
  sendError(res, statusCode, message, details = null) {
    const error = {
      error: true,
      statusCode,
      message,
      timestamp: new Date().toISOString()
    };

    if (details) {
      error.details = details;
    }

    res.writeHead(statusCode);
    res.end(JSON.stringify(error, null, 2));
  }

  /**
   * Detiene el servidor
   */
  async stop() {
    if (!this.isRunning || !this.server) {
      return;
    }

    return new Promise((resolve) => {
      this.server.close(() => {
        logger.info('🔌 Servidor HTTP detenido');
        this.isRunning = false;
        resolve();
      });
    });
  }

  /**
   * Obtiene información de la configuración de la API
   */
  getApiInfo() {
    return {
      version: this.apiConfig.version,
      basePath: this.apiConfig.basePath,
      baseUrl: this.apiConfig.baseUrl,
      fullUrl: `${this.apiConfig.baseUrl}${this.apiConfig.basePath}`,
      environment: config.server.environment
    };
  }

  /**
   * Obtiene el estado del servidor
   */
  getStatus() {
    return {
      running: this.isRunning,
      port: this.port,
      uptime: this.isRunning ? process.uptime() : 0,
      api: this.getApiInfo()
    };
  }
}