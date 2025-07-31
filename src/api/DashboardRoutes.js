import { DashboardController } from './DashboardController.js';
import { logger } from '../utils/logger.js';

/**
 * Rutas para el dashboard de métricas
 */
export class DashboardRoutes {
  constructor() {
    this.controller = new DashboardController();
  }

  /**
   * Configura las rutas del dashboard
   */
  setupRoutes(app) {
    // Dashboard principal (HTML)
    app.get('/dashboard', this.serveDashboardHTML.bind(this));
    
    // API endpoints para datos del dashboard
    app.get('/api/dashboard/data', this.getDashboardData.bind(this));
    app.get('/api/dashboard/gcs', this.getGCSMetrics.bind(this));
    app.get('/api/dashboard/bigquery', this.getBigQueryMetrics.bind(this));
    app.get('/api/dashboard/costs', this.getCostReport.bind(this));
    app.get('/api/dashboard/health', this.getHealthStatus.bind(this));
    app.get('/api/dashboard/alerts', this.getAlertsStatus.bind(this));
    app.get('/api/dashboard/cleanup', this.getCleanupStats.bind(this));
    app.get('/api/dashboard/backup', this.getBackupMetrics.bind(this));
    
    // Endpoints de acción
    app.post('/api/dashboard/cleanup/run', this.runCleanup.bind(this));
    app.post('/api/dashboard/cleanup/intelligent', this.runIntelligentCleanup.bind(this));
    app.post('/api/dashboard/cleanup/emergency', this.runEmergencyCleanup.bind(this));
    app.post('/api/dashboard/metrics/reset', this.resetMetrics.bind(this));
    app.post('/api/dashboard/alerts/test', this.testAlert.bind(this));
    app.post('/api/dashboard/costs/optimize', this.optimizeCosts.bind(this));
    
    // Endpoints de reportes
    app.get('/api/dashboard/reports/cost-optimization', this.getCostOptimizationReport.bind(this));
    app.get('/api/dashboard/reports/usage-patterns', this.getUsagePatternsReport.bind(this));
    app.get('/api/dashboard/export/metrics', this.exportMetrics.bind(this));

    logger.info('📊 Rutas del dashboard configuradas');
  }

  /**
   * Sirve la página HTML del dashboard
   */
  async serveDashboardHTML(req, res) {
    try {
      const html = this.generateDashboardHTML();
      res.setHeader('Content-Type', 'text/html');
      res.send(html);
    } catch (error) {
      logger.error('❌ Error sirviendo dashboard HTML:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene datos completos del dashboard
   */
  async getDashboardData(req, res) {
    try {
      const data = await this.controller.getDashboardData();
      res.json(data);
    } catch (error) {
      logger.error('❌ Error obteniendo datos del dashboard:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene métricas específicas de GCS
   */
  async getGCSMetrics(req, res) {
    try {
      const metrics = await this.controller.metricsCollector.getGCSMetrics();
      res.json(metrics);
    } catch (error) {
      logger.error('❌ Error obteniendo métricas GCS:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene métricas específicas de BigQuery
   */
  async getBigQueryMetrics(req, res) {
    try {
      const metrics = await this.controller.metricsCollector.getBigQueryMetrics();
      res.json(metrics);
    } catch (error) {
      logger.error('❌ Error obteniendo métricas BigQuery:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene reporte de costos
   */
  async getCostReport(req, res) {
    try {
      const report = await this.controller.costMonitor.getCostReport();
      res.json(report);
    } catch (error) {
      logger.error('❌ Error obteniendo reporte de costos:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene estado de salud
   */
  async getHealthStatus(req, res) {
    try {
      const health = await this.controller.getHealthStatus();
      res.json(health);
    } catch (error) {
      logger.error('❌ Error obteniendo estado de salud:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene estado de alertas
   */
  async getAlertsStatus(req, res) {
    try {
      const status = this.controller.alertManager.getStatus();
      res.json(status);
    } catch (error) {
      logger.error('❌ Error obteniendo estado de alertas:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene estadísticas de limpieza
   */
  async getCleanupStats(req, res) {
    try {
      const stats = this.controller.autoCleanup.getStats();
      const estimate = await this.controller.autoCleanup.getCleanupEstimate();
      res.json({ ...stats, estimate });
    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas de limpieza:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Ejecuta limpieza manual
   */
  async runCleanup(req, res) {
    try {
      const result = await this.controller.autoCleanup.runFullCleanup();
      res.json({
        success: true,
        message: 'Limpieza ejecutada exitosamente',
        result
      });
    } catch (error) {
      logger.error('❌ Error ejecutando limpieza:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Resetea métricas
   */
  async resetMetrics(req, res) {
    try {
      await this.controller.metricsCollector.resetMetrics();
      res.json({
        success: true,
        message: 'Métricas reseteadas exitosamente'
      });
    } catch (error) {
      logger.error('❌ Error reseteando métricas:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Prueba el sistema de alertas
   */
  async testAlert(req, res) {
    try {
      await this.controller.alertManager.alertGCSFailure(
        new Error('Prueba de alerta desde dashboard'),
        'test_operation',
        'test',
        0
      );
      
      res.json({
        success: true,
        message: 'Alerta de prueba enviada'
      });
    } catch (error) {
      logger.error('❌ Error enviando alerta de prueba:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Ejecuta limpieza inteligente
   */
  async runIntelligentCleanup(req, res) {
    try {
      const result = await this.controller.autoCleanup.runIntelligentCleanup();
      res.json({
        success: true,
        message: 'Limpieza inteligente ejecutada exitosamente',
        result
      });
    } catch (error) {
      logger.error('❌ Error ejecutando limpieza inteligente:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Ejecuta limpieza de emergencia
   */
  async runEmergencyCleanup(req, res) {
    try {
      const result = await this.controller.autoCleanup.runEmergencyCleanup();
      res.json({
        success: true,
        message: 'Limpieza de emergencia ejecutada exitosamente',
        result
      });
    } catch (error) {
      logger.error('❌ Error ejecutando limpieza de emergencia:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Optimiza costos
   */
  async optimizeCosts(req, res) {
    try {
      const result = await this.controller.costMonitor.optimizeCosts();
      res.json({
        success: true,
        message: 'Optimización de costos ejecutada exitosamente',
        result
      });
    } catch (error) {
      logger.error('❌ Error optimizando costos:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene reporte de optimización de costos
   */
  async getCostOptimizationReport(req, res) {
    try {
      const report = await this.controller.costMonitor.generateOptimizationReport();
      res.json(report);
    } catch (error) {
      logger.error('❌ Error obteniendo reporte de optimización:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene reporte de patrones de uso
   */
  async getUsagePatternsReport(req, res) {
    try {
      const patterns = await this.controller.autoCleanup.analyzeUsagePatterns();
      res.json({
        timestamp: new Date().toISOString(),
        patterns
      });
    } catch (error) {
      logger.error('❌ Error obteniendo patrones de uso:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Obtiene métricas específicas de backup
   */
  async getBackupMetrics(req, res) {
    try {
      const healthController = new (await import('./HealthController.js')).HealthController();
      const metrics = await healthController.getBackupMetrics();
      res.json(metrics);
    } catch (error) {
      logger.error('❌ Error obteniendo métricas de backup:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Exporta métricas
   */
  async exportMetrics(req, res) {
    try {
      const format = req.query.format || 'json';
      const data = await this.controller.getDashboardData();
      
      if (format === 'csv') {
        const csv = this.convertToCSV(data);
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', 'attachment; filename=metrics.csv');
        res.send(csv);
      } else {
        res.setHeader('Content-Type', 'application/json');
        res.setHeader('Content-Disposition', 'attachment; filename=metrics.json');
        res.json(data);
      }
    } catch (error) {
      logger.error('❌ Error exportando métricas:', error.message);
      res.status(500).json({ error: error.message });
    }
  }

  /**
   * Convierte datos a CSV
   */
  convertToCSV(data) {
    const rows = [];
    
    // Header
    rows.push('Timestamp,Service,Metric,Value,Unit');
    
    // Overview metrics
    if (data.overview?.cards) {
      data.overview.cards.forEach(card => {
        rows.push(`${data.timestamp},Overview,${card.title},${card.value},${card.subtitle || ''}`);
      });
    }
    
    // GCS metrics
    if (data.gcs?.cards) {
      data.gcs.cards.forEach(card => {
        rows.push(`${data.timestamp},GCS,${card.title},${card.value},${card.subtitle || ''}`);
      });
    }
    
    // BigQuery metrics
    if (data.bigQuery?.cards) {
      data.bigQuery.cards.forEach(card => {
        rows.push(`${data.timestamp},BigQuery,${card.title},${card.value},${card.subtitle || ''}`);
      });
    }
    
    // Cost metrics
    if (data.costs?.cards) {
      data.costs.cards.forEach(card => {
        rows.push(`${data.timestamp},Costs,${card.title},${card.value},${card.subtitle || ''}`);
      });
    }
    
    return rows.join('\n');
  }

  /**
   * Genera HTML del dashboard
   */
  generateDashboardHTML() {
    return `
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GPS BigQuery Service - Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f5f5f5;
            color: #333;
        }
        
        .header {
            background: #1a73e8;
            color: white;
            padding: 1rem 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .header h1 {
            font-size: 1.5rem;
            font-weight: 500;
        }
        
        .refresh-info {
            font-size: 0.9rem;
            opacity: 0.9;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        .section {
            background: white;
            border-radius: 8px;
            padding: 1.5rem;
            margin-bottom: 2rem;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        
        .section-title {
            font-size: 1.2rem;
            font-weight: 600;
            margin-bottom: 1rem;
            color: #1a73e8;
        }
        
        .cards-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 1rem;
            margin-bottom: 1.5rem;
        }
        
        .card {
            background: #f8f9fa;
            border-radius: 6px;
            padding: 1rem;
            border-left: 4px solid #ddd;
        }
        
        .card.green { border-left-color: #34a853; }
        .card.red { border-left-color: #ea4335; }
        .card.blue { border-left-color: #1a73e8; }
        .card.orange { border-left-color: #ff9800; }
        .card.purple { border-left-color: #9c27b0; }
        .card.gray { border-left-color: #666; }
        
        .card-title {
            font-size: 0.9rem;
            color: #666;
            margin-bottom: 0.5rem;
        }
        
        .card-value {
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }
        
        .card-subtitle {
            font-size: 0.8rem;
            color: #888;
        }
        
        .details {
            background: #f8f9fa;
            border-radius: 6px;
            padding: 1rem;
            margin-top: 1rem;
        }
        
        .details-title {
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        
        .details-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
        }
        
        .detail-item {
            background: white;
            padding: 0.75rem;
            border-radius: 4px;
        }
        
        .detail-label {
            font-size: 0.8rem;
            color: #666;
            margin-bottom: 0.25rem;
        }
        
        .detail-value {
            font-weight: 500;
        }
        
        .actions {
            display: flex;
            gap: 1rem;
            margin-top: 1rem;
        }
        
        .btn {
            background: #1a73e8;
            color: white;
            border: none;
            padding: 0.5rem 1rem;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9rem;
        }
        
        .btn:hover {
            background: #1557b0;
        }
        
        .btn.secondary {
            background: #666;
        }
        
        .btn.danger {
            background: #ea4335;
        }
        
        .loading {
            text-align: center;
            padding: 2rem;
            color: #666;
        }
        
        .error {
            background: #ffeaea;
            color: #d93025;
            padding: 1rem;
            border-radius: 4px;
            margin: 1rem 0;
        }
        
        .health-indicator {
            display: inline-block;
            width: 12px;
            height: 12px;
            border-radius: 50%;
            margin-right: 0.5rem;
        }
        
        .health-healthy { background: #34a853; }
        .health-degraded { background: #ff9800; }
        .health-unhealthy { background: #ea4335; }
        
        @media (max-width: 768px) {
            .container {
                padding: 1rem;
            }
            
            .cards-grid {
                grid-template-columns: 1fr;
            }
            
            .actions {
                flex-direction: column;
            }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚀 GPS BigQuery Service Dashboard</h1>
        <div class="refresh-info">
            <span id="lastUpdate">Cargando...</span> | 
            <span id="refreshTimer">--</span>
        </div>
    </div>
    
    <div class="container">
        <div id="loading" class="loading">
            <div>📊 Cargando datos del dashboard...</div>
        </div>
        
        <div id="error" class="error" style="display: none;"></div>
        
        <div id="dashboard" style="display: none;">
            <!-- El contenido se generará dinámicamente -->
        </div>
    </div>

    <script>
        let refreshInterval;
        let refreshTimer;
        let nextRefresh = 30;
        
        // Cargar datos iniciales
        loadDashboard();
        
        // Configurar auto-refresh
        function startAutoRefresh(intervalSeconds) {
            if (refreshInterval) clearInterval(refreshInterval);
            if (refreshTimer) clearInterval(refreshTimer);
            
            nextRefresh = intervalSeconds;
            
            refreshInterval = setInterval(loadDashboard, intervalSeconds * 1000);
            refreshTimer = setInterval(updateTimer, 1000);
        }
        
        function updateTimer() {
            nextRefresh--;
            document.getElementById('refreshTimer').textContent = 
                nextRefresh > 0 ? \`Próxima actualización: \${nextRefresh}s\` : 'Actualizando...';
            
            if (nextRefresh <= 0) {
                nextRefresh = 30; // Reset
            }
        }
        
        async function loadDashboard() {
            try {
                const response = await fetch('/api/dashboard/data');
                const data = await response.json();
                
                if (data.error) {
                    showError(data.error);
                    return;
                }
                
                renderDashboard(data);
                document.getElementById('lastUpdate').textContent = 
                    \`Última actualización: \${new Date().toLocaleTimeString()}\`;
                
                // Iniciar auto-refresh si no está iniciado
                if (!refreshInterval) {
                    startAutoRefresh(data.refreshInterval || 30);
                }
                
            } catch (error) {
                showError('Error cargando datos: ' + error.message);
            }
        }
        
        function showError(message) {
            document.getElementById('loading').style.display = 'none';
            document.getElementById('dashboard').style.display = 'none';
            document.getElementById('error').style.display = 'block';
            document.getElementById('error').textContent = message;
        }
        
        function renderDashboard(data) {
            document.getElementById('loading').style.display = 'none';
            document.getElementById('error').style.display = 'none';
            document.getElementById('dashboard').style.display = 'block';
            
            const dashboard = document.getElementById('dashboard');
            dashboard.innerHTML = \`
                \${renderSection('📊 Resumen General', data.overview)}
                \${renderSection('☁️ Google Cloud Storage', data.gcs)}
                \${renderSection('📈 BigQuery', data.bigQuery)}
                \${renderSection('💰 Costos GCP', data.costs)}
                \${renderSection('🖥️ Sistema', data.system)}
                \${renderSection('🚨 Alertas', data.alerts)}
                \${renderSection('🧹 Limpieza', data.cleanup)}
                \${renderHealthSection(data.health)}
                \${renderActionsSection()}
            \`;
        }
        
        function renderSection(title, section) {
            if (!section || section.error) {
                return \`
                    <div class="section">
                        <div class="section-title">\${title}</div>
                        <div class="error">Error: \${section?.error || 'Datos no disponibles'}</div>
                    </div>
                \`;
            }
            
            const cards = section.cards?.map(card => \`
                <div class="card \${card.color || 'gray'}">
                    <div class="card-title">\${card.title}</div>
                    <div class="card-value">\${card.value}</div>
                    \${card.subtitle ? \`<div class="card-subtitle">\${card.subtitle}</div>\` : ''}
                </div>
            \`).join('') || '';
            
            return \`
                <div class="section">
                    <div class="section-title">\${title}</div>
                    <div class="cards-grid">
                        \${cards}
                    </div>
                </div>
            \`;
        }
        
        function renderHealthSection(health) {
            if (!health) return '';
            
            const checks = health.checks?.map(check => \`
                <div class="detail-item">
                    <div class="detail-label">
                        <span class="health-indicator health-\${check.status}"></span>
                        \${check.name}
                    </div>
                    <div class="detail-value">\${check.message}</div>
                </div>
            \`).join('') || '';
            
            return \`
                <div class="section">
                    <div class="section-title">
                        <span class="health-indicator health-\${health.overall}"></span>
                        🏥 Estado de Salud (\${health.score || 0}%)
                    </div>
                    <div class="details-grid">
                        \${checks}
                    </div>
                </div>
            \`;
        }
        
        function renderActionsSection() {
            return \`
                <div class="section">
                    <div class="section-title">⚡ Acciones</div>
                    <div class="actions">
                        <button class="btn" onclick="runCleanup()">🧹 Limpieza Estándar</button>
                        <button class="btn" onclick="runIntelligentCleanup()">🧠 Limpieza Inteligente</button>
                        <button class="btn danger" onclick="runEmergencyCleanup()">🚨 Limpieza Emergencia</button>
                        <button class="btn" onclick="optimizeCosts()">💰 Optimizar Costos</button>
                        <button class="btn secondary" onclick="resetMetrics()">🔄 Resetear Métricas</button>
                        <button class="btn secondary" onclick="testAlert()">🚨 Probar Alerta</button>
                        <button class="btn secondary" onclick="exportMetrics()">📊 Exportar Métricas</button>
                        <button class="btn" onclick="loadDashboard()">🔄 Actualizar</button>
                    </div>
                </div>
            \`;
        }
        
        async function runCleanup() {
            if (!confirm('¿Ejecutar limpieza automática?')) return;
            
            try {
                const response = await fetch('/api/dashboard/cleanup/run', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Limpieza ejecutada exitosamente');
                    loadDashboard();
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error ejecutando limpieza: ' + error.message);
            }
        }
        
        async function resetMetrics() {
            if (!confirm('¿Resetear todas las métricas?')) return;
            
            try {
                const response = await fetch('/api/dashboard/metrics/reset', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Métricas reseteadas exitosamente');
                    loadDashboard();
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error reseteando métricas: ' + error.message);
            }
        }
        
        async function testAlert() {
            try {
                const response = await fetch('/api/dashboard/alerts/test', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Alerta de prueba enviada');
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error enviando alerta: ' + error.message);
            }
        }
        
        async function runIntelligentCleanup() {
            if (!confirm('¿Ejecutar limpieza inteligente? Esta analizará patrones de uso para optimizar la limpieza.')) return;
            
            try {
                const response = await fetch('/api/dashboard/cleanup/intelligent', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Limpieza inteligente ejecutada exitosamente');
                    loadDashboard();
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error ejecutando limpieza inteligente: ' + error.message);
            }
        }
        
        async function runEmergencyCleanup() {
            if (!confirm('⚠️ ¿Ejecutar limpieza de emergencia? Esta eliminará archivos más agresivamente.')) return;
            
            try {
                const response = await fetch('/api/dashboard/cleanup/emergency', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Limpieza de emergencia ejecutada exitosamente');
                    loadDashboard();
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error ejecutando limpieza de emergencia: ' + error.message);
            }
        }
        
        async function optimizeCosts() {
            if (!confirm('¿Ejecutar optimización de costos? Esta analizará y aplicará optimizaciones automáticas.')) return;
            
            try {
                const response = await fetch('/api/dashboard/costs/optimize', { method: 'POST' });
                const result = await response.json();
                
                if (result.success) {
                    alert(\`✅ Optimización completada. Ahorros estimados: $\${result.result.estimatedSavings || 0}\`);
                    loadDashboard();
                } else {
                    alert('❌ Error: ' + result.error);
                }
            } catch (error) {
                alert('❌ Error optimizando costos: ' + error.message);
            }
        }
        
        async function exportMetrics() {
            try {
                const format = prompt('Formato de exportación (json/csv):', 'json');
                if (!format) return;
                
                const response = await fetch(\`/api/dashboard/export/metrics?format=\${format}\`);
                
                if (response.ok) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = \`metrics.\${format}\`;
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                    window.URL.revokeObjectURL(url);
                    
                    alert('✅ Métricas exportadas exitosamente');
                } else {
                    alert('❌ Error exportando métricas');
                }
            } catch (error) {
                alert('❌ Error exportando métricas: ' + error.message);
            }
        }
    </script>
</body>
</html>
    `;
  }
}