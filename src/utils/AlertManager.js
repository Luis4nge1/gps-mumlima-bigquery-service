import { logger } from './logger.js';
import { config } from '../config/env.js';
import { MetricsCollector } from './MetricsCollector.js';

/**
 * Gestor de alertas para fallos de GCS y BigQuery
 */
export class AlertManager {
  constructor() {
    this.enabled = process.env.ALERTS_ENABLED === 'true';
    this.webhookUrl = process.env.ALERTS_WEBHOOK_URL;
    this.emailEnabled = process.env.ALERTS_EMAIL_ENABLED === 'true';
    this.emailConfig = {
      to: process.env.ALERTS_EMAIL_TO,
      smtp: {
        host: process.env.ALERTS_SMTP_HOST,
        port: parseInt(process.env.ALERTS_SMTP_PORT) || 587,
        user: process.env.ALERTS_SMTP_USER,
        pass: process.env.ALERTS_SMTP_PASS
      }
    };

    // Umbrales de alertas
    this.thresholds = {
      errorRate: parseFloat(process.env.ALERT_ERROR_RATE_THRESHOLD) || 5.0,
      processingTime: parseInt(process.env.ALERT_PROCESSING_TIME_THRESHOLD) || 300000,
      memoryUsage: parseInt(process.env.ALERT_MEMORY_USAGE_THRESHOLD) || 80,
      diskUsage: parseInt(process.env.ALERT_DISK_USAGE_THRESHOLD) || 85,
      gcsFailures: parseInt(process.env.ALERT_GCS_FAILURE_THRESHOLD) || 3,
      bigQueryFailures: parseInt(process.env.ALERT_BIGQUERY_FAILURE_THRESHOLD) || 3
    };

    // Control de frecuencia de alertas (evitar spam)
    this.alertCooldown = new Map();
    this.cooldownPeriod = 15 * 60 * 1000; // 15 minutos

    this.metricsCollector = MetricsCollector.getInstance();
  }

  /**
   * Envía alerta de fallo de GCS
   */
  async alertGCSFailure(error, operation, dataType, retryCount = 0) {
    if (!this.enabled) return;

    const alertKey = `gcs_failure_${dataType}`;
    if (this.isInCooldown(alertKey)) return;

    const alert = {
      type: 'GCS_FAILURE',
      severity: retryCount >= this.thresholds.gcsFailures ? 'CRITICAL' : 'WARNING',
      title: `🚨 Fallo en Google Cloud Storage - ${dataType.toUpperCase()}`,
      message: `Error en operación GCS: ${operation}`,
      details: {
        dataType,
        operation,
        error: error.message,
        retryCount,
        timestamp: new Date().toISOString(),
        service: 'GPS BigQuery Service'
      },
      actions: [
        'Verificar conectividad a GCS',
        'Revisar credenciales de service account',
        'Verificar permisos del bucket',
        'Revisar logs detallados'
      ]
    };

    await this.sendAlert(alert);
    this.setCooldown(alertKey);
  }

  /**
   * Envía alerta de fallo de BigQuery
   */
  async alertBigQueryFailure(error, operation, dataType, jobId = null, retryCount = 0) {
    if (!this.enabled) return;

    const alertKey = `bigquery_failure_${dataType}`;
    if (this.isInCooldown(alertKey)) return;

    const alert = {
      type: 'BIGQUERY_FAILURE',
      severity: retryCount >= this.thresholds.bigQueryFailures ? 'CRITICAL' : 'WARNING',
      title: `🚨 Fallo en BigQuery - ${dataType.toUpperCase()}`,
      message: `Error en operación BigQuery: ${operation}`,
      details: {
        dataType,
        operation,
        jobId,
        error: error.message,
        retryCount,
        timestamp: new Date().toISOString(),
        service: 'GPS BigQuery Service'
      },
      actions: [
        'Verificar estado del dataset en BigQuery',
        'Revisar permisos de BigQuery',
        'Verificar formato de datos',
        'Revisar quotas de BigQuery'
      ]
    };

    await this.sendAlert(alert);
    this.setCooldown(alertKey);
  }

  /**
   * Envía alerta de alto uso de recursos
   */
  async alertHighResourceUsage(resourceType, currentValue, threshold) {
    if (!this.enabled) return;

    const alertKey = `resource_${resourceType}`;
    if (this.isInCooldown(alertKey)) return;

    const alert = {
      type: 'HIGH_RESOURCE_USAGE',
      severity: currentValue > threshold * 1.2 ? 'CRITICAL' : 'WARNING',
      title: `⚠️ Alto uso de ${resourceType.toUpperCase()}`,
      message: `Uso de ${resourceType}: ${currentValue}% (umbral: ${threshold}%)`,
      details: {
        resourceType,
        currentValue,
        threshold,
        timestamp: new Date().toISOString(),
        service: 'GPS BigQuery Service'
      },
      actions: [
        'Revisar procesos en ejecución',
        'Verificar limpieza automática',
        'Considerar escalamiento de recursos',
        'Revisar configuración de batch size'
      ]
    };

    await this.sendAlert(alert);
    this.setCooldown(alertKey);
  }

  /**
   * Envía alerta de costos elevados
   */
  async alertHighCosts(currentCost, threshold) {
    if (!this.enabled) return;

    const alertKey = 'high_costs';
    if (this.isInCooldown(alertKey)) return;

    const alert = {
      type: 'HIGH_COSTS',
      severity: currentCost > threshold * 1.5 ? 'CRITICAL' : 'WARNING',
      title: `💰 Costos GCP Elevados`,
      message: `Costo estimado: $${currentCost} (umbral: $${threshold})`,
      details: {
        currentCost,
        threshold,
        timestamp: new Date().toISOString(),
        service: 'GPS BigQuery Service'
      },
      actions: [
        'Revisar uso de BigQuery',
        'Verificar limpieza de archivos GCS',
        'Optimizar batch sizes',
        'Revisar configuración de retención'
      ]
    };

    await this.sendAlert(alert);
    this.setCooldown(alertKey);
  }

  /**
   * Monitorea métricas y envía alertas automáticamente
   */
  async monitorAndAlert() {
    if (!this.enabled) return;

    try {
      const metrics = await this.metricsCollector.getMetrics();

      // Monitorear tasa de errores GCS
      const gcsMetrics = await this.metricsCollector.getGCSMetrics();
      const gcsErrorRate = this.calculateErrorRate(
        gcsMetrics.summary.failedUploads,
        gcsMetrics.summary.totalUploads
      );

      if (gcsErrorRate > this.thresholds.errorRate) {
        await this.alertGCSFailure(
          new Error(`Tasa de error GCS: ${gcsErrorRate}%`),
          'batch_monitoring',
          'general',
          Math.floor(gcsErrorRate / this.thresholds.errorRate)
        );
      }

      // Monitorear tasa de errores BigQuery
      const bqMetrics = await this.metricsCollector.getBigQueryMetrics();
      const bqErrorRate = this.calculateErrorRate(
        bqMetrics.summary.failedBatchJobs,
        bqMetrics.summary.totalBatchJobs
      );

      if (bqErrorRate > this.thresholds.errorRate) {
        await this.alertBigQueryFailure(
          new Error(`Tasa de error BigQuery: ${bqErrorRate}%`),
          'batch_monitoring',
          'general',
          null,
          Math.floor(bqErrorRate / this.thresholds.errorRate)
        );
      }

      // Monitorear uso de memoria
      if (metrics.system.memoryUsage) {
        const memoryUsagePercent = (metrics.system.memoryUsage.heapUsed / metrics.system.memoryUsage.heapTotal) * 100;
        if (memoryUsagePercent > this.thresholds.memoryUsage) {
          await this.alertHighResourceUsage('memory', memoryUsagePercent, this.thresholds.memoryUsage);
        }
      }

      // Monitorear costos
      const costMetrics = await this.metricsCollector.getCostMetrics();
      const costThreshold = parseFloat(process.env.COST_ALERT_THRESHOLD) || 100.0;
      if (costMetrics.totalEstimatedCost > costThreshold) {
        await this.alertHighCosts(costMetrics.totalEstimatedCost, costThreshold);
      }

    } catch (error) {
      logger.error('❌ Error en monitoreo de alertas:', error.message);
    }
  }

  /**
   * Calcula tasa de error
   */
  calculateErrorRate(failures, total) {
    return total > 0 ? (failures / total) * 100 : 0;
  }

  /**
   * Envía alerta a través de múltiples canales
   */
  async sendAlert(alert) {
    const promises = [];

    // Enviar a webhook (Slack, Teams, etc.)
    if (this.webhookUrl) {
      promises.push(this.sendWebhookAlert(alert));
    }

    // Enviar por email
    if (this.emailEnabled && this.emailConfig.to) {
      promises.push(this.sendEmailAlert(alert));
    }

    // Log local
    promises.push(this.logAlert(alert));

    try {
      await Promise.allSettled(promises);
      logger.info(`🚨 Alerta enviada: ${alert.title}`);
    } catch (error) {
      logger.error('❌ Error enviando alerta:', error.message);
    }
  }

  /**
   * Envía alerta via webhook
   */
  async sendWebhookAlert(alert) {
    const webhookUrls = this.getWebhookUrls();
    const promises = webhookUrls.map(({ url, type }) => this.sendToWebhook(url, alert, type));
    
    try {
      await Promise.allSettled(promises);
    } catch (error) {
      logger.error('❌ Error enviando webhook alerts:', error.message);
    }
  }

  /**
   * Obtiene URLs de webhooks configurados
   */
  getWebhookUrls() {
    const urls = [];
    
    if (process.env.SLACK_WEBHOOK_URL) {
      urls.push({ url: process.env.SLACK_WEBHOOK_URL, type: 'slack' });
    }
    
    if (process.env.TEAMS_WEBHOOK_URL) {
      urls.push({ url: process.env.TEAMS_WEBHOOK_URL, type: 'teams' });
    }
    
    if (process.env.DISCORD_WEBHOOK_URL) {
      urls.push({ url: process.env.DISCORD_WEBHOOK_URL, type: 'discord' });
    }
    
    // Webhook genérico (legacy)
    if (this.webhookUrl && !urls.some(u => u.url === this.webhookUrl)) {
      urls.push({ url: this.webhookUrl, type: 'generic' });
    }
    
    return urls;
  }

  /**
   * Envía alerta a webhook específico
   */
  async sendToWebhook(url, alert, type) {
    try {
      let payload;
      
      switch (type) {
        case 'slack':
          payload = this.formatSlackPayload(alert);
          break;
        case 'teams':
          payload = this.formatTeamsPayload(alert);
          break;
        case 'discord':
          payload = this.formatDiscordPayload(alert);
          break;
        default:
          payload = this.formatGenericPayload(alert);
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      logger.debug(`✅ Alerta enviada a ${type}: ${alert.title}`);

    } catch (error) {
      logger.error(`❌ Error enviando alerta a ${type}:`, error.message);
    }
  }

  /**
   * Formatea payload para Slack
   */
  formatSlackPayload(alert) {
    const color = alert.severity === 'CRITICAL' ? 'danger' : 'warning';
    const emoji = alert.severity === 'CRITICAL' ? '🚨' : '⚠️';
    
    return {
      text: `${emoji} ${alert.title}`,
      attachments: [{
        color,
        fields: [
          {
            title: 'Mensaje',
            value: alert.message,
            short: false
          },
          {
            title: 'Severidad',
            value: alert.severity,
            short: true
          },
          {
            title: 'Servicio',
            value: alert.details.service,
            short: true
          },
          {
            title: 'Timestamp',
            value: alert.details.timestamp,
            short: false
          },
          {
            title: 'Acciones Recomendadas',
            value: alert.actions.map(action => `• ${action}`).join('\n'),
            short: false
          }
        ],
        footer: 'GPS BigQuery Service',
        ts: Math.floor(new Date(alert.details.timestamp).getTime() / 1000)
      }]
    };
  }

  /**
   * Formatea payload para Microsoft Teams
   */
  formatTeamsPayload(alert) {
    const color = alert.severity === 'CRITICAL' ? 'attention' : 'warning';
    
    return {
      '@type': 'MessageCard',
      '@context': 'http://schema.org/extensions',
      themeColor: alert.severity === 'CRITICAL' ? 'FF0000' : 'FFA500',
      summary: alert.title,
      sections: [{
        activityTitle: alert.title,
        activitySubtitle: alert.message,
        facts: [
          {
            name: 'Severidad',
            value: alert.severity
          },
          {
            name: 'Servicio',
            value: alert.details.service
          },
          {
            name: 'Timestamp',
            value: alert.details.timestamp
          }
        ],
        text: `**Acciones Recomendadas:**\n${alert.actions.map(action => `• ${action}`).join('\n')}`
      }]
    };
  }

  /**
   * Formatea payload para Discord
   */
  formatDiscordPayload(alert) {
    const color = alert.severity === 'CRITICAL' ? 0xFF0000 : 0xFFA500;
    
    return {
      embeds: [{
        title: alert.title,
        description: alert.message,
        color,
        fields: [
          {
            name: 'Severidad',
            value: alert.severity,
            inline: true
          },
          {
            name: 'Servicio',
            value: alert.details.service,
            inline: true
          },
          {
            name: 'Acciones Recomendadas',
            value: alert.actions.map(action => `• ${action}`).join('\n'),
            inline: false
          }
        ],
        timestamp: alert.details.timestamp,
        footer: {
          text: 'GPS BigQuery Service'
        }
      }]
    };
  }

  /**
   * Formatea payload genérico
   */
  formatGenericPayload(alert) {
    return {
      text: alert.title,
      attachments: [{
        color: alert.severity === 'CRITICAL' ? 'danger' : 'warning',
        fields: [
          {
            title: 'Mensaje',
            value: alert.message,
            short: false
          },
          {
            title: 'Severidad',
            value: alert.severity,
            short: true
          },
          {
            title: 'Timestamp',
            value: alert.details.timestamp,
            short: true
          },
          {
            title: 'Detalles',
            value: JSON.stringify(alert.details, null, 2),
            short: false
          },
          {
            title: 'Acciones Recomendadas',
            value: alert.actions.map(action => `• ${action}`).join('\n'),
            short: false
          }
        ]
      }]
    };
  }

  /**
   * Envía alerta por email (implementación básica)
   */
  async sendEmailAlert(alert) {
    try {
      // Nota: En producción real, usar nodemailer o servicio de email
      logger.info(`📧 Email alert enviado a ${this.emailConfig.to}: ${alert.title}`);
      
      // Implementación placeholder - en producción usar nodemailer
      const emailContent = {
        to: this.emailConfig.to,
        subject: `[${alert.severity}] ${alert.title}`,
        body: `
          ${alert.message}
          
          Detalles:
          ${JSON.stringify(alert.details, null, 2)}
          
          Acciones Recomendadas:
          ${alert.actions.map(action => `• ${action}`).join('\n')}
        `
      };

      // En producción, implementar envío real de email
      logger.debug('📧 Email content prepared:', emailContent);

    } catch (error) {
      logger.error('❌ Error enviando email alert:', error.message);
    }
  }

  /**
   * Registra alerta en logs
   */
  async logAlert(alert) {
    logger.warn(`🚨 ALERT [${alert.severity}] ${alert.title}`, {
      type: alert.type,
      message: alert.message,
      details: alert.details,
      actions: alert.actions
    });
  }

  /**
   * Verifica si una alerta está en cooldown
   */
  isInCooldown(alertKey) {
    const lastAlert = this.alertCooldown.get(alertKey);
    if (!lastAlert) return false;
    
    return (Date.now() - lastAlert) < this.cooldownPeriod;
  }

  /**
   * Establece cooldown para una alerta
   */
  setCooldown(alertKey) {
    this.alertCooldown.set(alertKey, Date.now());
  }

  /**
   * Limpia cooldowns expirados
   */
  cleanupCooldowns() {
    const now = Date.now();
    for (const [key, timestamp] of this.alertCooldown.entries()) {
      if (now - timestamp > this.cooldownPeriod) {
        this.alertCooldown.delete(key);
      }
    }
  }

  /**
   * Obtiene estado del sistema de alertas
   */
  getStatus() {
    return {
      enabled: this.enabled,
      webhookConfigured: !!this.webhookUrl,
      emailEnabled: this.emailEnabled,
      thresholds: this.thresholds,
      activeCooldowns: Array.from(this.alertCooldown.keys()),
      cooldownPeriod: this.cooldownPeriod
    };
  }
}