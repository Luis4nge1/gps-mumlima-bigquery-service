import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { MetricsCollector } from '../src/utils/MetricsCollector.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { FileUtils } from '../src/utils/FileUtils.js';
import fs from 'fs/promises';

describe('BackupMetrics', () => {
  let metricsCollector;
  let backupManager;
  const testBackupPath = 'tmp/test-backup-metrics/';

  beforeEach(async () => {
    // Limpiar instancia singleton
    MetricsCollector.instance = null;
    MetricsCollector.isLoaded = false;
    
    metricsCollector = new MetricsCollector();
    
    // Configurar BackupManager con path de prueba
    process.env.BACKUP_STORAGE_PATH = testBackupPath;
    process.env.BACKUP_MAX_RETRIES = '3';
    process.env.BACKUP_RETENTION_HOURS = '24';
    
    backupManager = new BackupManager();
    
    // Limpiar directorio de prueba
    await FileUtils.ensureDirectoryExists(testBackupPath);
    const files = await fs.readdir(testBackupPath);
    for (const file of files) {
      await fs.unlink(`${testBackupPath}${file}`);
    }
  });

  afterEach(async () => {
    // Limpiar directorio de prueba
    try {
      const files = await fs.readdir(testBackupPath);
      for (const file of files) {
        await fs.unlink(`${testBackupPath}${file}`);
      }
      await fs.rmdir(testBackupPath);
    } catch (error) {
      // Ignorar errores de limpieza
    }
  });

  describe('Métricas de creación de backup', () => {
    it('debe registrar métricas cuando se crea un backup local', async () => {
      const testData = [
        { id: 1, lat: 10.123, lng: -74.456, timestamp: '2025-01-25T10:00:00Z' },
        { id: 2, lat: 10.124, lng: -74.457, timestamp: '2025-01-25T10:01:00Z' }
      ];

      const result = await backupManager.saveToLocalBackup(testData, 'gps', {
        source: 'test'
      });

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(2);
      expect(result.type).toBe('gps');

      // Verificar métricas
      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.total).toBe(1);
      expect(backupMetrics.local.pending).toBe(1);
      expect(backupMetrics.local.totalRecords).toBe(2);
      expect(backupMetrics.local.byType.gps.total).toBe(1);
      expect(backupMetrics.local.byType.gps.pending).toBe(1);
      expect(backupMetrics.local.byType.gps.totalRecords).toBe(2);
    });

    it('debe registrar métricas para diferentes tipos de backup', async () => {
      const gpsData = [{ id: 1, lat: 10.123, lng: -74.456 }];
      const mobileData = [{ id: 1, device: 'test', signal: 85 }];

      await backupManager.saveToLocalBackup(gpsData, 'gps');
      await backupManager.saveToLocalBackup(mobileData, 'mobile');

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.total).toBe(2);
      expect(backupMetrics.local.byType.gps.total).toBe(1);
      expect(backupMetrics.local.byType.mobile.total).toBe(1);
      expect(backupMetrics.local.byType.gps.totalRecords).toBe(1);
      expect(backupMetrics.local.byType.mobile.totalRecords).toBe(1);
    });
  });

  describe('Métricas de procesamiento de backup', () => {
    it('debe registrar métricas cuando un backup se procesa exitosamente', async () => {
      // Crear backup
      const testData = [{ id: 1, lat: 10.123, lng: -74.456 }];
      const createResult = await backupManager.saveToLocalBackup(testData, 'gps');
      
      // Simular procesamiento exitoso
      const mockGcsUpload = vi.fn().mockResolvedValue({
        success: true,
        gcsFile: 'test-file.json',
        fileName: 'test-file.json'
      });

      const backupFiles = await backupManager.getLocalBackupFiles();
      const processResult = await backupManager.processLocalBackupFile(
        backupFiles[0], 
        mockGcsUpload
      );

      expect(processResult.success).toBe(true);
      expect(mockGcsUpload).toHaveBeenCalledWith(testData, 'gps');

      // Verificar métricas
      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.completed).toBe(1);
      expect(backupMetrics.local.pending).toBe(0);
      expect(backupMetrics.local.byType.gps.completed).toBe(1);
      expect(backupMetrics.summary.successRate).toBe('100.00');
    });

    it('debe registrar métricas cuando un backup falla y se reintenta', async () => {
      // Crear backup
      const testData = [{ id: 1, lat: 10.123, lng: -74.456 }];
      await backupManager.saveToLocalBackup(testData, 'gps');
      
      // Simular falla en GCS upload
      const mockGcsUpload = vi.fn().mockResolvedValue({
        success: false,
        error: 'GCS connection failed'
      });

      const backupFiles = await backupManager.getLocalBackupFiles();
      const processResult = await backupManager.processLocalBackupFile(
        backupFiles[0], 
        mockGcsUpload
      );

      expect(processResult.success).toBe(false);
      expect(processResult.willRetry).toBe(true);

      // Verificar métricas
      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.totalRetries).toBe(1);
      expect(backupMetrics.local.pending).toBe(1); // Vuelve a pending para retry
      expect(backupMetrics.local.failed).toBe(0); // No ha fallado definitivamente
      expect(backupMetrics.local.lastError).toBeDefined();
      expect(backupMetrics.local.lastError.message).toBe('GCS connection failed');
    });

    it('debe registrar métricas cuando un backup falla definitivamente', async () => {
      // Crear backup con maxRetries = 1 para prueba rápida
      process.env.BACKUP_MAX_RETRIES = '1';
      const testBackupManager = new BackupManager();
      
      const testData = [{ id: 1, lat: 10.123, lng: -74.456 }];
      await testBackupManager.saveToLocalBackup(testData, 'gps');
      
      // Simular falla en GCS upload
      const mockGcsUpload = vi.fn().mockResolvedValue({
        success: false,
        error: 'Permanent GCS failure'
      });

      const backupFiles = await testBackupManager.getLocalBackupFiles();
      
      // Primer intento (falla)
      await testBackupManager.processLocalBackupFile(backupFiles[0], mockGcsUpload);
      
      // Segundo intento (falla definitivamente)
      const updatedBackupFiles = await testBackupManager.getLocalBackupFiles();
      const finalResult = await testBackupManager.processLocalBackupFile(
        updatedBackupFiles[0], 
        mockGcsUpload
      );

      expect(finalResult.success).toBe(false);
      expect(finalResult.willRetry).toBe(false);

      // Verificar métricas
      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.failed).toBe(1);
      expect(backupMetrics.local.maxRetryTimeExceeded).toBe(1);
      expect(backupMetrics.alerts.maxRetriesExceeded.length).toBe(1);
      expect(backupMetrics.summary.successRate).toBe('0.00');
    });
  });

  describe('Métricas de tiempo de retry', () => {
    it('debe calcular tiempo promedio de retry', async () => {
      // Simular varios retries con diferentes tiempos
      await metricsCollector.recordBackupRetryTime(1000); // 1 segundo
      await metricsCollector.recordBackupRetryTime(2000); // 2 segundos
      await metricsCollector.recordBackupRetryTime(3000); // 3 segundos

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.avgRetryTime).toBe(2000); // Promedio: 2 segundos
    });

    it('debe mantener solo los últimos 100 registros de retry time', async () => {
      // Agregar más de 100 registros
      for (let i = 0; i < 150; i++) {
        await metricsCollector.recordBackupRetryTime(1000 + i);
      }

      // Verificar que solo se mantienen 100
      expect(metricsCollector.metrics.backup.retryTimes.length).toBe(100);
      
      // Verificar que se mantienen los más recientes
      const firstRetryTime = metricsCollector.metrics.backup.retryTimes[0];
      expect(firstRetryTime.time).toBe(1050); // 1000 + 50 (los primeros 50 fueron eliminados)
    });
  });

  describe('Alertas de backup', () => {
    it('debe generar alerta cuando un backup excede el máximo de reintentos', async () => {
      await metricsCollector.recordBackupAlert('maxRetriesExceeded', {
        backupId: 'test-backup-123',
        type: 'gps',
        retryCount: 3,
        timestamp: new Date().toISOString(),
        error: 'GCS upload failed'
      });

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.alerts.maxRetriesExceeded.length).toBe(1);
      expect(backupMetrics.summary.alertsCount.maxRetriesExceeded).toBe(1);
      
      const alert = backupMetrics.alerts.maxRetriesExceeded[0];
      expect(alert.backupId).toBe('test-backup-123');
      expect(alert.type).toBe('gps');
      expect(alert.retryCount).toBe(3);
    });

    it('debe generar alerta para backups pendientes antiguos', async () => {
      const oldTimestamp = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(); // 3 horas atrás
      
      await metricsCollector.recordBackupAlert('oldPendingBackups', {
        oldestPending: oldTimestamp,
        hoursSinceOldest: 3,
        pendingCount: 5,
        timestamp: new Date().toISOString()
      });

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.alerts.oldPendingBackups.length).toBe(1);
      expect(backupMetrics.summary.alertsCount.oldPendingBackups).toBe(1);
      
      const alert = backupMetrics.alerts.oldPendingBackups[0];
      expect(alert.hoursSinceOldest).toBe(3);
      expect(alert.pendingCount).toBe(5);
    });

    it('debe obtener alertas recientes filtradas por tiempo', async () => {
      const now = new Date();
      const recent = new Date(now.getTime() - 1 * 60 * 60 * 1000).toISOString(); // 1 hora atrás
      const old = new Date(now.getTime() - 25 * 60 * 60 * 1000).toISOString(); // 25 horas atrás

      // Agregar alerta reciente
      await metricsCollector.recordBackupAlert('maxRetriesExceeded', {
        backupId: 'recent-backup',
        timestamp: recent
      });

      // Agregar alerta antigua
      await metricsCollector.recordBackupAlert('maxRetriesExceeded', {
        backupId: 'old-backup',
        timestamp: old
      });

      const recentAlerts = metricsCollector.getRecentBackupAlerts(24); // Últimas 24 horas
      
      expect(recentAlerts.length).toBe(1);
      expect(recentAlerts[0].backupId).toBe('recent-backup');
    });
  });

  describe('Actualización de métricas desde stats', () => {
    it('debe actualizar métricas basándose en estadísticas de backup', async () => {
      const mockStats = {
        total: 10,
        pending: 3,
        processing: 1,
        completed: 5,
        failed: 1,
        totalRecords: 150,
        oldestPending: new Date(Date.now() - 30 * 60 * 1000).toISOString() // 30 minutos atrás
      };

      await metricsCollector.updateBackupMetrics(mockStats);

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      expect(backupMetrics.local.total).toBe(10);
      expect(backupMetrics.local.pending).toBe(3);
      expect(backupMetrics.local.processing).toBe(1);
      expect(backupMetrics.local.completed).toBe(5);
      expect(backupMetrics.local.failed).toBe(1);
      expect(backupMetrics.local.totalRecords).toBe(150);
    });

    it('debe generar alerta automática para backups pendientes antiguos', async () => {
      const oldTimestamp = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(); // 3 horas atrás
      
      const mockStats = {
        total: 5,
        pending: 2,
        completed: 3,
        failed: 0,
        totalRecords: 50,
        oldestPending: oldTimestamp
      };

      await metricsCollector.updateBackupMetrics(mockStats);

      const backupMetrics = await metricsCollector.getBackupMetrics();
      
      // Debe haber generado una alerta automática
      expect(backupMetrics.alerts.oldPendingBackups.length).toBe(1);
      expect(backupMetrics.alerts.oldPendingBackups[0].hoursSinceOldest).toBe(3);
    });
  });

  describe('Cálculo de tasas de éxito', () => {
    it('debe calcular correctamente la tasa de éxito general', async () => {
      // Simular métricas: 7 completados de 10 total
      metricsCollector.metrics.backup.local.total = 10;
      metricsCollector.metrics.backup.local.completed = 7;

      const successRate = metricsCollector.calculateBackupSuccessRate();
      
      expect(successRate).toBe('70.00');
    });

    it('debe calcular correctamente la tasa de éxito por tipo', async () => {
      // Simular métricas GPS: 8 completados de 10 total
      metricsCollector.metrics.backup.local.byType.gps.total = 10;
      metricsCollector.metrics.backup.local.byType.gps.completed = 8;

      const gpsSuccessRate = metricsCollector.calculateTypeSuccessRate('gps');
      
      expect(gpsSuccessRate).toBe('80.00');
    });

    it('debe retornar 0.00 cuando no hay backups', async () => {
      const successRate = metricsCollector.calculateBackupSuccessRate();
      const gpsSuccessRate = metricsCollector.calculateTypeSuccessRate('gps');
      
      expect(successRate).toBe('0.00');
      expect(gpsSuccessRate).toBe('0.00');
    });
  });
});