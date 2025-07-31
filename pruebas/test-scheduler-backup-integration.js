/**
 * Test de integración para verificar el procesamiento de backups en el scheduler
 */
import { SchedulerService } from '../src/services/SchedulerService.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { logger } from '../src/utils/logger.js';
import fs from 'fs/promises';
import path from 'path';

async function testSchedulerBackupIntegration() {
  console.log('🧪 Iniciando test de integración del scheduler con backups...');
  
  const scheduler = new SchedulerService();
  const backupManager = new BackupManager();
  
  try {
    // 1. Crear un backup de prueba
    console.log('📝 Creando backup de prueba...');
    const testData = [
      { lat: -12.0464, lng: -77.0428, timestamp: '2025-01-25T10:00:00Z' },
      { lat: -12.0465, lng: -77.0429, timestamp: '2025-01-25T10:01:00Z' }
    ];
    
    const backupResult = await backupManager.saveToLocalBackup(testData, 'gps', {
      source: 'test_integration'
    });
    
    if (!backupResult.success) {
      throw new Error(`Error creando backup: ${backupResult.error}`);
    }
    
    console.log(`✅ Backup creado: ${backupResult.backupId}`);
    
    // 2. Verificar que el backup está pendiente
    const pendingBackups = await backupManager.getLocalBackupFiles();
    console.log(`📋 Backups pendientes: ${pendingBackups.length}`);
    
    if (pendingBackups.length === 0) {
      throw new Error('No se encontraron backups pendientes');
    }
    
    // 3. Procesar backups usando el scheduler
    console.log('🔄 Procesando backups con el scheduler...');
    const processResult = await scheduler.processLocalBackups();
    
    console.log('📊 Resultado del procesamiento:', {
      success: processResult.success,
      processed: processResult.processed,
      failed: processResult.failed,
      alerts: processResult.alerts.length
    });
    
    // 4. Verificar estadísticas
    const status = await scheduler.getStatus();
    console.log('📈 Estado del scheduler:', {
      backupStats: status.backups.stats,
      cleanupInterval: status.backups.cleanupIntervalMinutes
    });
    
    // 5. Verificar alertas
    const alerts = await scheduler.getBackupAlerts();
    console.log(`🚨 Alertas de backup: ${alerts.length}`);
    
    if (alerts.length > 0) {
      alerts.forEach(alert => {
        console.log(`   - ${alert.backupId}: ${alert.lastError}`);
      });
    }
    
    // 6. Test de limpieza
    console.log('🧹 Ejecutando limpieza de backups...');
    await scheduler.executeBackupCleanup();
    
    console.log('✅ Test de integración completado exitosamente');
    
  } catch (error) {
    console.error('❌ Error en test de integración:', error.message);
    throw error;
  }
}

// Ejecutar test si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  testSchedulerBackupIntegration()
    .then(() => {
      console.log('🎉 Test completado exitosamente');
      process.exit(0);
    })
    .catch((error) => {
      console.error('💥 Test falló:', error.message);
      process.exit(1);
    });
}

export { testSchedulerBackupIntegration };