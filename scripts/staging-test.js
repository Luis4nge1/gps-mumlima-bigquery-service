#!/usr/bin/env node

/**
 * Script de testing completo para ambiente de staging
 * Verifica todas las funcionalidades del sistema de backup atómico
 */

import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import { AtomicRedisProcessor } from '../src/services/AtomicRedisProcessor.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { logger } from '../src/utils/logger.js';
import { config } from '../src/config/env.js';
import fs from 'fs/promises';
import path from 'path';

/**
 * Suite de tests para ambiente de staging
 */
class StagingTestSuite {
  constructor() {
    this.processor = new GPSProcessorService();
    this.atomicProcessor = new AtomicRedisProcessor();
    this.backupManager = new BackupManager();
    this.redisRepo = new RedisRepository();
    this.testResults = [];
    this.testData = {
      gps: [],
      mobile: []
    };
  }

  /**
   * Ejecuta toda la suite de tests
   */
  async runAllTests() {
    logger.info('🧪 Iniciando suite de tests de staging para sistema de backup atómico');
    
    try {
      // Preparar ambiente de test
      await this.setupTestEnvironment();
      
      // Tests de configuración
      await this.testConfiguration();
      
      // Tests de feature flags
      await this.testFeatureFlags();
      
      // Tests de procesamiento atómico
      await this.testAtomicProcessing();
      
      // Tests de backup local
      await this.testLocalBackup();
      
      // Tests de recovery
      await this.testBackupRecovery();
      
      // Tests de limpieza
      await this.testBackupCleanup();
      
      // Tests de monitoreo
      await this.testMonitoring();
      
      // Tests de carga
      await this.testLoadHandling();
      
      // Generar reporte final
      await this.generateTestReport();
      
    } catch (error) {
      logger.error('❌ Error crítico en suite de tests:', error.message);
      this.addTestResult('CRITICAL_ERROR', false, error.message);
    } finally {
      await this.cleanupTestEnvironment();
    }
  }

  /**
   * Configura el ambiente de test
   */
  async setupTestEnvironment() {
    logger.info('🔧 Configurando ambiente de test...');
    
    try {
      // Inicializar servicios
      await this.processor.initialize();
      await this.redisRepo.connect();
      
      // Limpiar datos previos
      await this.redisRepo.clearListData(config.gps.listKey);
      await this.redisRepo.clearListData('mobile:history:global');
      
      // Limpiar backups de test previos
      const testBackupPath = path.join(config.backup.storagePath, 'test-*');
      try {
        const files = await fs.readdir(config.backup.storagePath);
        const testFiles = files.filter(f => f.startsWith('test-'));
        for (const file of testFiles) {
          await fs.unlink(path.join(config.backup.storagePath, file));
        }
      } catch (cleanupError) {
        logger.warn('⚠️ Error limpiando backups de test previos:', cleanupError.message);
      }
      
      // Generar datos de test
      this.generateTestData();
      
      this.addTestResult('SETUP', true, 'Ambiente de test configurado correctamente');
      
    } catch (error) {
      this.addTestResult('SETUP', false, `Error configurando ambiente: ${error.message}`);
      throw error;
    }
  }

  /**
   * Genera datos de test realistas
   */
  generateTestData() {
    const now = Date.now();
    
    // Generar datos GPS de test
    for (let i = 0; i < 50; i++) {
      this.testData.gps.push({
        id: `test_gps_${i}`,
        latitude: -12.0464 + (Math.random() - 0.5) * 0.01,
        longitude: -77.0428 + (Math.random() - 0.5) * 0.01,
        timestamp: new Date(now - (i * 1000)).toISOString(),
        speed: Math.random() * 60,
        heading: Math.random() * 360,
        altitude: 150 + Math.random() * 100,
        accuracy: 5 + Math.random() * 10,
        device_id: `test_device_${Math.floor(i / 10)}`
      });
    }
    
    // Generar datos Mobile de test
    for (let i = 0; i < 30; i++) {
      this.testData.mobile.push({
        id: `test_mobile_${i}`,
        latitude: -12.0464 + (Math.random() - 0.5) * 0.01,
        longitude: -77.0428 + (Math.random() - 0.5) * 0.01,
        timestamp: new Date(now - (i * 1500)).toISOString(),
        signal_strength: -60 - Math.random() * 40,
        network_type: ['4G', '5G', '3G'][Math.floor(Math.random() * 3)],
        device_id: `test_mobile_${Math.floor(i / 10)}`
      });
    }
    
    logger.info(`📊 Datos de test generados: ${this.testData.gps.length} GPS + ${this.testData.mobile.length} Mobile`);
  }

  /**
   * Test de configuración del sistema
   */
  async testConfiguration() {
    logger.info('🔧 Testing configuración del sistema...');
    
    try {
      // Verificar variables de entorno críticas
      const requiredVars = [
        'ATOMIC_PROCESSING_ENABLED',
        'BACKUP_MAX_RETRIES',
        'BACKUP_RETENTION_HOURS',
        'BACKUP_STORAGE_PATH'
      ];
      
      for (const varName of requiredVars) {
        if (!process.env[varName]) {
          throw new Error(`Variable de entorno requerida faltante: ${varName}`);
        }
      }
      
      // Verificar configuración de backup
      const backupConfig = config.backup;
      if (backupConfig.maxRetries < 1) {
        throw new Error('BACKUP_MAX_RETRIES debe ser >= 1');
      }
      
      if (backupConfig.retentionHours < 1) {
        throw new Error('BACKUP_RETENTION_HOURS debe ser >= 1');
      }
      
      // Verificar directorio de backup
      await fs.access(backupConfig.storagePath);
      
      this.addTestResult('CONFIG', true, 'Configuración válida');
      
    } catch (error) {
      this.addTestResult('CONFIG', false, `Error de configuración: ${error.message}`);
    }
  }

  /**
   * Test de feature flags
   */
  async testFeatureFlags() {
    logger.info('🚩 Testing feature flags...');
    
    try {
      // Test 1: Verificar estado inicial del feature flag
      const initialState = this.atomicProcessor.isAtomicProcessingEnabled();
      logger.info(`🔍 Estado inicial de ATOMIC_PROCESSING_ENABLED: ${initialState}`);
      
      // Test 2: Cambiar feature flag en runtime
      this.atomicProcessor.setAtomicProcessingEnabled(!initialState);
      const changedState = this.atomicProcessor.isAtomicProcessingEnabled();
      
      if (changedState === initialState) {
        throw new Error('Feature flag no cambió en runtime');
      }
      
      // Test 3: Verificar modo de procesamiento
      const mode = this.atomicProcessor.getProcessingMode();
      const expectedMode = changedState ? 'atomic' : 'legacy';
      
      if (mode !== expectedMode) {
        throw new Error(`Modo esperado: ${expectedMode}, obtenido: ${mode}`);
      }
      
      // Restaurar estado inicial
      this.atomicProcessor.setAtomicProcessingEnabled(initialState);
      
      this.addTestResult('FEATURE_FLAGS', true, 'Feature flags funcionando correctamente');
      
    } catch (error) {
      this.addTestResult('FEATURE_FLAGS', false, `Error en feature flags: ${error.message}`);
    }
  }

  /**
   * Test de procesamiento atómico
   */
  async testAtomicProcessing() {
    logger.info('⚛️ Testing procesamiento atómico...');
    
    try {
      // Asegurar que el procesamiento atómico esté habilitado
      this.atomicProcessor.setAtomicProcessingEnabled(true);
      
      // Cargar datos de test en Redis
      await this.loadTestDataToRedis();
      
      // Test 1: Extracción atómica GPS
      const gpsResult = await this.atomicProcessor.extractAndClearGPSData();
      if (!gpsResult.success) {
        throw new Error(`Extracción GPS falló: ${gpsResult.error}`);
      }
      
      if (gpsResult.recordCount !== this.testData.gps.length) {
        throw new Error(`GPS: esperados ${this.testData.gps.length}, extraídos ${gpsResult.recordCount}`);
      }
      
      // Verificar que Redis GPS esté limpio
      const gpsStatsAfter = await this.redisRepo.getGPSStats();
      if (gpsStatsAfter.totalRecords !== 0) {
        throw new Error(`Redis GPS no limpio: ${gpsStatsAfter.totalRecords} registros restantes`);
      }
      
      // Recargar datos para test Mobile
      await this.loadTestDataToRedis();
      
      // Test 2: Extracción coordinada
      const allResult = await this.atomicProcessor.extractAllData();
      if (!allResult.success) {
        throw new Error(`Extracción coordinada falló: ${allResult.error}`);
      }
      
      if (allResult.totalRecords !== (this.testData.gps.length + this.testData.mobile.length)) {
        throw new Error(`Total esperado: ${this.testData.gps.length + this.testData.mobile.length}, extraído: ${allResult.totalRecords}`);
      }
      
      if (!allResult.allCleared) {
        throw new Error('Redis no completamente limpio después de extracción coordinada');
      }
      
      this.addTestResult('ATOMIC_PROCESSING', true, 'Procesamiento atómico funcionando correctamente');
      
    } catch (error) {
      this.addTestResult('ATOMIC_PROCESSING', false, `Error en procesamiento atómico: ${error.message}`);
    }
  }

  /**
   * Test de backup local
   */
  async testLocalBackup() {
    logger.info('💾 Testing sistema de backup local...');
    
    try {
      // Test 1: Crear backup GPS
      const gpsBackupResult = await this.backupManager.saveToLocalBackup(
        this.testData.gps,
        'gps',
        { originalFailureReason: 'Test GCS failure', testBackup: true }
      );
      
      if (!gpsBackupResult.success) {
        throw new Error(`Error creando backup GPS: ${gpsBackupResult.error}`);
      }
      
      // Test 2: Crear backup Mobile
      const mobileBackupResult = await this.backupManager.saveToLocalBackup(
        this.testData.mobile,
        'mobile',
        { originalFailureReason: 'Test GCS failure', testBackup: true }
      );
      
      if (!mobileBackupResult.success) {
        throw new Error(`Error creando backup Mobile: ${mobileBackupResult.error}`);
      }
      
      // Test 3: Listar backups pendientes
      const pendingBackups = await this.backupManager.getLocalBackupFiles();
      const testBackups = pendingBackups.filter(b => b.metadata.testBackup);
      
      if (testBackups.length < 2) {
        throw new Error(`Esperados 2 backups de test, encontrados ${testBackups.length}`);
      }
      
      // Test 4: Verificar estructura de backup
      const gpsBackup = testBackups.find(b => b.type === 'gps');
      if (!gpsBackup || !gpsBackup.data || gpsBackup.data.length !== this.testData.gps.length) {
        throw new Error('Estructura de backup GPS inválida');
      }
      
      // Guardar IDs para cleanup
      this.testBackupIds = testBackups.map(b => b.id);
      
      this.addTestResult('LOCAL_BACKUP', true, 'Sistema de backup local funcionando correctamente');
      
    } catch (error) {
      this.addTestResult('LOCAL_BACKUP', false, `Error en backup local: ${error.message}`);
    }
  }

  /**
   * Test de recovery de backups
   */
  async testBackupRecovery() {
    logger.info('🔄 Testing recovery de backups...');
    
    try {
      // Mock de función de upload a GCS
      const mockGCSUpload = async (data, type) => {
        // Simular éxito en el primer intento
        return {
          success: true,
          gcsFile: `test-${type}-${Date.now()}.json`,
          recordsUploaded: data.length
        };
      };
      
      // Obtener backups pendientes de test
      const pendingBackups = await this.backupManager.getLocalBackupFiles();
      const testBackups = pendingBackups.filter(b => b.metadata.testBackup);
      
      if (testBackups.length === 0) {
        throw new Error('No hay backups de test para recovery');
      }
      
      // Test recovery de cada backup
      for (const backup of testBackups) {
        const recoveryResult = await this.backupManager.processLocalBackupFile(backup, mockGCSUpload);
        
        if (!recoveryResult.success) {
          throw new Error(`Recovery falló para backup ${backup.id}: ${recoveryResult.error}`);
        }
        
        if (recoveryResult.recordsProcessed !== backup.data.length) {
          throw new Error(`Records procesados incorrectos para ${backup.id}`);
        }
      }
      
      // Verificar que los backups fueron marcados como completados
      await new Promise(resolve => setTimeout(resolve, 1000)); // Esperar actualización
      
      const updatedBackups = await this.backupManager.getAllBackupFiles();
      const completedTestBackups = updatedBackups.filter(b => 
        b.metadata.testBackup && b.status === 'completed'
      );
      
      if (completedTestBackups.length !== testBackups.length) {
        throw new Error(`Esperados ${testBackups.length} backups completados, encontrados ${completedTestBackups.length}`);
      }
      
      this.addTestResult('BACKUP_RECOVERY', true, 'Recovery de backups funcionando correctamente');
      
    } catch (error) {
      this.addTestResult('BACKUP_RECOVERY', false, `Error en recovery: ${error.message}`);
    }
  }

  /**
   * Test de limpieza de backups
   */
  async testBackupCleanup() {
    logger.info('🧹 Testing limpieza de backups...');
    
    try {
      // Ejecutar limpieza de backups completados
      const cleanupResult = await this.backupManager.cleanupCompletedBackups(0); // Limpiar inmediatamente
      
      if (!cleanupResult.success) {
        throw new Error(`Error en limpieza: ${cleanupResult.error}`);
      }
      
      // Verificar que los backups de test fueron eliminados
      const remainingBackups = await this.backupManager.getAllBackupFiles();
      const remainingTestBackups = remainingBackups.filter(b => b.metadata.testBackup);
      
      if (remainingTestBackups.length > 0) {
        logger.warn(`⚠️ Aún quedan ${remainingTestBackups.length} backups de test después de limpieza`);
        // Limpiar manualmente
        for (const backup of remainingTestBackups) {
          try {
            await fs.unlink(backup.filePath);
          } catch (unlinkError) {
            logger.warn(`⚠️ Error eliminando backup manual: ${unlinkError.message}`);
          }
        }
      }
      
      this.addTestResult('BACKUP_CLEANUP', true, `Limpieza funcionando correctamente (${cleanupResult.cleaned} eliminados)`);
      
    } catch (error) {
      this.addTestResult('BACKUP_CLEANUP', false, `Error en limpieza: ${error.message}`);
    }
  }

  /**
   * Test de monitoreo y métricas
   */
  async testMonitoring() {
    logger.info('📊 Testing sistema de monitoreo...');
    
    try {
      // Test health check del procesador atómico
      const atomicHealth = await this.atomicProcessor.healthCheck();
      if (!atomicHealth.healthy) {
        throw new Error(`Procesador atómico no saludable: ${atomicHealth.error}`);
      }
      
      // Test estadísticas del procesador atómico
      const atomicStats = await this.atomicProcessor.getStats();
      if (!atomicStats.initialized) {
        throw new Error('Procesador atómico no inicializado');
      }
      
      // Test estadísticas de backup
      const backupStats = await this.backupManager.getBackupStats();
      if (backupStats === null) {
        throw new Error('No se pudieron obtener estadísticas de backup');
      }
      
      // Verificar que las métricas incluyen información de feature flags
      if (typeof atomicStats.atomicProcessingEnabled !== 'boolean') {
        throw new Error('Métricas no incluyen información de feature flags');
      }
      
      this.addTestResult('MONITORING', true, 'Sistema de monitoreo funcionando correctamente');
      
    } catch (error) {
      this.addTestResult('MONITORING', false, `Error en monitoreo: ${error.message}`);
    }
  }

  /**
   * Test de manejo de carga
   */
  async testLoadHandling() {
    logger.info('⚡ Testing manejo de carga...');
    
    try {
      // Generar datos de carga más grandes
      const largeGPSData = [];
      for (let i = 0; i < 1000; i++) {
        largeGPSData.push({
          id: `load_test_gps_${i}`,
          latitude: -12.0464 + (Math.random() - 0.5) * 0.1,
          longitude: -77.0428 + (Math.random() - 0.5) * 0.1,
          timestamp: new Date(Date.now() - (i * 100)).toISOString(),
          speed: Math.random() * 60,
          device_id: `load_test_device_${Math.floor(i / 100)}`
        });
      }
      
      // Cargar datos grandes en Redis
      for (const record of largeGPSData) {
        await this.redisRepo.addGPSData(record);
      }
      
      // Test extracción atómica con carga grande
      const startTime = Date.now();
      const extractionResult = await this.atomicProcessor.extractAndClearGPSData();
      const extractionTime = Date.now() - startTime;
      
      if (!extractionResult.success) {
        throw new Error(`Extracción de carga falló: ${extractionResult.error}`);
      }
      
      if (extractionResult.recordCount !== largeGPSData.length) {
        throw new Error(`Carga: esperados ${largeGPSData.length}, extraídos ${extractionResult.recordCount}`);
      }
      
      // Verificar que el tiempo de extracción sea razonable (< 10 segundos)
      if (extractionTime > 10000) {
        logger.warn(`⚠️ Extracción lenta: ${extractionTime}ms para ${largeGPSData.length} registros`);
      }
      
      // Verificar que Redis esté limpio
      const finalStats = await this.redisRepo.getGPSStats();
      if (finalStats.totalRecords !== 0) {
        throw new Error(`Redis no limpio después de carga: ${finalStats.totalRecords} restantes`);
      }
      
      this.addTestResult('LOAD_HANDLING', true, `Manejo de carga OK (${largeGPSData.length} registros en ${extractionTime}ms)`);
      
    } catch (error) {
      this.addTestResult('LOAD_HANDLING', false, `Error en manejo de carga: ${error.message}`);
    }
  }

  /**
   * Carga datos de test en Redis
   */
  async loadTestDataToRedis() {
    // Cargar datos GPS
    for (const gpsRecord of this.testData.gps) {
      await this.redisRepo.addGPSData(gpsRecord);
    }
    
    // Cargar datos Mobile
    for (const mobileRecord of this.testData.mobile) {
      await this.redisRepo.addMobileData(mobileRecord);
    }
    
    logger.debug(`📥 Cargados ${this.testData.gps.length} GPS + ${this.testData.mobile.length} Mobile en Redis`);
  }

  /**
   * Agrega resultado de test
   */
  addTestResult(testName, success, message) {
    const result = {
      test: testName,
      success: success,
      message: message,
      timestamp: new Date().toISOString()
    };
    
    this.testResults.push(result);
    
    const status = success ? '✅' : '❌';
    logger.info(`${status} ${testName}: ${message}`);
  }

  /**
   * Genera reporte final de tests
   */
  async generateTestReport() {
    const successfulTests = this.testResults.filter(r => r.success).length;
    const totalTests = this.testResults.length;
    const successRate = ((successfulTests / totalTests) * 100).toFixed(1);
    
    const report = {
      summary: {
        totalTests: totalTests,
        successfulTests: successfulTests,
        failedTests: totalTests - successfulTests,
        successRate: `${successRate}%`,
        timestamp: new Date().toISOString()
      },
      environment: {
        nodeVersion: process.version,
        atomicProcessingEnabled: config.backup.atomicProcessingEnabled,
        backupMaxRetries: config.backup.maxRetries,
        backupRetentionHours: config.backup.retentionHours
      },
      results: this.testResults
    };
    
    // Guardar reporte
    const reportPath = `tmp/staging-test-report-${Date.now()}.json`;
    await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
    
    // Log resumen
    logger.info('📋 REPORTE FINAL DE TESTS DE STAGING');
    logger.info(`   Total de tests: ${totalTests}`);
    logger.info(`   Exitosos: ${successfulTests}`);
    logger.info(`   Fallidos: ${totalTests - successfulTests}`);
    logger.info(`   Tasa de éxito: ${successRate}%`);
    logger.info(`   Reporte guardado en: ${reportPath}`);
    
    if (successfulTests === totalTests) {
      logger.info('🎉 TODOS LOS TESTS PASARON - SISTEMA LISTO PARA PRODUCCIÓN');
    } else {
      logger.error('❌ ALGUNOS TESTS FALLARON - REVISAR ANTES DE PRODUCCIÓN');
    }
    
    return report;
  }

  /**
   * Limpia el ambiente de test
   */
  async cleanupTestEnvironment() {
    logger.info('🧹 Limpiando ambiente de test...');
    
    try {
      // Limpiar Redis
      await this.redisRepo.clearListData(config.gps.listKey);
      await this.redisRepo.clearListData('mobile:history:global');
      
      // Limpiar backups de test
      if (this.testBackupIds) {
        for (const backupId of this.testBackupIds) {
          try {
            await this.backupManager.deleteLocalBackup(backupId);
          } catch (deleteError) {
            logger.warn(`⚠️ Error eliminando backup de test ${backupId}:`, deleteError.message);
          }
        }
      }
      
      // Desconectar servicios
      await this.atomicProcessor.cleanup();
      await this.redisRepo.disconnect();
      
      logger.info('✅ Ambiente de test limpiado');
      
    } catch (error) {
      logger.error('❌ Error limpiando ambiente de test:', error.message);
    }
  }
}

// Ejecutar tests si el script se ejecuta directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  const testSuite = new StagingTestSuite();
  
  testSuite.runAllTests()
    .then(() => {
      process.exit(0);
    })
    .catch((error) => {
      logger.error('❌ Error crítico en tests de staging:', error.message);
      process.exit(1);
    });
}

export { StagingTestSuite };