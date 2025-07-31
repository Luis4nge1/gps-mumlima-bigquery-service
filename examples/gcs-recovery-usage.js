import { GCSRecoveryManager } from '../src/services/GCSRecoveryManager.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { logger } from '../src/utils/logger.js';

/**
 * Ejemplo de uso del GCSRecoveryManager
 * Demuestra el flujo completo de recovery para archivos GCS pendientes
 */
async function demonstrateGCSRecovery() {
  try {
    logger.info('🚀 Iniciando demostración del GCS Recovery Manager...');

    // Inicializar adaptadores
    const gcsAdapter = new GCSAdapter();
    const bigQueryProcessor = new BigQueryBatchProcessor();
    
    await gcsAdapter.initialize();
    await bigQueryProcessor.initialize();

    // Crear recovery manager
    const recoveryManager = new GCSRecoveryManager(gcsAdapter, bigQueryProcessor);
    await recoveryManager.initialize();

    // Ejemplo 1: Crear backup GCS para recovery
    logger.info('\n📋 Ejemplo 1: Crear backup GCS para recovery');
    
    const gpsData = [
      {
        deviceId: 'device_001',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: '2025-01-15T10:30:00Z'
      },
      {
        deviceId: 'device_002',
        lat: -12.0500,
        lng: -77.0500,
        timestamp: '2025-01-15T10:31:00Z'
      }
    ];

    const gcsBackupResult = await recoveryManager.createGCSBackup(
      'gps-2025-01-15-103000.json',
      {
        dataType: 'gps',
        recordCount: gpsData.length,
        source: 'redis:gps:history:global',
        processingId: 'gps_demo_001'
      },
      gpsData // Datos originales para fallback
    );

    logger.info('✅ Backup GCS creado:', {
      success: gcsBackupResult.success,
      backupId: gcsBackupResult.backupId,
      gcsFileName: gcsBackupResult.gcsFileName
    });

    // Ejemplo 2: Crear backup para datos Mobile
    logger.info('\n📋 Ejemplo 2: Crear backup para datos Mobile');
    
    const mobileData = [
      {
        userId: 'user_001',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: '2025-01-15T10:30:00Z',
        name: 'Juan Pérez',
        email: 'juan@example.com'
      }
    ];

    const mobileBackupResult = await recoveryManager.createGCSBackup(
      'mobile-2025-01-15-103000.json',
      {
        dataType: 'mobile',
        recordCount: mobileData.length,
        source: 'redis:mobile:history:global',
        processingId: 'mobile_demo_001'
      },
      mobileData
    );

    logger.info('✅ Backup Mobile creado:', {
      success: mobileBackupResult.success,
      backupId: mobileBackupResult.backupId,
      gcsFileName: mobileBackupResult.gcsFileName
    });

    // Ejemplo 3: Obtener archivos pendientes
    logger.info('\n📋 Ejemplo 3: Obtener archivos GCS pendientes');
    
    const pendingFiles = await recoveryManager.getGCSPendingFiles();
    logger.info(`📊 Archivos pendientes encontrados: ${pendingFiles.length}`);
    
    pendingFiles.forEach((file, index) => {
      logger.info(`   ${index + 1}. ${file.gcsFileName} (${file.metadata.dataType}) - ${file.metadata.recordCount} registros`);
    });

    // Ejemplo 4: Procesar archivos pendientes
    logger.info('\n📋 Ejemplo 4: Procesar archivos GCS pendientes');
    
    const processResult = await recoveryManager.processGCSPendingFiles();
    logger.info('✅ Procesamiento completado:', {
      success: processResult.success,
      processed: processResult.processed,
      failed: processResult.failed,
      total: processResult.total
    });

    if (processResult.results && processResult.results.length > 0) {
      logger.info('📊 Detalles de procesamiento:');
      processResult.results.forEach((result, index) => {
        logger.info(`   ${index + 1}. ${result.backupId}: ${result.success ? '✅' : '❌'} (${result.method || 'gcs_to_bigquery'})`);
        if (result.success) {
          logger.info(`      📊 Registros procesados: ${result.recordsProcessed}`);
          if (result.jobId) {
            logger.info(`      🔧 Job ID: ${result.jobId}`);
          }
        }
      });
    }

    // Ejemplo 5: Obtener estadísticas de recovery
    logger.info('\n📋 Ejemplo 5: Estadísticas de recovery');
    
    const recoveryStats = await recoveryManager.getGCSRecoveryStats();
    if (recoveryStats) {
      logger.info('📊 Estadísticas de GCS Recovery:', {
        total: recoveryStats.total,
        pending: recoveryStats.pending,
        completed: recoveryStats.completed,
        failed: recoveryStats.failed,
        totalRecords: recoveryStats.totalRecords
      });

      logger.info('📊 Por tipo de datos:');
      logger.info(`   GPS: ${recoveryStats.byDataType.gps.total} total, ${recoveryStats.byDataType.gps.completed} completados`);
      logger.info(`   Mobile: ${recoveryStats.byDataType.mobile.total} total, ${recoveryStats.byDataType.mobile.completed} completados`);
    }

    // Ejemplo 6: Estado general del recovery manager
    logger.info('\n📋 Ejemplo 6: Estado del Recovery Manager');
    
    const status = await recoveryManager.getStatus();
    logger.info('📊 Estado del Recovery Manager:', {
      initialized: status.initialized,
      maxRetryAttempts: status.maxRetryAttempts,
      cleanupProcessedFiles: status.cleanupProcessedFiles,
      backupStats: status.backupStats ? {
        total: status.backupStats.total,
        pending: status.backupStats.pending,
        completed: status.backupStats.completed
      } : null
    });

    // Ejemplo 7: Limpieza de archivos procesados
    logger.info('\n📋 Ejemplo 7: Limpieza de archivos procesados');
    
    const cleanupResult = await recoveryManager.cleanupProcessedGCSFiles(1000); // 1 segundo para demo
    logger.info('🧹 Limpieza completada:', {
      success: cleanupResult.success,
      cleaned: cleanupResult.cleaned
    });

    // Ejemplo 8: Escenario de recovery desde datos originales
    logger.info('\n📋 Ejemplo 8: Recovery desde datos originales');
    
    const originalDataExample = [
      {
        deviceId: 'device_recovery',
        lat: -12.1000,
        lng: -77.1000,
        timestamp: '2025-01-15T11:00:00Z'
      }
    ];

    const recoveryTestFile = {
      id: 'recovery_test_demo',
      gcsFileName: 'missing-file-demo.json',
      metadata: {
        dataType: 'gps',
        recordCount: 1,
        processingId: 'recovery_demo'
      },
      originalData: originalDataExample
    };

    const recoveryFromDataResult = await recoveryManager.recoverFromOriginalData(recoveryTestFile);
    logger.info('✅ Recovery desde datos originales:', {
      success: recoveryFromDataResult.success,
      method: recoveryFromDataResult.method,
      recordsProcessed: recoveryFromDataResult.recordsProcessed
    });

    // Limpieza final
    await recoveryManager.cleanup();
    await gcsAdapter.cleanup();
    await bigQueryProcessor.cleanup();

    logger.info('\n🎉 Demostración del GCS Recovery Manager completada exitosamente!');

  } catch (error) {
    logger.error('❌ Error en la demostración:', error.message);
    throw error;
  }
}

/**
 * Ejemplo de integración con el flujo principal de procesamiento
 */
async function demonstrateIntegrationFlow() {
  try {
    logger.info('\n🔄 Demostración de flujo de integración...');

    const gcsAdapter = new GCSAdapter();
    const bigQueryProcessor = new BigQueryBatchProcessor();
    const recoveryManager = new GCSRecoveryManager(gcsAdapter, bigQueryProcessor);

    await Promise.all([
      gcsAdapter.initialize(),
      bigQueryProcessor.initialize(),
      recoveryManager.initialize()
    ]);

    // Simular datos de Redis
    const redisData = [
      {
        deviceId: 'device_integration',
        lat: -12.0464,
        lng: -77.0428,
        timestamp: '2025-01-15T12:00:00Z'
      }
    ];

    const processingId = `integration_${Date.now()}`;
    const gcsFileName = `gps-integration-${processingId}.json`;

    logger.info('📤 Paso 1: Subir datos a GCS...');
    
    // Paso 1: Subir a GCS
    const uploadResult = await gcsAdapter.uploadJSON(redisData, gcsFileName, {
      dataType: 'gps',
      recordCount: redisData.length,
      processingId
    });

    if (!uploadResult.success) {
      // Si falla la subida a GCS, crear backup para recovery
      logger.warn('⚠️ Fallo en subida a GCS, creando backup para recovery...');
      
      await recoveryManager.createGCSBackup(gcsFileName, {
        dataType: 'gps',
        recordCount: redisData.length,
        processingId,
        originalSize: JSON.stringify(redisData).length
      }, redisData);
      
      logger.info('💾 Backup creado para recovery posterior');
      return;
    }

    logger.info('✅ Datos subidos a GCS exitosamente');

    // Paso 2: Procesar hacia BigQuery
    logger.info('📊 Paso 2: Procesar hacia BigQuery...');
    
    const bigQueryResult = await bigQueryProcessor.processGCSFile(
      uploadResult.gcsUri,
      'gps',
      { processingId, recordCount: redisData.length }
    );

    if (!bigQueryResult.success) {
      // Si falla BigQuery, crear backup GCS para recovery
      logger.warn('⚠️ Fallo en procesamiento BigQuery, creando backup GCS...');
      
      await recoveryManager.createGCSBackup(gcsFileName, {
        dataType: 'gps',
        recordCount: redisData.length,
        processingId,
        gcsUri: uploadResult.gcsUri
      });
      
      logger.info('💾 Backup GCS creado para recovery posterior');
      return;
    }

    logger.info('✅ Datos procesados en BigQuery exitosamente');
    logger.info(`📊 Job ID: ${bigQueryResult.jobId}`);
    logger.info(`📊 Registros procesados: ${bigQueryResult.recordsProcessed}`);

    // Paso 3: Limpiar archivo GCS procesado
    if (process.env.GCS_CLEANUP_PROCESSED_FILES !== 'false') {
      logger.info('🧹 Paso 3: Limpiar archivo GCS procesado...');
      
      const deleteResult = await gcsAdapter.deleteFile(gcsFileName);
      if (deleteResult.success) {
        logger.info('✅ Archivo GCS limpiado exitosamente');
      } else {
        logger.warn('⚠️ No se pudo limpiar el archivo GCS');
      }
    }

    await Promise.all([
      recoveryManager.cleanup(),
      gcsAdapter.cleanup(),
      bigQueryProcessor.cleanup()
    ]);

    logger.info('🎉 Flujo de integración completado exitosamente!');

  } catch (error) {
    logger.error('❌ Error en el flujo de integración:', error.message);
    throw error;
  }
}

// Ejecutar ejemplos si el archivo se ejecuta directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  (async () => {
    try {
      await demonstrateGCSRecovery();
      await demonstrateIntegrationFlow();
    } catch (error) {
      logger.error('❌ Error en la demostración:', error.message);
      process.exit(1);
    }
  })();
}

export {
  demonstrateGCSRecovery,
  demonstrateIntegrationFlow
};