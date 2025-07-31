#!/usr/bin/env node

/**
 * Script de diagnóstico para BigQuery
 */

import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { logger } from '../src/utils/logger.js';
import { config } from '../src/config/env.js';

async function diagnoseBigQuery() {
  const processor = new BigQueryBatchProcessor();
  
  try {
    logger.info('🔍 Iniciando diagnóstico de BigQuery...');
    
    // Mostrar configuración
    logger.info('📋 Configuración BigQuery:', {
      projectId: config.bigquery.projectId,
      datasetId: config.bigquery.datasetId,
      location: config.bigquery.location,
      keyFilename: config.bigquery.keyFilename,
      tables: config.bigquery.tables,
      simulationMode: processor.simulationMode
    });
    
    // Verificar inicialización
    logger.info('🔧 Intentando inicializar BigQuery...');
    await processor.initialize();
    logger.info('✅ BigQuery inicializado exitosamente');
    
    // Obtener estado
    const status = await processor.getStatus();
    logger.info('📊 Estado de BigQuery:', status);
    
    // Obtener estadísticas de tablas
    logger.info('📋 Obteniendo estadísticas de tablas...');
    const tableStats = await processor.getTableStats();
    logger.info('📊 Estadísticas de tablas:', tableStats);
    
    // Listar jobs recientes
    logger.info('📋 Listando jobs recientes...');
    const recentJobs = await processor.listRecentJobs({ maxResults: 5 });
    logger.info('📊 Jobs recientes:', recentJobs);
    
    // Probar procesamiento simulado
    logger.info('🧪 Probando procesamiento simulado...');
    const testResult = await processor.processGCSFileSimulated(
      'gs://test-bucket/test-file.json',
      'gps',
      { processingId: 'test-123', recordCount: 100 }
    );
    logger.info('✅ Procesamiento simulado exitoso:', testResult);
    
    logger.info('✅ Diagnóstico de BigQuery completado exitosamente');
    
  } catch (error) {
    logger.error('❌ Error en diagnóstico de BigQuery:', error.message);
    logger.error('Stack trace:', error.stack);
    
    // Información adicional de debugging
    logger.info('🔍 Información de debugging:');
    logger.info('- Modo simulación:', processor.simulationMode);
    logger.info('- Inicializado:', processor.isInitialized);
    logger.info('- Project ID:', processor.projectId);
    logger.info('- Dataset ID:', processor.datasetId);
    logger.info('- Key filename:', processor.keyFilename);
    
    // Verificar archivo de credenciales
    try {
      const fs = await import('fs/promises');
      const stats = await fs.stat(processor.keyFilename);
      logger.info('- Archivo de credenciales existe:', true);
      logger.info('- Tamaño del archivo:', stats.size, 'bytes');
    } catch (fileError) {
      logger.error('- Error con archivo de credenciales:', fileError.message);
    }
    
  } finally {
    // Limpiar recursos
    await processor.cleanup();
  }
}

// Ejecutar diagnóstico si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  diagnoseBigQuery()
    .then(() => {
      logger.info('🎉 Diagnóstico completado');
      process.exit(0);
    })
    .catch((error) => {
      logger.error('💥 Error fatal en diagnóstico:', error.message);
      process.exit(1);
    });
}

export { diagnoseBigQuery };