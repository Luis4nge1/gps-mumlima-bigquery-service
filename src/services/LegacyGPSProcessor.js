import { RedisRepository } from '../repositories/RedisRepository.js';
import { BigQueryAdapter } from '../adapters/BigQueryAdapter.js';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';
import { GPSValidator } from '../validators/GPSValidator.js';
import { metrics } from '../utils/metrics.js';

/**
 * Procesador GPS Legacy - Flujo original
 * Mantiene el flujo original: Redis → BigQueryAdapter (archivo simulado)
 * Usado para comparación durante la migración gradual
 */
export class LegacyGPSProcessor {
  constructor() {
    this.redisRepo = new RedisRepository();
    this.bigQueryAdapter = new BigQueryAdapter();
    this.validator = new GPSValidator();
    this.metrics = metrics;
    this.isProcessing = false;
  }

  /**
   * Inicializa el procesador legacy
   */
  async initialize() {
    try {
      logger.info('🔧 Inicializando Legacy GPS Processor...');
      
      await this.bigQueryAdapter.initialize();
      
      logger.info('✅ Legacy GPS Processor inicializado exitosamente');
    } catch (error) {
      logger.error('❌ Error inicializando Legacy GPS Processor:', error.message);
      throw error;
    }
  }

  /**
   * Procesa datos GPS usando el flujo legacy original
   * Flujo: Redis → Validación → BigQueryAdapter (archivo)
   */
  async processGPSData() {
    if (this.isProcessing) {
      logger.warn('⚠️ Procesamiento legacy ya en curso, saltando ejecución');
      return { success: false, error: 'Processing already in progress' };
    }

    this.isProcessing = true;
    const startTime = Date.now();

    try {
      logger.info('🔄 Iniciando procesamiento legacy de datos GPS...');

      // Paso 1: Obtener estadísticas de Redis
      const gpsStats = await this.redisRepo.getGPSStats();
      
      if (gpsStats.totalRecords === 0) {
        logger.info('📍 No hay datos GPS para procesar (legacy)');
        return {
          success: true,
          recordsProcessed: 0,
          message: 'No data to process',
          flowType: 'legacy'
        };
      }

      logger.info(`📊 Datos disponibles en Redis: ${gpsStats.totalRecords} registros GPS`);

      // Paso 2: Obtener datos de Redis con límite de seguridad
      const maxRecords = config.gps.batchSize * 5; // Límite más conservador para legacy
      const gpsData = await this.redisRepo.getListData(
        config.gps.listKey, 
        Math.min(maxRecords, gpsStats.totalRecords)
      );

      if (gpsData.length === 0) {
        logger.info('📍 No se obtuvieron datos de Redis (legacy)');
        return {
          success: true,
          recordsProcessed: 0,
          message: 'No data retrieved from Redis',
          flowType: 'legacy'
        };
      }

      logger.info(`📥 Obtenidos ${gpsData.length} registros de Redis (legacy)`);

      // Paso 3: Validar datos GPS
      const validationResult = await this.validateGPSData(gpsData);
      
      if (!validationResult.isValid || validationResult.validData.length === 0) {
        logger.warn('⚠️ No hay datos GPS válidos para procesar (legacy)');
        return {
          success: true,
          recordsProcessed: 0,
          message: 'No valid GPS data',
          validationStats: validationResult.stats,
          flowType: 'legacy'
        };
      }

      logger.info(`✅ Validación completada (legacy): ${validationResult.validData.length} registros válidos`);

      // Paso 4: Subir a BigQuery (simulado con archivo)
      const uploadResult = await this.bigQueryAdapter.uploadData(validationResult.validData);
      
      if (!uploadResult.success) {
        logger.error('❌ Error subiendo datos a BigQuery (legacy):', uploadResult.error);
        return {
          success: false,
          error: uploadResult.error,
          recordsProcessed: 0,
          stage: 'bigquery_upload',
          flowType: 'legacy'
        };
      }

      logger.info(`✅ Datos subidos a BigQuery (legacy): ${uploadResult.recordsProcessed} registros`);

      // Paso 5: Limpiar Redis solo si la subida fue exitosa
      try {
        await this.redisRepo.clearListData(config.gps.listKey);
        logger.info('🗑️ Datos limpiados de Redis después de procesamiento exitoso (legacy)');
      } catch (cleanupError) {
        logger.error('❌ Error limpiando Redis (legacy):', cleanupError.message);
        // No fallar el procesamiento por error de limpieza
      }

      // Recopilar métricas
      const processingTime = Date.now() - startTime;

      await this.metrics.recordProcessing({
        recordsProcessed: uploadResult.recordsProcessed,
        processingTime,
        success: true,
        flowType: 'legacy',
        validationStats: validationResult.stats,
        outputFile: uploadResult.outputFile,
        fileSize: uploadResult.fileSize
      });

      logger.info(`✅ Procesamiento legacy completado: ${uploadResult.recordsProcessed} registros en ${processingTime}ms`);

      return {
        success: true,
        recordsProcessed: uploadResult.recordsProcessed,
        processingTime,
        validationStats: validationResult.stats,
        outputFile: uploadResult.outputFile,
        fileSize: uploadResult.fileSize,
        backupId: uploadResult.backupId,
        stage: 'completed',
        flowType: 'legacy'
      };

    } catch (error) {
      logger.error('❌ Error en procesamiento GPS legacy:', error.message);
      
      await this.metrics.recordProcessing({
        recordsProcessed: 0,
        processingTime: Date.now() - startTime,
        success: false,
        error: error.message,
        flowType: 'legacy'
      });

      return {
        success: false,
        error: error.message,
        recordsProcessed: 0,
        flowType: 'legacy'
      };

    } finally {
      this.isProcessing = false;
    }
  }

  /**
   * Valida datos GPS usando el validador
   */
  async validateGPSData(gpsData) {
    try {
      logger.info(`🔍 Validando ${gpsData.length} registros GPS (legacy)...`);

      const validData = [];
      const invalidData = [];
      const errors = [];

      for (let i = 0; i < gpsData.length; i++) {
        try {
          const record = typeof gpsData[i] === 'string' ? 
            JSON.parse(gpsData[i]) : gpsData[i];

          const validation = this.validator.validateGPSRecord(record);
          
          if (validation.isValid) {
            validData.push(validation.cleanedRecord || record);
          } else {
            invalidData.push({
              index: i,
              record: record,
              errors: validation.errors
            });
            errors.push(...validation.errors);
          }
        } catch (parseError) {
          invalidData.push({
            index: i,
            record: gpsData[i],
            errors: [`Parse error: ${parseError.message}`]
          });
          errors.push(`Parse error at index ${i}: ${parseError.message}`);
        }
      }

      const stats = {
        total: gpsData.length,
        valid: validData.length,
        invalid: invalidData.length,
        validationRate: validData.length / gpsData.length,
        errorCount: errors.length
      };

      if (invalidData.length > 0) {
        logger.warn(`⚠️ Datos inválidos encontrados (legacy): ${invalidData.length}/${gpsData.length}`);
        
        // Log algunos errores de ejemplo
        const sampleErrors = errors.slice(0, 3);
        sampleErrors.forEach(error => logger.warn(`   - ${error}`));
        
        if (errors.length > 3) {
          logger.warn(`   ... y ${errors.length - 3} errores más`);
        }
      }

      return {
        isValid: validData.length > 0,
        validData,
        invalidData,
        errors,
        stats
      };

    } catch (error) {
      logger.error('❌ Error validando datos GPS (legacy):', error.message);
      return {
        isValid: false,
        validData: [],
        invalidData: [],
        errors: [error.message],
        stats: { total: 0, valid: 0, invalid: 0, validationRate: 0, errorCount: 1 }
      };
    }
  }

  /**
   * Obtiene estadísticas del procesador legacy
   */
  async getProcessorStats() {
    try {
      const gpsStats = await this.redisRepo.getGPSStats();
      const adapterStatus = await this.bigQueryAdapter.getStatus();
      const metrics = await this.metrics.getMetrics();

      return {
        redis: {
          gps: gpsStats,
          total: gpsStats.totalRecords
        },
        bigQuery: {
          adapter: adapterStatus,
          simulationMode: true
        },
        metrics: metrics,
        processor: {
          isProcessing: this.isProcessing,
          lastProcessed: metrics.lastProcessing || null,
          flowType: 'legacy'
        }
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas del procesador legacy:', error.message);
      return {
        error: error.message,
        processor: {
          isProcessing: this.isProcessing,
          flowType: 'legacy'
        }
      };
    }
  }

  /**
   * Verifica la salud del procesador legacy
   */
  async healthCheck() {
    try {
      const [redisHealth, adapterStatus] = await Promise.all([
        this.redisRepo.ping(),
        this.bigQueryAdapter.getStatus()
      ]);

      const services = {
        redis: redisHealth ? 'healthy' : 'unhealthy',
        bigQueryAdapter: adapterStatus.initialized ? 'healthy' : 'unhealthy'
      };

      const isHealthy = Object.values(services).every(status => status === 'healthy');

      return {
        healthy: isHealthy,
        services,
        details: {
          bigQueryAdapter: {
            simulationMode: true,
            outputFile: adapterStatus.outputFile,
            backup: adapterStatus.backup
          }
        },
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error en health check legacy:', error.message);
      return {
        healthy: false,
        error: error.message,
        timestamp: new Date().toISOString()
      };
    }
  }

  /**
   * Limpia recursos del procesador legacy
   */
  async cleanup() {
    try {
      logger.info('🧹 Limpiando recursos del procesador GPS legacy...');
      
      await Promise.all([
        this.redisRepo.disconnect(),
        this.metrics.flush()
      ]);
      
      logger.info('✅ Recursos del procesador GPS legacy limpiados exitosamente');
    } catch (error) {
      logger.error('❌ Error limpiando recursos del procesador GPS legacy:', error.message);
    }
  }
}