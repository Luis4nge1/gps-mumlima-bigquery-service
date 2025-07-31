import { config, validateConfig } from './config/env.js';
import { logger } from './utils/logger.js';
import { GPSProcessorService } from './services/GPSProcessorService.js';
import { HybridGPSProcessor } from './services/HybridGPSProcessor.js';
import { migrationConfig } from './config/migrationConfig.js';
import { SchedulerService } from './services/SchedulerService.js';
import { ErrorHandler } from './middleware/ErrorHandler.js';
import { metrics } from './utils/metrics.js';
import { HttpServer } from './api/HttpServer.js';
import { initializeGCP } from './utils/GCPInitializer.js';
import { fileURLToPath } from 'url';

/**
 * Servicio principal del microservicio GPS-BigQuery
 */
class GPSBigQueryService {
  constructor() {
    this.processor = null;
    this.scheduler = null;
    this.metrics = null;
    this.httpServer = null;
    this.isInitialized = false;
  }

  /**
   * Inicializa el servicio
   */
  async initialize() {
    try {
      logger.info('🚀 Inicializando GPS BigQuery Service...');

      // Validar configuración
      validateConfig();

      // Inicializar logger con configuración
      logger.initialize(config);

      // Validar configuración de migración
      try {
        migrationConfig.validateConfig();
      } catch (error) {
        logger.warn(`⚠️ Configuración de migración inválida: ${error.message}`);
      }

      // Inicializar configuración GCP
      const gcpInit = await initializeGCP();
      if (!gcpInit.success) {
        logger.warn(`⚠️  GCP no disponible: ${gcpInit.message}`);
      }

      // Configurar manejadores globales de errores
      ErrorHandler.setupGlobalHandlers();

      // Inicializar métricas (singleton)
      this.metrics = metrics;

      // Crear instancias de servicios según configuración de migración
      const migrationEnabled = migrationConfig.getConfig().migrationEnabled;
      
      if (migrationEnabled) {
        logger.info('🔄 Modo migración habilitado - usando HybridGPSProcessor');
        this.processor = new HybridGPSProcessor();
        await this.processor.initialize();
      } else {
        logger.info('📊 Modo estándar - usando GPSProcessorService');
        this.processor = new GPSProcessorService();
        await this.processor.initialize();
      }
      
      this.scheduler = new SchedulerService();
      this.httpServer = new HttpServer();

      this.isInitialized = true;
      logger.info('✅ Servicio inicializado exitosamente');
      
      // Mostrar configuración
      this.logConfiguration();

    } catch (error) {
      const errorInfo = ErrorHandler.handle(error, 'Service Initialization');
      logger.error('❌ Error al inicializar el servicio:', errorInfo);
      throw error;
    }
  }

  /**
   * Inicia el servicio completo
   */
  async start() {
    try {
      await this.initialize();

      // Verificar salud del sistema antes de iniciar
      const healthCheck = await this.processor.healthCheck();
      if (!healthCheck.healthy) {
        throw new Error(`Health check failed: ${JSON.stringify(healthCheck.services)}`);
      }

      // Ejecutar procesamiento inicial si hay datos
      logger.info('🔄 Ejecutando procesamiento inicial...');
      const initialResult = await this.processor.processGPSData();
      
      if (initialResult.success && initialResult.recordsProcessed > 0) {
        logger.info(`✅ Procesamiento inicial: ${initialResult.recordsProcessed} registros`);
      }

      // Iniciar servidor HTTP para health checks
      this.httpServer.start();

      // Iniciar scheduler automático si está configurado
      if (config.scheduler.enabled) {
        const schedulerStarted = this.scheduler.start();
        if (schedulerStarted) {
          logger.info('📅 Scheduler iniciado exitosamente');
        }
      } else {
        logger.info('📅 Scheduler deshabilitado por configuración');
      }

      // Configurar manejo de señales para cierre graceful
      this.setupGracefulShutdown();

      logger.info('🎯 GPS BigQuery Service ejecutándose exitosamente');

    } catch (error) {
      const errorInfo = ErrorHandler.handle(error, 'Service Start');
      logger.error('❌ Error al iniciar el servicio:', errorInfo);
      await this.shutdown();
      process.exit(1);
    }
  }

  /**
   * Ejecuta un procesamiento manual único
   */
  async runOnce() {
    try {
      await this.initialize();
      
      logger.info('🔄 Ejecutando procesamiento único...');
      const result = await this.processor.processGPSData();
      
      if (result.success) {
        logger.info('✅ Procesamiento único completado exitosamente');
      } else {
        logger.error('❌ Error en procesamiento único:', result.error);
      }

      await this.shutdown();
      return result;

    } catch (error) {
      logger.error('❌ Error en procesamiento único:', error.message);
      await this.shutdown();
      throw error;
    }
  }

  /**
   * Configura el cierre graceful del servicio
   */
  setupGracefulShutdown() {
    const signals = ['SIGINT', 'SIGTERM', 'SIGQUIT'];
    
    signals.forEach(signal => {
      process.on(signal, async () => {
        logger.info(`📡 Señal ${signal} recibida, cerrando servicio...`);
        await this.shutdown();
        process.exit(0);
      });
    });

    process.on('uncaughtException', async (error) => {
      logger.error('❌ Excepción no capturada:', error.message);
      await this.shutdown();
      process.exit(1);
    });

    process.on('unhandledRejection', async (reason) => {
      logger.error('❌ Promesa rechazada no manejada:', reason);
      await this.shutdown();
      process.exit(1);
    });
  }

  /**
   * Cierre graceful del servicio
   */
  async shutdown() {
    try {
      logger.info('🔄 Cerrando GPS BigQuery Service...');

      // Detener servidor HTTP
      if (this.httpServer) {
        await this.httpServer.stop();
      }

      // Detener scheduler
      if (this.scheduler) {
        await this.scheduler.cleanup();
      }

      // Limpiar procesador
      if (this.processor) {
        await this.processor.cleanup();
      }

      // Guardar métricas finales
      if (this.metrics) {
        await this.metrics.flush();
      }

      logger.info('✅ Servicio cerrado exitosamente');

    } catch (error) {
      const errorInfo = ErrorHandler.handle(error, 'Service Shutdown');
      logger.error('❌ Error durante el cierre del servicio:', errorInfo);
    }
  }

  /**
   * Muestra la configuración actual
   */
  logConfiguration() {
    logger.info('⚙️ Configuración del servicio:');
    logger.info(`   📊 Redis: ${config.redis.host}:${config.redis.port}/${config.redis.db}`);
    logger.info(`   📍 GPS Key: ${config.gps.listKey}`);
    logger.info(`   📁 Archivo salida: ${config.gps.outputFilePath}`);
    logger.info(`   ⏰ Intervalo: ${config.scheduler.intervalMinutes} minutos`);
    logger.info(`   📝 Log level: ${config.logging.level}`);
    
    // Mostrar configuración de migración si está habilitada
    const migrationEnabled = migrationConfig.getConfig().migrationEnabled;
    if (migrationEnabled) {
      const migrationStatus = migrationConfig.getStatus();
      logger.info('🔄 Configuración de migración:');
      logger.info(`   📋 Fase actual: ${migrationStatus.currentPhase}`);
      logger.info(`   🆕 Nuevo flujo habilitado: ${migrationStatus.newFlowEnabled}`);
      logger.info(`   🔄 Modo híbrido: ${migrationStatus.hybridMode}`);
      logger.info(`   🔙 Rollback habilitado: ${migrationStatus.rollbackEnabled}`);
      logger.info(`   📊 Comparación habilitada: ${migrationStatus.comparisonEnabled}`);
    }
  }

  /**
   * Obtiene el estado del servicio
   */
  getStatus() {
    return {
      redis: this.redisClient ? 'Conectado' : 'Desconectado',
      processor: this.processor ? 'Listo' : 'No inicializado',
      scheduler: this.scheduler ? this.scheduler.getStatus() : 'No inicializado',
      config: {
        intervalMinutes: config.scheduler.intervalMinutes,
        outputFile: config.gps.outputFilePath,
        redisKey: config.gps.listKey
      }
    };
  }
}

// Función principal
async function main() {
  console.log("Iniciando...")
  const service = new GPSBigQueryService();
  console.log("Saliendo del serice bigquery...")
  // Verificar argumentos de línea de comandos
  const args = process.argv.slice(2);
  
  if (args.includes('--once') || args.includes('-o')) {
    // Ejecutar una sola vez
    await service.runOnce();
  } else {
    // Ejecutar con scheduler
    await service.start();
  }
}

// Ejecutar si es el módulo principal
// if (import.meta.url === `file://${process.argv[1]}`) {
//   main().catch(error => {
//     logger.error('❌ Error fatal:', error.message);
//     process.exit(1);
//   });
// }

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    logger.error('❌ Error fatal:', error.message);
    process.exit(1);
  });
}

export { GPSBigQueryService };