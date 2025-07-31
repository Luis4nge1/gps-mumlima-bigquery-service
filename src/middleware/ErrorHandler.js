import { logger } from '../utils/logger.js';

/**
 * Middleware para manejo centralizado de errores
 */
export class ErrorHandler {
  /**
   * Maneja errores de forma centralizada
   */
  static handle(error, context = 'Unknown') {
    const errorInfo = {
      message: error.message,
      stack: error.stack,
      context,
      timestamp: new Date().toISOString(),
      type: error.constructor.name
    };

    // Log del error
    logger.error(`❌ Error en ${context}:`, errorInfo);

    // Clasificar error
    const classification = this.classifyError(error);
    
    return {
      ...errorInfo,
      classification,
      recoverable: classification.recoverable,
      retryable: classification.retryable
    };
  }

  /**
   * Clasifica el tipo de error
   */
  static classifyError(error) {
    const message = error.message.toLowerCase();
    const type = error.constructor.name;

    // Errores de conexión Redis
    if (message.includes('redis') || message.includes('connection') || type === 'ReplyError') {
      return {
        category: 'redis_connection',
        severity: 'high',
        recoverable: true,
        retryable: true,
        suggestedAction: 'Retry connection to Redis'
      };
    }

    // Errores de validación
    if (message.includes('validation') || message.includes('invalid')) {
      return {
        category: 'validation',
        severity: 'medium',
        recoverable: true,
        retryable: false,
        suggestedAction: 'Check data format and validation rules'
      };
    }

    // Errores de archivo/sistema
    if (message.includes('enoent') || message.includes('file') || message.includes('directory')) {
      return {
        category: 'filesystem',
        severity: 'medium',
        recoverable: true,
        retryable: true,
        suggestedAction: 'Check file permissions and paths'
      };
    }

    // Errores de memoria
    if (message.includes('memory') || message.includes('heap')) {
      return {
        category: 'memory',
        severity: 'critical',
        recoverable: false,
        retryable: false,
        suggestedAction: 'Restart service and check memory usage'
      };
    }

    // Errores de parsing JSON
    if (type === 'SyntaxError' && message.includes('json')) {
      return {
        category: 'json_parse',
        severity: 'medium',
        recoverable: true,
        retryable: false,
        suggestedAction: 'Check JSON format in data'
      };
    }

    // Error genérico
    return {
      category: 'unknown',
      severity: 'medium',
      recoverable: true,
      retryable: true,
      suggestedAction: 'Review error details and context'
    };
  }

  /**
   * Maneja errores con reintentos automáticos
   */
  static async handleWithRetry(operation, maxRetries = 3, delay = 1000, context = 'Operation') {
    let lastError;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const result = await operation();
        
        if (attempt > 1) {
          logger.info(`✅ ${context} exitoso en intento ${attempt}/${maxRetries}`);
        }
        
        return result;

      } catch (error) {
        lastError = error;
        const errorInfo = this.handle(error, context);
        
        if (!errorInfo.retryable || attempt === maxRetries) {
          logger.error(`❌ ${context} falló definitivamente después de ${attempt} intentos`);
          throw error;
        }

        logger.warn(`⚠️ ${context} falló en intento ${attempt}/${maxRetries}, reintentando en ${delay}ms...`);
        
        // Esperar antes del siguiente intento
        await new Promise(resolve => setTimeout(resolve, delay));
        
        // Incrementar delay exponencialmente
        delay *= 2;
      }
    }

    throw lastError;
  }

  /**
   * Crea un wrapper para funciones que pueden fallar
   */
  static createSafeWrapper(fn, context, defaultValue = null) {
    return async (...args) => {
      try {
        return await fn(...args);
      } catch (error) {
        const errorInfo = this.handle(error, context);
        
        if (errorInfo.recoverable) {
          logger.warn(`⚠️ Error recuperable en ${context}, retornando valor por defecto`);
          return defaultValue;
        }
        
        throw error;
      }
    };
  }

  /**
   * Maneja errores de proceso no capturados
   */
  static setupGlobalHandlers() {
    process.on('uncaughtException', (error) => {
      const errorInfo = this.handle(error, 'UncaughtException');
      
      logger.error('💥 Excepción no capturada:', errorInfo);
      
      // Si es crítico, terminar el proceso
      if (errorInfo.classification.severity === 'critical') {
        logger.error('🚨 Error crítico detectado, terminando proceso...');
        process.exit(1);
      }
    });

    process.on('unhandledRejection', (reason, promise) => {
      const error = reason instanceof Error ? reason : new Error(String(reason));
      const errorInfo = this.handle(error, 'UnhandledRejection');
      
      logger.error('💥 Promesa rechazada no manejada:', errorInfo);
      logger.error('Promise:', promise);
    });

    process.on('warning', (warning) => {
      logger.warn('⚠️ Node.js Warning:', {
        name: warning.name,
        message: warning.message,
        stack: warning.stack
      });
    });

    logger.info('🛡️ Manejadores globales de errores configurados');
  }

  /**
   * Crea un circuit breaker simple
   */
  static createCircuitBreaker(operation, options = {}) {
    const {
      failureThreshold = 5,
      resetTimeout = 60000, // 1 minuto
      context = 'CircuitBreaker'
    } = options;

    let failures = 0;
    let lastFailureTime = null;
    let state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN

    return async (...args) => {
      // Si está abierto, verificar si es tiempo de intentar
      if (state === 'OPEN') {
        if (Date.now() - lastFailureTime < resetTimeout) {
          throw new Error(`Circuit breaker is OPEN for ${context}`);
        }
        state = 'HALF_OPEN';
        logger.info(`🔄 Circuit breaker para ${context} cambiado a HALF_OPEN`);
      }

      try {
        const result = await operation(...args);
        
        // Éxito: resetear contador y cerrar circuito
        if (state === 'HALF_OPEN') {
          state = 'CLOSED';
          failures = 0;
          logger.info(`✅ Circuit breaker para ${context} cerrado exitosamente`);
        }
        
        return result;

      } catch (error) {
        failures++;
        lastFailureTime = Date.now();

        const errorInfo = this.handle(error, context);
        
        // Abrir circuito si se alcanza el umbral
        if (failures >= failureThreshold) {
          state = 'OPEN';
          logger.error(`🚨 Circuit breaker para ${context} ABIERTO después de ${failures} fallos`);
        }

        throw error;
      }
    };
  }

  /**
   * Obtiene estadísticas de errores
   */
  static getErrorStats() {
    // Esta implementación básica podría expandirse para mantener estadísticas
    return {
      globalHandlersActive: true,
      timestamp: new Date().toISOString(),
      note: 'Error statistics tracking could be implemented here'
    };
  }
}