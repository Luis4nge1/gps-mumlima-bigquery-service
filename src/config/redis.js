import Redis from 'ioredis';
import { config } from './env.js';
import { logger } from '../utils/logger.js';

let redisClient = null;

/**
 * Crea y configura el cliente Redis
 */
export function createRedisClient() {
  if (redisClient) {
    return redisClient;
  }

  try {
    const redisConfig = {
      host: config.redis.host,
      port: config.redis.port,
      db: config.redis.db,
      retryDelayOnFailover: config.redis.retryDelayOnFailover,
      maxRetriesPerRequest: config.redis.maxRetriesPerRequest,
      lazyConnect: config.redis.lazyConnect
      // keyPrefix removido porque las claves ya incluyen el prefijo
    };

    // Configuración para Redis Cloud con SSL (opcional)
    if (config.redis.tls || config.redis.host.includes('redislabs.com') || config.redis.host.includes('redis.cloud')) {
      redisConfig.tls = {};
    }

    // Agregar password si está configurado
    if (config.redis.password) {
      redisConfig.password = config.redis.password;
    }

    redisClient = new Redis(redisConfig);

    // Configurar eventos
    redisClient.on('connect', () => {
      logger.info('🔗 Conectando a Redis...');
    });

    redisClient.on('ready', () => {
      logger.info('✅ Redis conectado y listo');
    });

    redisClient.on('error', (error) => {
      logger.error('❌ Error de Redis:', error.message);
    });

    redisClient.on('close', () => {
      logger.info('🔌 Conexión Redis cerrada');
    });

    redisClient.on('reconnecting', (delay) => {
      logger.info(`🔄 Reconectando a Redis en ${delay}ms...`);
    });

    return redisClient;

  } catch (error) {
    logger.error('❌ Error creando cliente Redis:', error.message);
    throw error;
  }
}

/**
 * Cierra la conexión Redis
 */
export async function closeRedisConnection() {
  if (redisClient) {
    try {
      await redisClient.quit();
      redisClient = null;
      logger.info('✅ Conexión Redis cerrada exitosamente');
    } catch (error) {
      logger.error('❌ Error cerrando conexión Redis:', error.message);
      redisClient = null;
    }
  }
}

/**
 * Obtiene el cliente Redis actual
 */
export function getRedisClient() {
  return redisClient;
}

/**
 * Verifica la conexión Redis
 */
export async function pingRedis() {
  try {
    if (!redisClient) {
      throw new Error('Cliente Redis no inicializado');
    }

    const result = await redisClient.ping();
    return result === 'PONG';
  } catch (error) {
    logger.error('❌ Error en ping Redis:', error.message);
    return false;
  }
}