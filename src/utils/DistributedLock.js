import { logger } from './logger.js';

/**
 * Lock distribuido usando Redis para evitar procesamiento concurrente
 */
export class DistributedLock {
  constructor(redisClient, lockKey = 'gps:processing:lock', ttl = 300000) {
    this.redis = redisClient;
    this.lockKey = lockKey;
    this.ttl = ttl; // 5 minutos por defecto
    this.lockValue = null;
  }

  /**
   * Intenta adquirir el lock
   */
  async acquire() {
    try {
      this.lockValue = `${Date.now()}-${Math.random()}`;
      
      const result = await this.redis.set(
        this.lockKey, 
        this.lockValue, 
        'PX', 
        this.ttl, 
        'NX'
      );
      
      if (result === 'OK') {
        logger.debug(`🔒 Lock adquirido: ${this.lockKey}`);
        return true;
      }
      
      logger.debug(`⏳ Lock no disponible: ${this.lockKey}`);
      return false;
      
    } catch (error) {
      logger.error('❌ Error adquiriendo lock:', error.message);
      return false;
    }
  }

  /**
   * Libera el lock
   */
  async release() {
    try {
      if (!this.lockValue) {
        return true;
      }

      const script = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("del", KEYS[1])
        else
          return 0
        end
      `;
      
      const result = await this.redis.eval(script, 1, this.lockKey, this.lockValue);
      
      if (result === 1) {
        logger.debug(`🔓 Lock liberado: ${this.lockKey}`);
        this.lockValue = null;
        return true;
      }
      
      logger.warn(`⚠️ Lock no pudo ser liberado (expiró?): ${this.lockKey}`);
      return false;
      
    } catch (error) {
      logger.error('❌ Error liberando lock:', error.message);
      return false;
    }
  }

  /**
   * Ejecuta función con lock automático
   */
  async withLock(fn, maxWaitMs = 30000) {
    const startTime = Date.now();
    
    while (Date.now() - startTime < maxWaitMs) {
      if (await this.acquire()) {
        try {
          return await fn();
        } finally {
          await this.release();
        }
      }
      
      // Esperar antes de reintentar
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    throw new Error(`No se pudo adquirir lock después de ${maxWaitMs}ms`);
  }
}