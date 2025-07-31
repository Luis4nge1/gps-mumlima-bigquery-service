import { createRedisClient } from '../config/redis.js';
import { logger } from '../utils/logger.js';
import { config } from '../config/env.js';

/**
 * Repositorio para operaciones con Redis
 */
export class RedisRepository {
  constructor() {
    this.client = null;
    this.isConnected = false;
  }

  /**
   * Getter para acceso al cliente Redis (para DistributedLock)
   */
  get redis() {
    return this.client;
  }

  /**
   * Conecta a Redis
   */
  async connect() {
    try {
      if (!this.client) {
        this.client = createRedisClient();
      }

      // Verificar si ya está conectado
      if (this.client.status === 'ready') {
        this.isConnected = true;
        return this.client;
      }

      // Solo conectar si no está conectado
      if (!this.isConnected && this.client.status !== 'connecting') {
        await this.client.connect();
        this.isConnected = true;
        logger.info('✅ Conectado a Redis exitosamente');
      }

      return this.client;
    } catch (error) {
      // Si el error es que ya está conectado, no es realmente un error
      if (error.message.includes('already connecting') || error.message.includes('already connected')) {
        logger.debug('🔗 Redis ya está conectado, reutilizando conexión');
        this.isConnected = true;
        return this.client;
      }
      
      logger.error('❌ Error conectando a Redis:', error.message);
      throw error;
    }
  }

  /**
   * Desconecta de Redis
   */
  async disconnect() {
    try {
      if (this.client && this.isConnected) {
        // Solo desconectar si realmente está conectado
        if (this.client.status === 'ready') {
          await this.client.quit();
        }
        this.isConnected = false;
        logger.info('✅ Desconectado de Redis');
      }
    } catch (error) {
      // Ignorar errores de desconexión si ya está desconectado
      if (!error.message.includes('Connection is closed')) {
        logger.error('❌ Error desconectando de Redis:', error.message);
      }
    }
  }

  /**
   * Obtiene todos los datos GPS del historial global
   */
  async getAllGPSData() {
    try {
      await this.connect();
      
      const key = config.gps.listKey;
      const length = await this.client.llen(key);
      
      if (length === 0) {
        logger.info('📍 No hay datos GPS en Redis');
        return [];
      }

      // Obtener todos los elementos de la lista
      const data = await this.client.lrange(key, 0, -1);
      
      logger.info(`📍 Obtenidos ${data.length} registros GPS de Redis`);
      return data.map(item => {
        try {
          return JSON.parse(item);
        } catch (parseError) {
          logger.warn('⚠️ Error parseando dato GPS:', parseError.message);
          return item; // Retornar como string si no se puede parsear
        }
      });

    } catch (error) {
      logger.error('❌ Error obteniendo datos GPS:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene datos GPS en lotes
   */
  async getGPSDataBatch(start = 0, count = null) {
    try {
      await this.connect();
      
      const key = config.gps.listKey;
      const batchSize = count || config.gps.batchSize;
      const end = start + batchSize - 1;
      
      const data = await this.client.lrange(key, start, end);
      
      return data.map(item => {
        try {
          return JSON.parse(item);
        } catch (parseError) {
          logger.warn('⚠️ Error parseando dato GPS en lote:', parseError.message);
          return item;
        }
      });

    } catch (error) {
      logger.error('❌ Error obteniendo lote de datos GPS:', error.message);
      throw error;
    }
  }

  /**
   * Elimina todos los datos GPS después de procesarlos
   */
  async clearGPSData() {
    try {
      await this.connect();
      
      const key = config.gps.listKey;
      const deletedCount = await this.client.del(key);
      
      logger.info(`🗑️ Eliminados datos GPS de Redis (${deletedCount} claves)`);
      return deletedCount > 0;

    } catch (error) {
      logger.error('❌ Error eliminando datos GPS:', error.message);
      throw error;
    }
  }

  /**
   * Elimina un rango específico de datos GPS
   */
  async clearGPSDataRange(start, end) {
    try {
      await this.connect();
      
      const key = config.gps.listKey;
      
      // Eliminar elementos del rango especificado
      for (let i = 0; i < (end - start + 1); i++) {
        await this.client.lpop(key);
      }
      
      logger.info(`🗑️ Eliminado rango ${start}-${end} de datos GPS`);
      return true;

    } catch (error) {
      logger.error('❌ Error eliminando rango de datos GPS:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas de los datos GPS
   */
  async getGPSStats() {
    try {
      await this.connect();
      
      const key = config.gps.listKey;
      const length = await this.client.llen(key);
      const memoryUsage = await this.client.memory('usage', key).catch(() => 0);
      
      return {
        totalRecords: length,
        memoryUsage: memoryUsage,
        key: key,
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas GPS:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene estadísticas de los datos Mobile
   */
  async getMobileStats() {
    try {
      await this.connect();
      
      const key = 'mobile:history:global';
      const length = await this.client.llen(key);
      const memoryUsage = await this.client.memory('usage', key).catch(() => 0);
      
      return {
        totalRecords: length,
        memoryUsage: memoryUsage,
        key: key,
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      logger.error('❌ Error obteniendo estadísticas Mobile:', error.message);
      throw error;
    }
  }

  /**
   * Verifica la conexión a Redis
   */
  async ping() {
    try {
      await this.connect();
      const result = await this.client.ping();
      return result === 'PONG';
    } catch (error) {
      logger.error('❌ Error en ping a Redis:', error.message);
      return false;
    }
  }

  /**
   * Obtiene datos de una lista específica de Redis
   * @param {string} key - Clave de la lista en Redis
   * @returns {Array} Datos de la lista
   */
  async getListData(key) {
    try {
      await this.connect();
      
      const length = await this.client.llen(key);
      
      if (length === 0) {
        logger.info(`📍 No hay datos en la lista ${key}`);
        return [];
      }

      // Obtener todos los elementos de la lista
      const data = await this.client.lrange(key, 0, -1);
      
      logger.info(`📍 Obtenidos ${data.length} registros de ${key}`);
      return data.map(item => {
        try {
          return JSON.parse(item);
        } catch (parseError) {
          logger.warn(`⚠️ Error parseando dato de ${key}:`, parseError.message);
          return item; // Retornar como string si no se puede parsear
        }
      });

    } catch (error) {
      logger.error(`❌ Error obteniendo datos de ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Limpia una lista específica de Redis
   * @param {string} key - Clave de la lista en Redis
   * @returns {boolean} True si se eliminó exitosamente
   */
  async clearListData(key) {
    try {
      await this.connect();
      
      const deletedCount = await this.client.del(key);
      
      logger.info(`🗑️ Eliminada lista ${key} de Redis (${deletedCount} claves)`);
      return deletedCount > 0;

    } catch (error) {
      logger.error(`❌ Error eliminando lista ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Agrega un elemento a una lista de Redis
   * @param {string} key - Clave de la lista en Redis
   * @param {string} value - Valor a agregar (debe ser string o se convertirá a JSON)
   * @returns {number} Longitud de la lista después de la inserción
   */
  async addToList(key, value) {
    try {
      await this.connect();
      
      // Convertir a string si es necesario
      const stringValue = typeof value === 'string' ? value : JSON.stringify(value);
      
      // Agregar al final de la lista (RPUSH)
      const newLength = await this.client.rpush(key, stringValue);
      
      logger.debug(`📝 Agregado elemento a ${key}, nueva longitud: ${newLength}`);
      return newLength;

    } catch (error) {
      logger.error(`❌ Error agregando elemento a ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Agrega múltiples elementos a una lista de Redis
   * @param {string} key - Clave de la lista en Redis
   * @param {Array} values - Array de valores a agregar
   * @returns {number} Longitud de la lista después de las inserciones
   */
  async addMultipleToList(key, values) {
    try {
      await this.connect();
      
      if (!Array.isArray(values) || values.length === 0) {
        return await this.client.llen(key);
      }
      
      // Convertir todos los valores a strings
      const stringValues = values.map(value => 
        typeof value === 'string' ? value : JSON.stringify(value)
      );
      
      // Agregar todos los elementos de una vez
      const newLength = await this.client.rpush(key, ...stringValues);
      
      logger.info(`📝 Agregados ${values.length} elementos a ${key}, nueva longitud: ${newLength}`);
      return newLength;

    } catch (error) {
      logger.error(`❌ Error agregando elementos a ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Obtiene información del servidor Redis
   */
  async getServerInfo() {
    try {
      await this.connect();
      const info = await this.client.info();
      return info;
    } catch (error) {
      logger.error('❌ Error obteniendo info del servidor Redis:', error.message);
      throw error;
    }
  }

  /**
   * Obtiene la longitud de una lista
   * @param {string} key - Clave de la lista en Redis
   * @returns {number} Longitud de la lista
   */
  async getListLength(key) {
    try {
      await this.connect();
      const length = await this.client.llen(key);
      return length;
    } catch (error) {
      logger.error(`❌ Error obteniendo longitud de ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Obtiene un lote de elementos de una lista
   * @param {string} key - Clave de la lista en Redis
   * @param {number} count - Número de elementos a obtener
   * @returns {Array} Array de elementos parseados
   */
  async getBatch(key, count) {
    try {
      await this.connect();
      
      const data = await this.client.lrange(key, 0, count - 1);
      
      return data.map(item => {
        try {
          return JSON.parse(item);
        } catch (parseError) {
          logger.warn(`⚠️ Error parseando elemento de ${key}:`, parseError.message);
          return item;
        }
      });

    } catch (error) {
      logger.error(`❌ Error obteniendo lote de ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Remueve un número específico de elementos del inicio de una lista
   * @param {string} key - Clave de la lista en Redis
   * @param {number} count - Número de elementos a remover
   * @returns {number} Número de elementos removidos
   */
  async removeBatch(key, count) {
    try {
      await this.connect();
      
      let removedCount = 0;
      for (let i = 0; i < count; i++) {
        const result = await this.client.lpop(key);
        if (result !== null) {
          removedCount++;
        } else {
          break; // No hay más elementos
        }
      }
      
      logger.info(`🗑️ Removidos ${removedCount} elementos de ${key}`);
      return removedCount;

    } catch (error) {
      logger.error(`❌ Error removiendo lote de ${key}:`, error.message);
      throw error;
    }
  }

  /**
   * Inicializa la conexión (alias para connect)
   */
  async initialize() {
    return await this.connect();
  }
}