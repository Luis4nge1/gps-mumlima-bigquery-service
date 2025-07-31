import { config } from '../config/env.js';

/**
 * Logger simple y eficiente para el microservicio
 */
class Logger {
  constructor() {
    this.level = config.logging.level || 'info';
    this.levels = {
      error: 0,
      warn: 1,
      info: 2,
      debug: 3
    };
  }

  /**
   * Formatea el mensaje de log
   */
  formatMessage(level, message, meta = null) {
    const timestamp = new Date().toISOString();
    const levelUpper = level.toUpperCase().padEnd(5);
    
    let logMessage = `[${timestamp}] ${levelUpper} ${message}`;
    
    if (meta) {
      if (typeof meta === 'object') {
        logMessage += ` ${JSON.stringify(meta, null, 2)}`;
      } else {
        logMessage += ` ${meta}`;
      }
    }
    
    return logMessage;
  }

  /**
   * Verifica si el nivel de log debe ser mostrado
   */
  shouldLog(level) {
    return this.levels[level] <= this.levels[this.level];
  }

  /**
   * Log de error
   */
  error(message, meta = null) {
    if (this.shouldLog('error')) {
      console.error(this.formatMessage('error', message, meta));
    }
  }

  /**
   * Log de advertencia
   */
  warn(message, meta = null) {
    if (this.shouldLog('warn')) {
      console.warn(this.formatMessage('warn', message, meta));
    }
  }

  /**
   * Log de información
   */
  info(message, meta = null) {
    if (this.shouldLog('info')) {
      console.log(this.formatMessage('info', message, meta));
    }
  }

  /**
   * Log de debug
   */
  debug(message, meta = null) {
    if (this.shouldLog('debug')) {
      console.log(this.formatMessage('debug', message, meta));
    }
  }

  /**
   * Cambia el nivel de logging
   */
  setLevel(level) {
    if (this.levels.hasOwnProperty(level)) {
      this.level = level;
      this.info(`📝 Nivel de logging cambiado a: ${level}`);
    } else {
      this.warn(`⚠️ Nivel de logging inválido: ${level}`);
    }
  }

  /**
   * Obtiene el nivel actual
   */
  getLevel() {
    return this.level;
  }
}

// Exportar instancia singleton
export const logger = new Logger();