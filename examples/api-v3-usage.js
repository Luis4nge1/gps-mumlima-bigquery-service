#!/usr/bin/env node

/**
 * Ejemplo de uso de la API v3 del GPS BigQuery Microservice
 * 
 * Este script demuestra cómo interactuar con los diferentes endpoints
 * de la API v3 del microservicio.
 */

import { config } from '../src/config/env.js';

class ApiV3Client {
  constructor(baseUrl = null) {
    this.baseUrl = baseUrl || process.env.API_BASE_URL || `http://localhost:${config.server.port}`;
    this.basePath = process.env.API_BASE_PATH || '/api/v3';
    this.fullUrl = `${this.baseUrl}${this.basePath}`;
  }

  /**
   * Realiza una petición HTTP a la API
   */
  async makeRequest(endpoint, method = 'GET', data = null) {
    try {
      const url = `${this.fullUrl}${endpoint}`;
      const options = {
        method,
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'API-v3-Example/1.0'
        }
      };

      if (data && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(data);
      }

      console.log(`📡 ${method} ${url}`);
      const response = await fetch(url, options);
      const result = await response.json();

      if (!response.ok) {
        throw new Error(`API Error: ${response.status} - ${result.message || 'Unknown error'}`);
      }

      return result;
    } catch (error) {
      if (error.code === 'ECONNREFUSED') {
        throw new Error(`❌ No se puede conectar al servidor en ${this.baseUrl}. ¿Está ejecutándose el servicio?`);
      }
      throw error;
    }
  }

  /**
   * Verifica el estado de salud del sistema
   */
  async checkHealth() {
    console.log('\n🏥 === HEALTH CHECKS ===');
    
    try {
      // Health check básico
      const basicHealth = await this.makeRequest('/health');
      console.log('✅ Health básico:', basicHealth.status);
      
      // Health check detallado
      const detailedHealth = await this.makeRequest('/health/detailed');
      console.log('📊 Health detallado:', {
        status: detailedHealth.status,
        services: Object.keys(detailedHealth.services || {}).length
      });
      
      // Health checks específicos
      try {
        const gcsHealth = await this.makeRequest('/health/gcs');
        console.log('☁️ GCS Health:', gcsHealth.healthy ? '✅' : '❌');
      } catch (error) {
        console.log('☁️ GCS Health: ❌ (No disponible)');
      }
      
      try {
        const bqHealth = await this.makeRequest('/health/bigquery');
        console.log('📊 BigQuery Health:', bqHealth.healthy ? '✅' : '❌');
      } catch (error) {
        console.log('📊 BigQuery Health: ❌ (No disponible)');
      }
      
    } catch (error) {
      console.error('❌ Error en health checks:', error.message);
    }
  }

  /**
   * Obtiene métricas del sistema
   */
  async getMetrics() {
    console.log('\n📈 === MÉTRICAS ===');
    
    try {
      // Métricas generales
      const metrics = await this.makeRequest('/metrics');
      console.log('📊 Métricas generales obtenidas');
      
      // Métricas específicas
      try {
        const gcsMetrics = await this.makeRequest('/metrics/gcs');
        console.log('☁️ Métricas GCS:', {
          service: gcsMetrics.service,
          timestamp: gcsMetrics.timestamp
        });
      } catch (error) {
        console.log('☁️ Métricas GCS: No disponibles');
      }
      
      try {
        const bqMetrics = await this.makeRequest('/metrics/bigquery');
        console.log('📊 Métricas BigQuery:', {
          service: bqMetrics.service,
          timestamp: bqMetrics.timestamp
        });
      } catch (error) {
        console.log('📊 Métricas BigQuery: No disponibles');
      }
      
    } catch (error) {
      console.error('❌ Error obteniendo métricas:', error.message);
    }
  }

  /**
   * Verifica el estado del procesador
   */
  async getStatus() {
    console.log('\n⚙️ === ESTADO DEL SISTEMA ===');
    
    try {
      const status = await this.makeRequest('/status');
      console.log('🔄 Estado del procesador:', {
        scheduler: status.scheduler?.running ? '✅ Activo' : '❌ Inactivo',
        lastExecution: status.lastExecution || 'N/A',
        uptime: status.uptime || 'N/A'
      });
      
    } catch (error) {
      console.error('❌ Error obteniendo estado:', error.message);
    }
  }

  /**
   * Demuestra el sistema híbrido/migración
   */
  async demonstrateHybridSystem() {
    console.log('\n🔄 === SISTEMA HÍBRIDO/MIGRACIÓN ===');
    
    try {
      // Estado híbrido
      const hybridStatus = await this.makeRequest('/hybrid/status');
      console.log('🔄 Estado híbrido:', {
        fase: hybridStatus.migration?.currentPhase || 'N/A',
        nuevoFlujo: hybridStatus.migration?.newFlowEnabled ? '✅' : '❌',
        rollback: hybridStatus.migration?.rollbackEnabled ? '✅' : '❌'
      });
      
      // Métricas híbridas
      try {
        const hybridMetrics = await this.makeRequest('/hybrid/metrics');
        console.log('📊 Métricas híbridas obtenidas');
      } catch (error) {
        console.log('📊 Métricas híbridas: No disponibles');
      }
      
      // Fase actual
      try {
        const currentPhase = await this.makeRequest('/migration/phase');
        console.log('📋 Fase actual:', currentPhase.currentPhase || 'N/A');
      } catch (error) {
        console.log('📋 Fase actual: No disponible');
      }
      
    } catch (error) {
      console.error('❌ Error en sistema híbrido:', error.message);
    }
  }

  /**
   * Ejecuta procesamiento manual (solo si está habilitado)
   */
  async triggerManualProcessing() {
    console.log('\n🚀 === PROCESAMIENTO MANUAL ===');
    
    try {
      console.log('⚠️ Ejecutando procesamiento manual...');
      const result = await this.makeRequest('/process', 'POST');
      
      if (result.success) {
        console.log('✅ Procesamiento ejecutado exitosamente');
        console.log('📊 Resultado:', {
          recordsProcessed: result.recordsProcessed || 'N/A',
          duration: result.duration || 'N/A'
        });
      } else {
        console.log('❌ Error en procesamiento:', result.error);
      }
      
    } catch (error) {
      console.error('❌ Error ejecutando procesamiento:', error.message);
    }
  }

  /**
   * Demuestra información de la API
   */
  async showApiInfo() {
    console.log('\n🌐 === INFORMACIÓN DE LA API ===');
    
    try {
      const apiInfo = await this.makeRequest('');
      console.log('📋 Información de la API:', {
        servicio: apiInfo.service,
        versión: apiInfo.version,
        versionAPI: apiInfo.apiVersion,
        basePath: apiInfo.basePath,
        ambiente: apiInfo.configuration?.environment
      });
      
      console.log('\n📚 Endpoints disponibles:');
      const endpoints = apiInfo.endpoints || {};
      Object.entries(endpoints).slice(0, 5).forEach(([key, value]) => {
        console.log(`  • ${key}: ${value}`);
      });
      console.log(`  ... y ${Object.keys(endpoints).length - 5} más`);
      
    } catch (error) {
      console.error('❌ Error obteniendo información de API:', error.message);
    }
  }

  /**
   * Ejecuta demostración completa
   */
  async runDemo() {
    console.log('🚀 === DEMO API v3 GPS BigQuery Microservice ===');
    console.log(`🔗 URL Base: ${this.fullUrl}`);
    
    await this.showApiInfo();
    await this.checkHealth();
    await this.getMetrics();
    await this.getStatus();
    await this.demonstrateHybridSystem();
    
    // Solo ejecutar procesamiento si se pasa como argumento
    if (process.argv.includes('--process')) {
      await this.triggerManualProcessing();
    } else {
      console.log('\n💡 Tip: Usa --process para ejecutar procesamiento manual');
    }
    
    console.log('\n✅ Demo completada');
  }
}

// Función principal
async function main() {
  const client = new ApiV3Client();
  
  try {
    await client.runDemo();
  } catch (error) {
    console.error('❌ Error en demo:', error.message);
    process.exit(1);
  }
}

// Ejecutar si es el módulo principal
if (process.argv[1].endsWith('api-v3-usage.js')) {
  main().catch(error => {
    console.error('❌ Error fatal:', error.message);
    process.exit(1);
  });
}

export { ApiV3Client };