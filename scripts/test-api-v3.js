#!/usr/bin/env node

/**
 * Script de prueba para la API v3
 * Verifica que todos los endpoints estén funcionando correctamente
 */

import { config } from '../src/config/env.js';

class ApiV3Tester {
  constructor() {
    this.baseUrl = process.env.API_BASE_URL || `http://localhost:${config.server.port}`;
    this.basePath = process.env.API_BASE_PATH || '/api/v3';
    this.fullUrl = `${this.baseUrl}${this.basePath}`;
    this.results = {
      passed: 0,
      failed: 0,
      tests: []
    };
  }

  /**
   * Realiza una petición HTTP
   */
  async makeRequest(endpoint, method = 'GET', data = null, expectedStatus = 200) {
    try {
      const url = `${this.fullUrl}${endpoint}`;
      const options = {
        method,
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'API-v3-Tester/1.0'
        }
      };

      if (data && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(data);
      }

      const response = await fetch(url, options);
      const result = await response.json();

      return {
        success: response.status === expectedStatus,
        status: response.status,
        data: result,
        headers: Object.fromEntries(response.headers.entries())
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Ejecuta un test individual
   */
  async runTest(name, testFn) {
    try {
      console.log(`🧪 Ejecutando: ${name}`);
      const result = await testFn();
      
      if (result.success) {
        console.log(`✅ PASS: ${name}`);
        this.results.passed++;
      } else {
        console.log(`❌ FAIL: ${name} - ${result.error || 'Test failed'}`);
        this.results.failed++;
      }
      
      this.results.tests.push({
        name,
        success: result.success,
        error: result.error,
        details: result.details
      });
      
    } catch (error) {
      console.log(`❌ ERROR: ${name} - ${error.message}`);
      this.results.failed++;
      this.results.tests.push({
        name,
        success: false,
        error: error.message
      });
    }
  }

  /**
   * Test: Verificar que el servidor responde
   */
  async testServerConnection() {
    const response = await this.makeRequest('');
    return {
      success: response.success && response.data.service,
      error: response.error || (!response.data.service ? 'Invalid API response' : null),
      details: { status: response.status }
    };
  }

  /**
   * Test: Verificar headers de API
   */
  async testApiHeaders() {
    const response = await this.makeRequest('/health');
    const hasVersionHeader = response.headers['x-api-version'] === 'v3';
    const hasResponseTime = 'x-response-time' in response.headers;
    
    return {
      success: response.success && hasVersionHeader && hasResponseTime,
      error: !hasVersionHeader ? 'Missing X-API-Version header' : 
             !hasResponseTime ? 'Missing X-Response-Time header' : null,
      details: { 
        headers: {
          'x-api-version': response.headers['x-api-version'],
          'x-response-time': response.headers['x-response-time']
        }
      }
    };
  }

  /**
   * Test: Health check básico
   */
  async testBasicHealth() {
    const response = await this.makeRequest('/health');
    const hasStatus = response.data && response.data.status;
    
    return {
      success: response.success && hasStatus,
      error: !hasStatus ? 'Missing status field' : response.error,
      details: { status: response.data?.status }
    };
  }

  /**
   * Test: Health check detallado
   */
  async testDetailedHealth() {
    const response = await this.makeRequest('/health/detailed');
    const hasServices = response.data && response.data.services;
    
    return {
      success: response.success && hasServices,
      error: !hasServices ? 'Missing services field' : response.error,
      details: { 
        status: response.data?.status,
        serviceCount: Object.keys(response.data?.services || {}).length
      }
    };
  }

  /**
   * Test: Métricas generales
   */
  async testMetrics() {
    const response = await this.makeRequest('/metrics');
    
    return {
      success: response.success,
      error: response.error,
      details: { status: response.status }
    };
  }

  /**
   * Test: Estado del sistema
   */
  async testStatus() {
    const response = await this.makeRequest('/status');
    
    return {
      success: response.success,
      error: response.error,
      details: { status: response.status }
    };
  }

  /**
   * Test: Compatibilidad con rutas legacy
   */
  async testLegacyCompatibility() {
    try {
      // Probar ruta legacy
      const legacyUrl = `${this.baseUrl}/api/massive-data/health`;
      const response = await fetch(legacyUrl);
      const result = await response.json();
      
      return {
        success: response.ok && result.status,
        error: !response.ok ? `Legacy route failed: ${response.status}` : null,
        details: { legacyStatus: response.status }
      };
    } catch (error) {
      return {
        success: false,
        error: error.message
      };
    }
  }

  /**
   * Test: Endpoint no existente (debe devolver 404)
   */
  async testNotFound() {
    const response = await this.makeRequest('/nonexistent', 'GET', null, 404);
    
    return {
      success: response.success && response.status === 404,
      error: response.status !== 404 ? `Expected 404, got ${response.status}` : null,
      details: { status: response.status }
    };
  }

  /**
   * Test: Método no permitido (debe devolver 405)
   */
  async testMethodNotAllowed() {
    const response = await this.makeRequest('/health', 'DELETE', null, 405);
    
    return {
      success: response.success && response.status === 405,
      error: response.status !== 405 ? `Expected 405, got ${response.status}` : null,
      details: { status: response.status }
    };
  }

  /**
   * Test: CORS headers
   */
  async testCorsHeaders() {
    const response = await this.makeRequest('/health');
    const hasCors = response.headers['access-control-allow-origin'] === '*';
    
    return {
      success: response.success && hasCors,
      error: !hasCors ? 'Missing CORS headers' : response.error,
      details: { 
        corsHeader: response.headers['access-control-allow-origin']
      }
    };
  }

  /**
   * Ejecuta todos los tests
   */
  async runAllTests() {
    console.log('🚀 === INICIANDO TESTS API v3 ===');
    console.log(`🔗 URL Base: ${this.fullUrl}\n`);

    // Tests básicos
    await this.runTest('Conexión al servidor', () => this.testServerConnection());
    await this.runTest('Headers de API', () => this.testApiHeaders());
    await this.runTest('CORS Headers', () => this.testCorsHeaders());
    
    // Tests de endpoints
    await this.runTest('Health check básico', () => this.testBasicHealth());
    await this.runTest('Health check detallado', () => this.testDetailedHealth());
    await this.runTest('Métricas generales', () => this.testMetrics());
    await this.runTest('Estado del sistema', () => this.testStatus());
    
    // Tests de compatibilidad y errores
    await this.runTest('Compatibilidad legacy', () => this.testLegacyCompatibility());
    await this.runTest('Endpoint no existente (404)', () => this.testNotFound());
    await this.runTest('Método no permitido (405)', () => this.testMethodNotAllowed());

    // Mostrar resultados
    this.showResults();
  }

  /**
   * Muestra los resultados de los tests
   */
  showResults() {
    console.log('\n📊 === RESULTADOS ===');
    console.log(`✅ Tests pasados: ${this.results.passed}`);
    console.log(`❌ Tests fallidos: ${this.results.failed}`);
    console.log(`📊 Total: ${this.results.passed + this.results.failed}`);
    
    const successRate = (this.results.passed / (this.results.passed + this.results.failed)) * 100;
    console.log(`📈 Tasa de éxito: ${successRate.toFixed(1)}%`);
    
    if (this.results.failed > 0) {
      console.log('\n❌ Tests fallidos:');
      this.results.tests
        .filter(test => !test.success)
        .forEach(test => {
          console.log(`  • ${test.name}: ${test.error}`);
        });
    }
    
    console.log('\n' + (this.results.failed === 0 ? '🎉 Todos los tests pasaron!' : '⚠️ Algunos tests fallaron'));
    
    // Exit code
    process.exit(this.results.failed > 0 ? 1 : 0);
  }
}

// Función principal
async function main() {
  const tester = new ApiV3Tester();
  
  try {
    await tester.runAllTests();
  } catch (error) {
    console.error('❌ Error ejecutando tests:', error.message);
    process.exit(1);
  }
}

// Ejecutar si es el módulo principal
if (process.argv[1].endsWith('test-api-v3.js')) {
  main().catch(error => {
    console.error('❌ Error fatal:', error.message);
    process.exit(1);
  });
}

export { ApiV3Tester };