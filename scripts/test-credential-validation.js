#!/usr/bin/env node

/**
 * Script para probar la validación de credenciales GCP
 * Uso: node scripts/test-credential-validation.js
 */

import { gcpConfig, getGCPStatus } from '../src/config/gcpConfig.js';
import { GCPInitializer } from '../src/utils/GCPInitializer.js';

async function testCredentialValidation() {
  console.log('🧪 Probando validación de credenciales GCP...\n');

  // Test 1: Validación básica de credenciales
  console.log('📋 Test 1: Validación básica de credenciales');
  const validation = gcpConfig.validateCredentials();
  console.log(`   Resultado: ${validation.valid ? '✅' : '❌'} ${validation.message}`);
  console.log(`   Modo: ${validation.mode}`);
  
  // Test 2: Estado completo de configuración
  console.log('\n📋 Test 2: Estado completo de configuración');
  const status = getGCPStatus();
  console.log(`   Modo simulación: ${status.simulationMode ? '✅' : '❌'}`);
  console.log(`   Credenciales válidas: ${status.credentialsValid ? '✅' : '❌'}`);
  console.log(`   Proyecto: ${status.projectId || 'No configurado'}`);
  console.log(`   Bucket GCS: ${status.gcs.bucketName}`);
  console.log(`   Dataset BigQuery: ${status.bigQuery.datasetId}`);

  // Test 3: Inicialización completa
  console.log('\n📋 Test 3: Inicialización completa');
  const initializer = new GCPInitializer();
  const initResult = await initializer.initialize();
  console.log(`   Inicialización: ${initResult.success ? '✅' : '❌'} ${initResult.message}`);
  
  // Test 4: Fallback graceful
  console.log('\n📋 Test 4: Fallback graceful');
  const fallbackResult = await initializer.gracefulFallback();
  console.log(`   Fallback necesario: ${fallbackResult.fallback ? '✅' : '❌'}`);
  console.log(`   Modo fallback: ${fallbackResult.mode || 'N/A'}`);
  console.log(`   Mensaje: ${fallbackResult.message}`);

  // Test 5: Health status
  console.log('\n📋 Test 5: Health status');
  const healthStatus = initializer.getHealthStatus();
  console.log(`   GCP configurado: ${healthStatus.gcp.configured ? '✅' : '❌'}`);
  console.log(`   Modo: ${healthStatus.gcp.mode}`);
  console.log(`   Credenciales: ${healthStatus.gcp.credentials}`);

  console.log('\n🎉 Pruebas de validación completadas');
}

// Ejecutar pruebas si se llama directamente
testCredentialValidation().catch(error => {
  console.error('❌ Error en las pruebas:', error.message);
  process.exit(1);
});

export { testCredentialValidation };