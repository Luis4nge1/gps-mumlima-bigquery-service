#!/usr/bin/env node

/**
 * Test simple de BigQuery
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('🔍 Iniciando test simple de BigQuery...');

// Mostrar variables de entorno relevantes
console.log('📋 Variables de entorno BigQuery:');
console.log('- BIGQUERY_PROJECT_ID:', process.env.BIGQUERY_PROJECT_ID);
console.log('- BIGQUERY_DATASET_ID:', process.env.BIGQUERY_DATASET_ID);
console.log('- BIGQUERY_LOCATION:', process.env.BIGQUERY_LOCATION);
console.log('- BIGQUERY_KEY_FILE:', process.env.BIGQUERY_KEY_FILE);
console.log('- GOOGLE_APPLICATION_CREDENTIALS:', process.env.GOOGLE_APPLICATION_CREDENTIALS);
console.log('- GCP_SIMULATION_MODE:', process.env.GCP_SIMULATION_MODE);
console.log('- BIGQUERY_SIMULATION_MODE:', process.env.BIGQUERY_SIMULATION_MODE);

// Verificar archivo de credenciales
import fs from 'fs/promises';

async function checkCredentials() {
  const keyFile = process.env.BIGQUERY_KEY_FILE || process.env.GOOGLE_APPLICATION_CREDENTIALS || 'service-account.json';
  
  try {
    const stats = await fs.stat(keyFile);
    console.log('✅ Archivo de credenciales encontrado:', keyFile);
    console.log('   Tamaño:', stats.size, 'bytes');
    
    // Leer y verificar estructura básica del JSON
    const content = await fs.readFile(keyFile, 'utf8');
    const credentials = JSON.parse(content);
    
    console.log('✅ Archivo JSON válido');
    console.log('   Tipo:', credentials.type);
    console.log('   Project ID:', credentials.project_id);
    console.log('   Client Email:', credentials.client_email);
    
  } catch (error) {
    console.error('❌ Error con archivo de credenciales:', error.message);
  }
}

// Test de BigQuery
async function testBigQuery() {
  try {
    console.log('\n🔧 Probando inicialización de BigQuery...');
    
    const { BigQueryBatchProcessor } = await import('../src/services/BigQueryBatchProcessor.js');
    const processor = new BigQueryBatchProcessor();
    
    console.log('📊 Configuración del procesador:');
    console.log('- Simulation Mode:', processor.simulationMode);
    console.log('- Project ID:', processor.projectId);
    console.log('- Dataset ID:', processor.datasetId);
    console.log('- Location:', processor.location);
    console.log('- Key Filename:', processor.keyFilename);
    
    await processor.initialize();
    console.log('✅ BigQuery inicializado exitosamente');
    
    const status = await processor.getStatus();
    console.log('📊 Estado:', status);
    
    await processor.cleanup();
    
  } catch (error) {
    console.error('❌ Error en test de BigQuery:', error.message);
    console.error('Stack:', error.stack);
  }
}

async function main() {
  await checkCredentials();
  await testBigQuery();
  console.log('\n🎉 Test completado');
}

main().catch(console.error);