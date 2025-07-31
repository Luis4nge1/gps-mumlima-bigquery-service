#!/usr/bin/env node

/**
 * Script para validar la configuración de GCP
 * Uso: node scripts/validate-gcp-setup.js
 */

import { gcpConfig, getGCPStatus } from '../src/config/gcpConfig.js';
import { config } from '../src/config/env.js';

function printHeader(title) {
  console.log('\n' + '='.repeat(50));
  console.log(`  ${title}`);
  console.log('='.repeat(50));
}

function printSection(title, data) {
  console.log(`\n📋 ${title}:`);
  Object.entries(data).forEach(([key, value]) => {
    if (typeof value === 'object' && value !== null) {
      console.log(`  ${key}:`);
      Object.entries(value).forEach(([subKey, subValue]) => {
        console.log(`    ${subKey}: ${subValue}`);
      });
    } else {
      console.log(`  ${key}: ${value}`);
    }
  });
}

async function validateGCPSetup() {
  try {
    printHeader('VALIDACIÓN DE CONFIGURACIÓN GCP');

    // Obtener estado de configuración
    const status = getGCPStatus();
    
    // Mostrar información general
    printSection('Estado General', {
      'Modo Simulación': status.simulationMode ? '✅ Activado' : '❌ Desactivado',
      'Credenciales Válidas': status.credentialsValid ? '✅ Sí' : '❌ No',
      'Modo de Credenciales': status.credentialsMode,
      'Mensaje': status.credentialsMessage,
      'Ruta Service Account': status.serviceAccountPath || 'No encontrado'
    });

    // Mostrar configuración de proyecto
    printSection('Configuración de Proyecto', {
      'Project ID': status.projectId || 'No configurado'
    });

    // Mostrar configuración de GCS
    printSection('Google Cloud Storage', status.gcs);

    // Mostrar configuración de BigQuery
    printSection('BigQuery', status.bigQuery);

    // Validar directorios de simulación si está activado
    if (status.simulationMode) {
      console.log('\n🔧 Modo Simulación Activado');
      gcpConfig.initializeSimulationDirectories();
      
      const simulationConfig = config.gcp.simulation;
      printSection('Directorios de Simulación', {
        'GCS Local': simulationConfig.paths.gcs,
        'BigQuery Local': simulationConfig.paths.bigQuery
      });
    }

    // Mostrar variables de entorno relevantes
    printHeader('VARIABLES DE ENTORNO DETECTADAS');
    
    const envVars = {
      'GCP_SIMULATION_MODE': process.env.GCP_SIMULATION_MODE || 'false',
      'GOOGLE_APPLICATION_CREDENTIALS': process.env.GOOGLE_APPLICATION_CREDENTIALS || 'No configurado',
      'GCP_PROJECT_ID': process.env.GCP_PROJECT_ID || 'No configurado',
      'GCS_BUCKET_NAME': process.env.GCS_BUCKET_NAME || 'Valor por defecto',
      'GCS_REGION': process.env.GCS_REGION || 'Valor por defecto',
      'BIGQUERY_DATASET_ID': process.env.BIGQUERY_DATASET_ID || 'Valor por defecto',
      'BIGQUERY_LOCATION': process.env.BIGQUERY_LOCATION || 'Valor por defecto'
    };

    Object.entries(envVars).forEach(([key, value]) => {
      const status = value === 'No configurado' ? '❌' : '✅';
      console.log(`  ${status} ${key}: ${value}`);
    });

    // Validación final
    printHeader('RESULTADO DE VALIDACIÓN');
    
    if (status.credentialsValid || status.simulationMode) {
      console.log('✅ Configuración GCP válida y lista para usar');
      
      if (status.simulationMode) {
        console.log('🔧 Ejecutándose en modo simulación - no se conectará a GCP real');
      } else {
        console.log('🌐 Configurado para conectar a GCP en producción');
      }
      
      process.exit(0);
    } else {
      console.log('❌ Configuración GCP inválida');
      console.log('\n💡 Para solucionar:');
      console.log('   1. Coloca tu service-account.json en la raíz del proyecto');
      console.log('   2. O configura GOOGLE_APPLICATION_CREDENTIALS');
      console.log('   3. O activa el modo simulación con GCP_SIMULATION_MODE=true');
      
      process.exit(1);
    }

  } catch (error) {
    console.error('\n❌ Error durante la validación:', error.message);
    process.exit(1);
  }
}

// Ejecutar validación si se llama directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  validateGCPSetup();
}

export { validateGCPSetup };