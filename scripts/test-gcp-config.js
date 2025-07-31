#!/usr/bin/env node

/**
 * Script para probar la configuración de Google Cloud Platform
 */

import { GCPCredentialsValidator } from '../src/utils/GCPCredentialsValidator.js';
import { Storage } from '@google-cloud/storage';
import { BigQuery } from '@google-cloud/bigquery';
import dotenv from 'dotenv';

// Cargar variables de entorno
dotenv.config();

async function testGCPConfiguration() {
  console.log('🧪 Probando configuración de Google Cloud Platform...\n');

  try {
    // 1. Validar credenciales
    console.log('1️⃣ Validando credenciales...');
    const validator = new GCPCredentialsValidator();
    const credentialsResult = await validator.validateCredentials();
    
    if (!credentialsResult.isValid) {
      throw new Error(`Credenciales inválidas: ${credentialsResult.error}`);
    }
    
    console.log('   ✅ Credenciales válidas');
    console.log(`   📋 Proyecto: ${credentialsResult.projectId}`);
    console.log(`   📧 Service Account: ${credentialsResult.clientEmail}\n`);

    // 2. Probar conexión a Google Cloud Storage
    console.log('2️⃣ Probando conexión a Google Cloud Storage...');
    const storage = new Storage({
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
      projectId: process.env.GCP_PROJECT_ID
    });

    try {
      await storage.getBuckets();
      console.log('   ✅ Conexión a GCS exitosa\n');
    } catch (error) {
      console.log(`   ⚠️ Conexión a GCS: ${error.message.split('\n')[0]}\n`);
    }

    // 3. Probar conexión a BigQuery
    console.log('3️⃣ Probando conexión a BigQuery...');
    const bigquery = new BigQuery({
      keyFilename: process.env.GOOGLE_APPLICATION_CREDENTIALS,
      projectId: process.env.GCP_PROJECT_ID
    });

    try {
      await bigquery.getDatasets();
      console.log('   ✅ Conexión a BigQuery exitosa\n');
    } catch (error) {
      console.log(`   ⚠️ Conexión a BigQuery: ${error.message.split('\n')[0]}\n`);
    }

    // 4. Verificar variables de entorno
    console.log('4️⃣ Verificando variables de entorno...');
    const requiredVars = [
      'GOOGLE_APPLICATION_CREDENTIALS',
      'GCP_PROJECT_ID',
      'GCS_BUCKET_NAME',
      'GCS_GPS_PREFIX',
      'GCS_MOBILE_PREFIX',
      'BIGQUERY_DATASET_ID',
      'BIGQUERY_GPS_TABLE_ID',
      'BIGQUERY_MOBILE_TABLE_ID'
    ];

    let allVarsPresent = true;
    for (const varName of requiredVars) {
      const value = process.env[varName];
      if (value) {
        console.log(`   ✅ ${varName}: ${value}`);
      } else {
        console.log(`   ❌ ${varName}: NO DEFINIDA`);
        allVarsPresent = false;
      }
    }

    if (allVarsPresent) {
      console.log('\n🎉 ¡Configuración de GCP completada exitosamente!');
      console.log('\n📋 Resumen de configuración:');
      console.log(`   • Proyecto GCP: ${process.env.GCP_PROJECT_ID}`);
      console.log(`   • Bucket GCS: ${process.env.GCS_BUCKET_NAME}`);
      console.log(`   • Dataset BigQuery: ${process.env.BIGQUERY_DATASET_ID}`);
      console.log(`   • Tabla GPS: ${process.env.BIGQUERY_GPS_TABLE_ID}`);
      console.log(`   • Tabla Mobile: ${process.env.BIGQUERY_MOBILE_TABLE_ID}`);
      console.log(`   • Modo simulación: ${process.env.GCP_SIMULATION_MODE === 'true' ? 'ACTIVADO' : 'DESACTIVADO'}`);
    } else {
      throw new Error('Faltan variables de entorno requeridas');
    }

  } catch (error) {
    console.error('\n❌ Error en la configuración de GCP:', error.message);
    process.exit(1);
  }
}

// Ejecutar el test
testGCPConfiguration();