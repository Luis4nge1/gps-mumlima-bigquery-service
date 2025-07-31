#!/usr/bin/env node

/**
 * Script para validar la configuración de backup local
 */

import { config, validateConfig } from '../src/config/env.js';
import { logger } from '../src/utils/logger.js';
import fs from 'fs';
import path from 'path';

async function validateBackupConfiguration() {
  console.log('🔍 Validando configuración de backup local...\n');

  try {
    // Validar configuración general
    validateConfig();
    console.log('✅ Configuración general válida');

    // Mostrar configuración de backup
    const backupConfig = config.backup;
    console.log('\n📋 Configuración de Backup Local:');
    console.log(`   Max Retries: ${backupConfig.maxRetries}`);
    console.log(`   Retention Hours: ${backupConfig.retentionHours}`);
    console.log(`   Storage Path: ${backupConfig.storagePath}`);
    console.log(`   Cleanup Interval: ${backupConfig.cleanupIntervalMinutes} minutos`);
    console.log(`   Atomic Processing: ${backupConfig.atomicProcessingEnabled ? 'Habilitado' : 'Deshabilitado'}`);
    console.log(`   Processing Timeout: ${backupConfig.atomicProcessingTimeoutMs}ms`);

    // Validar directorio de backup
    const backupDir = path.resolve(backupConfig.storagePath);
    console.log(`\n📁 Validando directorio de backup: ${backupDir}`);
    
    if (!fs.existsSync(backupDir)) {
      console.log('⚠️  Directorio no existe, creándolo...');
      fs.mkdirSync(backupDir, { recursive: true });
      console.log('✅ Directorio creado exitosamente');
    } else {
      console.log('✅ Directorio existe');
    }

    // Verificar permisos de escritura
    try {
      const testFile = path.join(backupDir, 'test-write.tmp');
      fs.writeFileSync(testFile, 'test');
      fs.unlinkSync(testFile);
      console.log('✅ Permisos de escritura verificados');
    } catch (error) {
      throw new Error(`Sin permisos de escritura en ${backupDir}: ${error.message}`);
    }

    // Validar valores de configuración
    console.log('\n🔧 Validando valores de configuración:');
    
    if (backupConfig.maxRetries >= 0) {
      console.log(`✅ Max Retries válido: ${backupConfig.maxRetries}`);
    } else {
      throw new Error('Max Retries debe ser >= 0');
    }

    if (backupConfig.retentionHours > 0) {
      console.log(`✅ Retention Hours válido: ${backupConfig.retentionHours}`);
    } else {
      throw new Error('Retention Hours debe ser > 0');
    }

    if (backupConfig.cleanupIntervalMinutes > 0) {
      console.log(`✅ Cleanup Interval válido: ${backupConfig.cleanupIntervalMinutes}`);
    } else {
      throw new Error('Cleanup Interval debe ser > 0');
    }

    if (backupConfig.atomicProcessingTimeoutMs > 0) {
      console.log(`✅ Processing Timeout válido: ${backupConfig.atomicProcessingTimeoutMs}ms`);
    } else {
      throw new Error('Processing Timeout debe ser > 0');
    }

    // Mostrar advertencias si es necesario
    console.log('\n⚠️  Advertencias:');
    if (!backupConfig.atomicProcessingEnabled) {
      console.log('   - Procesamiento atómico deshabilitado (ATOMIC_PROCESSING_ENABLED=false)');
    }
    
    if (backupConfig.maxRetries === 0) {
      console.log('   - Sin reintentos configurados (BACKUP_MAX_RETRIES=0)');
    }

    if (backupConfig.retentionHours < 24) {
      console.log('   - Retención menor a 24 horas puede causar pérdida de backups');
    }

    console.log('\n🎉 Configuración de backup local válida y lista para usar!');

  } catch (error) {
    console.error('\n❌ Error en configuración de backup:', error.message);
    process.exit(1);
  }
}

// Ejecutar validación
validateBackupConfiguration().catch(error => {
  console.error('❌ Error ejecutando validación:', error.message);
  process.exit(1);
});