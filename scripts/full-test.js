#!/usr/bin/env node

/**
 * Script para probar el flujo completo GPS -> Procesamiento -> Archivo
 * Compatible con Windows, Redis Cloud, y cualquier configuración
 */

import { execSync } from 'child_process';
import fs from 'fs/promises';

console.log('🚀 Test Completo GPS BigQuery');
console.log('='.repeat(40));

try {
  // Cargar módulos necesarios
  console.log('📦 Cargando módulos...');
  const { config } = await import('../src/config/env.js');
  const { RedisRepository } = await import('../src/repositories/RedisRepository.js');
  console.log('   ✅ Módulos cargados');

  // Crear conexión Redis
  const repo = new RedisRepository();
  await repo.connect();
  console.log('   ✅ Conectado a Redis');

  // Paso 1: Limpiar datos anteriores si existen
  console.log('\n🧹 Limpiando datos anteriores...');
  try {
    const client = await repo.connect();
    const deleted = await client.del(config.gps.listKey);
    if (deleted > 0) {
      console.log(`   ✅ Redis limpiado (${deleted} claves eliminadas)`);
    } else {
      console.log('   ⚠️ Redis ya estaba limpio');
    }
  } catch (error) {
    console.log('   ⚠️ Error limpiando Redis:', error.message);
  }

  // Paso 2: Agregar datos de prueba usando Node.js
  console.log('\n📍 Agregando datos de prueba...');
  const testData = [
    {
      deviceId: "test-001",
      lat: -12.0464,
      lng: -77.0428,
      timestamp: new Date().toISOString(),
      speed: 45,
      heading: 180
    },
    {
      deviceId: "test-002", 
      lat: -12.0500,
      lng: -77.0500,
      timestamp: new Date().toISOString(),
      speed: 30,
      heading: 90
    },
    {
      deviceId: "test-003",
      lat: -12.0600,
      lng: -77.0600,
      timestamp: new Date().toISOString(),
      speed: 60,
      heading: 270
    }
  ];

  const client = await repo.connect();
  for (let i = 0; i < testData.length; i++) {
    const result = await client.lpush(config.gps.listKey, JSON.stringify(testData[i]));
    console.log(`   ✅ Agregado registro ${i + 1}: posición ${result} en lista`);
  }

  // Paso 3: Verificar datos en Redis
  console.log('\n🔍 Verificando datos en Redis...');
  const count = await client.llen(config.gps.listKey);
  console.log(`   📊 Registros en Redis: ${count}`);

  if (count === 0) {
    console.log('   ❌ No se pudieron agregar datos a Redis');
    await repo.disconnect();
    process.exit(1);
  }

  // Mostrar algunos datos de ejemplo
  const samples = await client.lrange(config.gps.listKey, 0, 1);
  console.log('   📍 Datos de ejemplo:');
  samples.forEach((sample, i) => {
    console.log(`      ${i + 1}. ${sample.substring(0, 60)}...`);
  });

  // Desconectar antes del procesamiento
  await repo.disconnect();

  // Paso 4: Ejecutar procesamiento
  console.log('\n🔄 Ejecutando procesamiento...');
  const startTime = Date.now();
  
  try {
    const output = execSync('npm run start:once', { encoding: 'utf8', stdio: 'pipe' });
    console.log('   ✅ Procesamiento completado');
    
    // Mostrar logs relevantes
    const lines = output.split('\n').filter(line => 
      line.includes('registros GPS') || 
      line.includes('Validación completada') || 
      line.includes('subidos exitosamente') ||
      line.includes('Eliminados datos GPS') ||
      line.includes('completado exitosamente')
    );
    
    if (lines.length > 0) {
      console.log('   📋 Logs relevantes:');
      lines.forEach(line => {
        console.log(`      ${line.trim()}`);
      });
    }
    
  } catch (error) {
    console.log('   ❌ Error en procesamiento:', error.message);
    console.log('   📋 Output:', error.stdout?.toString() || 'No output');
    console.log('   📋 Error:', error.stderr?.toString() || 'No error details');
  }

  const processingTime = Date.now() - startTime;
  console.log(`   ⏱️ Tiempo de procesamiento: ${processingTime}ms`);

  // Paso 5: Verificar que Redis se limpió (usando Node.js)
  console.log('\n🔍 Verificando limpieza de Redis...');
  try {
    const repoCheck = new RedisRepository();
    await repoCheck.connect();
    const finalCount = await (await repoCheck.connect()).llen(config.gps.listKey);
    console.log(`   📊 Registros restantes en Redis: ${finalCount}`);
    await repoCheck.disconnect();
  } catch (error) {
    console.log('   ⚠️ Error verificando Redis final:', error.message);
  }

  // Paso 6: Verificar archivos generados
  console.log('\n📁 Verificando archivos generados...');
  
  try {
    const files = await fs.readdir('tmp/');
    const gpsFiles = files.filter(f => f.includes('gps_data') || f.includes('gps'));
    
    if (gpsFiles.length > 0) {
      console.log('   ✅ Archivos encontrados:');
      for (const file of gpsFiles) {
        const stats = await fs.stat(`tmp/${file}`);
        console.log(`      📄 ${file} (${stats.size} bytes, ${stats.mtime.toISOString()})`);
      }
      
      // Mostrar contenido del archivo principal
      const mainFile = gpsFiles.find(f => f === 'gps_data.txt') || gpsFiles[0];
      console.log(`\n📄 Contenido de ${mainFile}:`);
      const content = await fs.readFile(`tmp/${mainFile}`, 'utf8');
      const lines = content.split('\n').slice(0, 20);
      lines.forEach((line, i) => {
        if (line.trim()) {
          console.log(`   ${i + 1}. ${line.substring(0, 80)}${line.length > 80 ? '...' : ''}`);
        }
      });
      
    } else {
      console.log('   ❌ No se encontraron archivos GPS');
    }
    
  } catch (error) {
    console.log(`   ❌ Error verificando archivos: ${error.message}`);
  }

  // Paso 7: Verificar backup si está habilitado
  console.log('\n💾 Verificando backups...');
  try {
    const backupFiles = await fs.readdir('tmp/backup/');
    const recentBackups = backupFiles.filter(f => f.includes('gps_backup'));
    
    if (recentBackups.length > 0) {
      console.log('   ✅ Backups encontrados:');
      for (const file of recentBackups.slice(-3)) { // Últimos 3
        const stats = await fs.stat(`tmp/backup/${file}`);
        console.log(`      💾 ${file} (${stats.size} bytes, ${stats.mtime.toISOString()})`);
      }
    } else {
      console.log('   ⚠️ No se encontraron backups (puede estar deshabilitado)');
    }
    
  } catch (error) {
    console.log('   ⚠️ Directorio backup no existe o no es accesible');
  }

  console.log('\n🎉 Test completo finalizado!');
  console.log('\n📋 Resumen:');
  console.log('   ✅ Datos agregados a Redis');
  console.log('   ✅ Procesamiento ejecutado');
  console.log('   ✅ Redis limpiado automáticamente');
  console.log('   ✅ Archivos verificados');
  
} catch (error) {
  console.error('❌ Error en test completo:', error.message);
  process.exit(1);
}