#!/usr/bin/env node

/**
 * Script de inicialización del microservicio GPS-BigQuery
 */

import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.join(__dirname, '..');

async function createDirectories() {
  const directories = [
    'tmp',
    'tmp/backup',
    'logs',
    'data',
    'data/backup'
  ];

  console.log('📁 Creando directorios necesarios...');
  
  for (const dir of directories) {
    const fullPath = path.join(projectRoot, dir);
    try {
      await fs.mkdir(fullPath, { recursive: true });
      console.log(`   ✅ ${dir}`);
    } catch (error) {
      console.log(`   ❌ Error creando ${dir}:`, error.message);
    }
  }
}

async function checkEnvironment() {
  console.log('🔍 Verificando entorno...');
  
  // Verificar Node.js version
  const nodeVersion = process.version;
  const majorVersion = parseInt(nodeVersion.slice(1).split('.')[0]);
  
  if (majorVersion < 18) {
    console.log(`   ❌ Node.js ${nodeVersion} detectado. Se requiere >= 18.0.0`);
    process.exit(1);
  } else {
    console.log(`   ✅ Node.js ${nodeVersion}`);
  }

  // Verificar archivo .env
  const envPath = path.join(projectRoot, '.env');
  const envExamplePath = path.join(projectRoot, '.env.example');
  
  try {
    await fs.access(envPath);
    console.log('   ✅ Archivo .env encontrado');
  } catch {
    try {
      await fs.copyFile(envExamplePath, envPath);
      console.log('   ✅ Archivo .env creado desde .env.example');
      console.log('   ⚠️  Recuerda configurar las variables de entorno en .env');
    } catch (error) {
      console.log('   ❌ Error creando .env:', error.message);
    }
  }
}

async function testRedisConnection() {
  console.log('🔗 Probando conexión Redis...');
  
  try {
    const { createRedisClient } = await import('../src/config/redis.js');
    const client = createRedisClient();
    
    await client.connect();
    const result = await client.ping();
    
    if (result === 'PONG') {
      console.log('   ✅ Conexión Redis exitosa');
    } else {
      console.log('   ❌ Respuesta inesperada de Redis:', result);
    }
    
    await client.quit();
    
  } catch (error) {
    console.log('   ❌ Error conectando a Redis:', error.message);
    console.log('   💡 Asegúrate de que Redis esté ejecutándose');
  }
}

async function showConfiguration() {
  console.log('⚙️ Configuración detectada:');
  
  try {
    const { config } = await import('../src/config/env.js');
    
    console.log(`   📊 Redis: ${config.redis.host}:${config.redis.port}/${config.redis.db}`);
    console.log(`   📍 GPS Key: ${config.gps.listKey}`);
    console.log(`   📁 Archivo salida: ${config.gps.outputFilePath}`);
    console.log(`   ⏰ Intervalo: ${config.scheduler.intervalMinutes} minutos`);
    console.log(`   📝 Log level: ${config.logging.level}`);
    console.log(`   🔄 Scheduler: ${config.scheduler.enabled ? 'Habilitado' : 'Deshabilitado'}`);
    
  } catch (error) {
    console.log('   ❌ Error cargando configuración:', error.message);
  }
}

async function main() {
  console.log('🚀 Inicializando GPS BigQuery Microservice...\n');
  
  try {
    await createDirectories();
    console.log();
    
    await checkEnvironment();
    console.log();
    
    await testRedisConnection();
    console.log();
    
    await showConfiguration();
    console.log();
    
    console.log('✅ Inicialización completada!');
    console.log('\n📋 Próximos pasos:');
    console.log('   1. Configurar variables en .env si es necesario');
    console.log('   2. Ejecutar: npm start');
    console.log('   3. Para ejecución única: npm run start:once');
    console.log('   4. Para desarrollo: npm run dev');
    
  } catch (error) {
    console.error('❌ Error durante la inicialización:', error.message);
    process.exit(1);
  }
}

// Ejecutar si es llamado directamente
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}