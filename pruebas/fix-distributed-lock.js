#!/usr/bin/env node

/**
 * Script para diagnosticar y corregir el problema del DistributedLock
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('🔍 Diagnosticando problema del DistributedLock...');

async function diagnoseLockIssue() {
  try {
    // 1. Verificar configuración de Redis
    console.log('\n📋 Configuración Redis:');
    console.log('- REDIS_HOST:', process.env.REDIS_HOST);
    console.log('- REDIS_PORT:', process.env.REDIS_PORT);
    console.log('- REDIS_PASSWORD:', process.env.REDIS_PASSWORD ? '***' : 'No configurado');
    
    // 2. Probar conexión directa a Redis
    console.log('\n🔗 Probando conexión directa a Redis...');
    const { RedisRepository } = await import('../src/repositories/RedisRepository.js');
    const redisRepo = new RedisRepository();
    
    try {
      await redisRepo.connect();
      console.log('✅ Conexión Redis exitosa');
      
      // 3. Probar el cliente Redis interno
      console.log('\n🔍 Verificando cliente Redis interno...');
      console.log('- redisRepo.redis existe:', !!redisRepo.redis);
      console.log('- redisRepo.redis.set existe:', !!redisRepo.redis?.set);
      
      if (redisRepo.redis && redisRepo.redis.set) {
        // 4. Probar DistributedLock directamente
        console.log('\n🔒 Probando DistributedLock...');
        const { DistributedLock } = await import('../src/utils/DistributedLock.js');
        
        const lock = new DistributedLock(redisRepo.redis, 'test:lock', 10000);
        
        const acquired = await lock.acquire();
        console.log('- Lock adquirido:', acquired);
        
        if (acquired) {
          const released = await lock.release();
          console.log('- Lock liberado:', released);
        }
        
        console.log('✅ DistributedLock funciona correctamente');
      } else {
        console.log('❌ Cliente Redis no tiene método set');
      }
      
      await redisRepo.disconnect();
      
    } catch (redisError) {
      console.error('❌ Error con Redis:', redisError.message);
    }
    
    // 5. Probar GPSProcessorService
    console.log('\n🔧 Probando GPSProcessorService...');
    const { GPSProcessorService } = await import('../src/services/GPSProcessorService.js');
    const processor = new GPSProcessorService();
    
    console.log('- Processor creado');
    console.log('- redisRepo existe:', !!processor.redisRepo);
    console.log('- redisRepo.redis existe antes de init:', !!processor.redisRepo?.redis);
    
    try {
      await processor.initialize();
      console.log('- Processor inicializado');
      console.log('- redisRepo.redis existe después de init:', !!processor.redisRepo?.redis);
      console.log('- redisRepo.redis.set existe:', !!processor.redisRepo?.redis?.set);
      
      await processor.cleanup();
    } catch (processorError) {
      console.error('❌ Error con GPSProcessorService:', processorError.message);
    }
    
  } catch (error) {
    console.error('❌ Error en diagnóstico:', error.message);
    console.error('Stack:', error.stack);
  }
}

diagnoseLockIssue().then(() => {
  console.log('\n🎉 Diagnóstico completado');
  process.exit(0);
}).catch(error => {
  console.error('💥 Error fatal:', error.message);
  process.exit(1);
});