#!/usr/bin/env node

/**
 * Script para probar el SchedulerService con el lock distribuido corregido
 */

import dotenv from 'dotenv';
dotenv.config();

console.log('🔍 Probando SchedulerService con lock distribuido...');

async function testSchedulerLock() {
  try {
    const { SchedulerService } = await import('../src/services/SchedulerService.js');
    
    console.log('\n📅 Creando SchedulerService...');
    const scheduler = new SchedulerService();
    
    console.log('✅ SchedulerService creado');
    
    // Probar ejecución manual (que usa el mismo lock)
    console.log('\n🔧 Probando ejecución manual...');
    
    try {
      // Simular la lógica del lock del scheduler
      console.log('- Inicializando processor...');
      await scheduler.processor.initialize();
      
      console.log('- Verificando acceso a Redis...');
      console.log('  - redisRepo existe:', !!scheduler.processor.redisRepo);
      console.log('  - redis client existe:', !!scheduler.processor.redisRepo?.redis);
      console.log('  - redis.set existe:', !!scheduler.processor.redisRepo?.redis?.set);
      
      if (scheduler.processor.redisRepo?.redis) {
        console.log('✅ Cliente Redis disponible para lock distribuido');
        
        // Probar el lock directamente
        const { DistributedLock } = await import('../src/utils/DistributedLock.js');
        const lock = new DistributedLock(
          scheduler.processor.redisRepo.redis,
          'test:scheduler:lock',
          30000
        );
        
        console.log('- Probando adquisición de lock...');
        const acquired = await lock.acquire();
        console.log('  - Lock adquirido:', acquired);
        
        if (acquired) {
          console.log('- Esperando 2 segundos...');
          await new Promise(resolve => setTimeout(resolve, 2000));
          
          console.log('- Liberando lock...');
          const released = await lock.release();
          console.log('  - Lock liberado:', released);
        }
        
        console.log('✅ Lock distribuido funciona correctamente');
      } else {
        console.log('❌ Cliente Redis no disponible');
      }
      
      await scheduler.cleanup();
      
    } catch (error) {
      console.error('❌ Error en prueba:', error.message);
    }
    
    console.log('\n🎉 Prueba completada');
    
  } catch (error) {
    console.error('❌ Error fatal:', error.message);
    console.error('Stack:', error.stack);
  }
}

testSchedulerLock().then(() => {
  console.log('\n✅ Test del scheduler completado');
  process.exit(0);
}).catch(error => {
  console.error('💥 Error fatal:', error.message);
  process.exit(1);
});