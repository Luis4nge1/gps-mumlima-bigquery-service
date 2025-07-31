import { createRedisClient } from '../src/config/redis.js';
import { config } from '../src/config/env.js';

async function checkRedisData() {
  const redis = createRedisClient();
  
  try {
    await redis.connect();
    console.log('✅ Conectado a Redis');
    
    // Verificar datos GPS
    const gpsLength = await redis.llen('gps:history:global');
    console.log(`📍 gps:history:global: ${gpsLength} registros`);
    
    // Verificar datos Mobile
    const mobileLength = await redis.llen('mobile:history:global');
    console.log(`📱 mobile:history:global: ${mobileLength} registros`);
    
    // Mostrar algunos ejemplos si hay datos
    if (gpsLength > 0) {
      console.log('\n📍 Ejemplo GPS:');
      const gpsExample = await redis.lrange('gps:history:global', 0, 2);
      gpsExample.forEach((item, i) => {
        console.log(`  ${i + 1}:`, JSON.parse(item));
      });
    }
    
    if (mobileLength > 0) {
      console.log('\n📱 Ejemplo Mobile:');
      const mobileExample = await redis.lrange('mobile:history:global', 0, 2);
      mobileExample.forEach((item, i) => {
        console.log(`  ${i + 1}:`, JSON.parse(item));
      });
    }
    
    // Verificar otras claves relacionadas
    console.log('\n🔍 Buscando otras claves relacionadas...');
    const allKeys = await redis.keys('*history*');
    console.log('Claves encontradas:', allKeys);
    
    await redis.quit();
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    process.exit(1);
  }
}

checkRedisData();