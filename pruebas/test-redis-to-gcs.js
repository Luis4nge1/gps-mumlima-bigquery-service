import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import dotenv from 'dotenv';

dotenv.config();

async function testRedisToGCS() {
    console.log('🔄 Probando flujo Redis → GCS con keys del sistema real');
    console.log('📋 Usando keys: gps:history:global y mobile:history:global\n');
    
    let redisRepo, gcsAdapter;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        
        await redisRepo.initialize();
        console.log('✅ Redis inicializado');
        console.log('✅ GCS Adapter listo');
        
        // 2. Agregar datos de prueba a las keys del sistema
        console.log('\n📋 Paso 2: Agregando datos de prueba a Redis...');
        await addTestDataToRedis(redisRepo);
        
        // 3. Procesar datos GPS
        console.log('\n🔄 === PROCESANDO DATOS GPS ===');
        await processGPSData(redisRepo, gcsAdapter);
        
        // 4. Procesar datos Mobile
        console.log('\n🔄 === PROCESANDO DATOS MOBILE ===');
        await processMobileData(redisRepo, gcsAdapter);
        
        // 5. Verificar estado final
        console.log('\n📊 === VERIFICACIÓN FINAL ===');
        await verifyFinalState(redisRepo, gcsAdapter);
        
        console.log('\n🎉 ¡Prueba Redis → GCS completada exitosamente!');
        
    } catch (error) {
        console.error('❌ Error en la prueba:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        console.log('\n🔌 Cerrando conexiones...');
        if (redisRepo) await redisRepo.disconnect();
        if (gcsAdapter) await gcsAdapter.cleanup();
        process.exit(0);
    }
}

async function addTestDataToRedis(redisRepo) {
    try {
        // Datos GPS de prueba para Lima
        const gpsTestData = [
            {
                deviceId: 'GPS001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,  // Lima Centro
                lng: -77.0428
            },
            {
                deviceId: 'GPS002',
                timestamp: new Date(Date.now() - 30000).toISOString(),
                lat: -12.0500,  // Miraflores
                lng: -77.0450
            },
            {
                deviceId: 'GPS003',
                timestamp: new Date(Date.now() - 60000).toISOString(),
                lat: -12.0520,  // San Isidro
                lng: -77.0470
            }
        ];
        
        // Datos Mobile de prueba
        const mobileTestData = [
            {
                userId: 'USER001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,
                lng: -77.0428,
                name: 'Inspector Municipal 1',
                email: 'inspector1@lima.gob.pe'
            },
            {
                userId: 'USER002',
                timestamp: new Date(Date.now() - 45000).toISOString(),
                lat: -12.0500,
                lng: -77.0450,
                name: 'Inspector Municipal 2',
                email: 'inspector2@lima.gob.pe'
            }
        ];
        
        // Usar las keys del sistema real
        const gpsKey = process.env.GPS_LIST_KEY || 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        console.log(`📝 Agregando ${gpsTestData.length} registros GPS a: ${gpsKey}`);
        await redisRepo.addMultipleToList(gpsKey, gpsTestData);
        
        console.log(`📝 Agregando ${mobileTestData.length} registros Mobile a: ${mobileKey}`);
        await redisRepo.addMultipleToList(mobileKey, mobileTestData);
        
        // Verificar que se agregaron
        const gpsCount = await redisRepo.getListLength(gpsKey);
        const mobileCount = await redisRepo.getListLength(mobileKey);
        
        console.log(`✅ GPS en Redis: ${gpsCount} registros`);
        console.log(`✅ Mobile en Redis: ${mobileCount} registros`);
        
    } catch (error) {
        console.error('❌ Error agregando datos de prueba:', error.message);
        throw error;
    }
}

async function processGPSData(redisRepo, gcsAdapter) {
    try {
        const gpsKey = process.env.GPS_LIST_KEY || 'gps:history:global';
        const gcsPrefix = process.env.GCS_GPS_PREFIX || 'gps-data/';
        
        console.log(`📦 Obteniendo datos GPS de: ${gpsKey}`);
        
        // Obtener todos los datos GPS
        const gpsData = await redisRepo.getListData(gpsKey);
        
        if (gpsData.length === 0) {
            console.log('⚠️ No hay datos GPS para procesar');
            return;
        }
        
        console.log(`📊 Encontrados ${gpsData.length} registros GPS`);
        
        // Crear nombre de archivo con timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const processingId = `gps_${Date.now()}`;
        const fileName = `${gcsPrefix}${timestamp}_${processingId}.json`;
        
        // Convertir a JSON Lines
        const jsonLines = gpsData.map(item => JSON.stringify(item)).join('\n');
        
        console.log(`📤 Subiendo archivo GPS: ${fileName}`);
        console.log(`📊 Tamaño: ${jsonLines.length} bytes`);
        
        // Subir a GCS
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'gps',
            processingId: processingId,
            recordCount: gpsData.length,
            sourceKey: gpsKey
        });
        
        if (gcsResult.success) {
            console.log(`✅ Archivo GPS subido exitosamente`);
            console.log(`🔗 URI: ${gcsResult.gcsUri}`);
            console.log(`📊 Registros: ${gcsResult.metadata.recordCount}`);
            
            // Limpiar datos de Redis después del upload exitoso
            console.log(`🗑️ Limpiando datos GPS de Redis...`);
            const cleared = await redisRepo.clearListData(gpsKey);
            console.log(`✅ Datos GPS ${cleared ? 'eliminados' : 'ya estaban vacíos'} de Redis`);
            
        } else {
            console.log(`❌ Error subiendo GPS: ${gcsResult.error}`);
        }
        
    } catch (error) {
        console.error('❌ Error procesando datos GPS:', error.message);
        throw error;
    }
}

async function processMobileData(redisRepo, gcsAdapter) {
    try {
        const mobileKey = 'mobile:history:global';
        const gcsPrefix = process.env.GCS_MOBILE_PREFIX || 'mobile-data/';
        
        console.log(`📦 Obteniendo datos Mobile de: ${mobileKey}`);
        
        // Obtener todos los datos Mobile
        const mobileData = await redisRepo.getListData(mobileKey);
        
        if (mobileData.length === 0) {
            console.log('⚠️ No hay datos Mobile para procesar');
            return;
        }
        
        console.log(`📊 Encontrados ${mobileData.length} registros Mobile`);
        
        // Crear nombre de archivo con timestamp
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const processingId = `mobile_${Date.now()}`;
        const fileName = `${gcsPrefix}${timestamp}_${processingId}.json`;
        
        // Convertir a JSON Lines
        const jsonLines = mobileData.map(item => JSON.stringify(item)).join('\n');
        
        console.log(`📤 Subiendo archivo Mobile: ${fileName}`);
        console.log(`📊 Tamaño: ${jsonLines.length} bytes`);
        
        // Subir a GCS
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'mobile',
            processingId: processingId,
            recordCount: mobileData.length,
            sourceKey: mobileKey
        });
        
        if (gcsResult.success) {
            console.log(`✅ Archivo Mobile subido exitosamente`);
            console.log(`🔗 URI: ${gcsResult.gcsUri}`);
            console.log(`📊 Registros: ${gcsResult.metadata.recordCount}`);
            
            // Limpiar datos de Redis después del upload exitoso
            console.log(`🗑️ Limpiando datos Mobile de Redis...`);
            const cleared = await redisRepo.clearListData(mobileKey);
            console.log(`✅ Datos Mobile ${cleared ? 'eliminados' : 'ya estaban vacíos'} de Redis`);
            
        } else {
            console.log(`❌ Error subiendo Mobile: ${gcsResult.error}`);
        }
        
    } catch (error) {
        console.error('❌ Error procesando datos Mobile:', error.message);
        throw error;
    }
}

async function verifyFinalState(redisRepo, gcsAdapter) {
    try {
        console.log('🔍 Verificando estado final...');
        
        // Verificar Redis
        const gpsKey = process.env.GPS_LIST_KEY || 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        const gpsRemaining = await redisRepo.getListLength(gpsKey);
        const mobileRemaining = await redisRepo.getListLength(mobileKey);
        
        console.log(`📊 Redis GPS restantes: ${gpsRemaining}`);
        console.log(`📊 Redis Mobile restantes: ${mobileRemaining}`);
        
        // Verificar GCS
        const bucketStats = await gcsAdapter.getBucketStats();
        console.log(`☁️ Total archivos en GCS: ${bucketStats.totalFiles}`);
        console.log(`💾 Tamaño total GCS: ${Math.round(bucketStats.totalSize / 1024)} KB`);
        console.log(`📁 Archivos por tipo:`, bucketStats.filesByType);
        
        // Verificar conexión Redis
        const redisConnected = await redisRepo.ping();
        console.log(`🔗 Redis conectado: ${redisConnected ? '✅' : '❌'}`);
        
    } catch (error) {
        console.error('❌ Error verificando estado final:', error.message);
    }
}

// Ejecutar la prueba
testRedisToGCS();