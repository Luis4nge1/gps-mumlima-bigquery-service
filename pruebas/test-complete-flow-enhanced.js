import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import dotenv from 'dotenv';

dotenv.config();

async function testCompleteFlowEnhanced() {
    console.log('🚀 Iniciando prueba COMPLETA del flujo Redis → GCS → BigQuery');
    console.log('📋 Probando datos GPS y Mobile con manejo de errores\n');
    
    let redisRepo, gcsAdapter, bigQueryProcessor;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        bigQueryProcessor = new BigQueryBatchProcessor();
        
        await redisRepo.initialize();
        console.log('✅ Redis inicializado');
        
        // 2. Probar flujo GPS
        console.log('\n🔄 === PROBANDO FLUJO GPS ===');
        await testGPSFlow(redisRepo, gcsAdapter, bigQueryProcessor);
        
        // 3. Probar flujo Mobile
        console.log('\n🔄 === PROBANDO FLUJO MOBILE ===');
        await testMobileFlow(redisRepo, gcsAdapter, bigQueryProcessor);
        
        // 4. Verificar estadísticas finales
        console.log('\n📊 === ESTADÍSTICAS FINALES ===');
        await showFinalStats(redisRepo, gcsAdapter, bigQueryProcessor);
        
        console.log('\n🎉 ¡Todas las pruebas completadas exitosamente!');
        
    } catch (error) {
        console.error('❌ Error crítico en el flujo:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        // Cerrar conexiones
        console.log('\n🔌 Cerrando conexiones...');
        if (redisRepo) {
            await redisRepo.disconnect();
        }
        if (gcsAdapter) {
            await gcsAdapter.cleanup();
        }
        if (bigQueryProcessor) {
            await bigQueryProcessor.cleanup();
        }
        process.exit(0);
    }
}

async function testGPSFlow(redisRepo, gcsAdapter, bigQueryProcessor) {
    try {
        // Datos GPS de prueba
        const gpsData = [
            {
                deviceId: 'GPS001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,  // Lima, Perú
                lng: -77.0428
            },
            {
                deviceId: 'GPS002', 
                timestamp: new Date(Date.now() - 60000).toISOString(), // 1 min atrás
                lat: -12.0500,
                lng: -77.0450
            },
            {
                deviceId: 'GPS003',
                timestamp: new Date(Date.now() - 120000).toISOString(), // 2 min atrás
                lat: -12.0520,
                lng: -77.0470
            }
        ];
        
        const gpsQueueKey = 'gps_test_queue';
        
        // Agregar datos a Redis
        console.log('📝 Agregando datos GPS a Redis...');
        await redisRepo.addMultipleToList(gpsQueueKey, gpsData);
        const queueLength = await redisRepo.getListLength(gpsQueueKey);
        console.log(`✅ ${gpsData.length} registros GPS agregados (queue length: ${queueLength})`);
        
        // Procesar batch de Redis
        console.log('📦 Obteniendo batch GPS de Redis...');
        const batch = await redisRepo.getBatch(gpsQueueKey, 100);
        console.log(`✅ Obtenido batch de ${batch.length} registros GPS`);
        
        // Subir a GCS
        const fileName = `gps_test_${Date.now()}.json`;
        const jsonLines = batch.map(item => JSON.stringify(item)).join('\n');
        
        console.log('📤 Subiendo datos GPS a GCS...');
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'gps',
            processingId: `gps_test_${Date.now()}`
        });
        
        if (gcsResult.success) {
            console.log(`✅ Archivo GPS subido: ${gcsResult.gcsUri}`);
            console.log(`📊 Tamaño: ${gcsResult.fileSize} bytes`);
        } else {
            throw new Error(`Error subiendo GPS a GCS: ${gcsResult.error}`);
        }
        
        // Cargar a BigQuery
        console.log('📊 Cargando datos GPS a BigQuery...');
        const bigQueryResult = await bigQueryProcessor.processGCSFile(
            gcsResult.gcsUri,
            'gps',
            { processingId: `gps_test_${Date.now()}`, recordCount: batch.length }
        );
        
        if (bigQueryResult.success) {
            console.log(`✅ Datos GPS cargados exitosamente a BigQuery`);
            console.log(`📈 Job ID: ${bigQueryResult.jobId}`);
            console.log(`📊 Registros procesados: ${bigQueryResult.recordsProcessed}`);
            console.log(`📁 Bytes procesados: ${bigQueryResult.bytesProcessed}`);
        } else {
            console.log(`❌ Error cargando GPS a BigQuery: ${bigQueryResult.error}`);
            if (bigQueryResult.errors) {
                console.log('🔍 Errores detallados:', JSON.stringify(bigQueryResult.errors, null, 2));
            }
        }
        
        // Limpiar queue
        console.log('🗑️ Limpiando queue GPS...');
        await redisRepo.removeBatch(gpsQueueKey, batch.length);
        const finalLength = await redisRepo.getListLength(gpsQueueKey);
        console.log(`✅ Queue GPS limpiada (longitud final: ${finalLength})`);
        
    } catch (error) {
        console.error('❌ Error en flujo GPS:', error.message);
        throw error;
    }
}

async function testMobileFlow(redisRepo, gcsAdapter, bigQueryProcessor) {
    try {
        // Datos Mobile de prueba
        const mobileData = [
            {
                userId: 'USER001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,  // Lima, Perú
                lng: -77.0428,
                name: 'Juan Pérez',
                email: 'juan.perez@example.com'
            },
            {
                userId: 'USER002', 
                timestamp: new Date(Date.now() - 60000).toISOString(),
                lat: -12.0500,
                lng: -77.0450,
                name: 'María García',
                email: 'maria.garcia@example.com'
            },
            {
                userId: 'USER003',
                timestamp: new Date(Date.now() - 120000).toISOString(),
                lat: -12.0520,
                lng: -77.0470,
                name: 'Carlos López',
                email: 'carlos.lopez@example.com'
            }
        ];
        
        const mobileQueueKey = 'mobile_test_queue';
        
        // Agregar datos a Redis
        console.log('📝 Agregando datos Mobile a Redis...');
        await redisRepo.addMultipleToList(mobileQueueKey, mobileData);
        const queueLength = await redisRepo.getListLength(mobileQueueKey);
        console.log(`✅ ${mobileData.length} registros Mobile agregados (queue length: ${queueLength})`);
        
        // Procesar batch de Redis
        console.log('📦 Obteniendo batch Mobile de Redis...');
        const batch = await redisRepo.getBatch(mobileQueueKey, 100);
        console.log(`✅ Obtenido batch de ${batch.length} registros Mobile`);
        
        // Subir a GCS
        const fileName = `mobile_test_${Date.now()}.json`;
        const jsonLines = batch.map(item => JSON.stringify(item)).join('\n');
        
        console.log('📤 Subiendo datos Mobile a GCS...');
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'mobile',
            processingId: `mobile_test_${Date.now()}`
        });
        
        if (gcsResult.success) {
            console.log(`✅ Archivo Mobile subido: ${gcsResult.gcsUri}`);
            console.log(`📊 Tamaño: ${gcsResult.fileSize} bytes`);
        } else {
            throw new Error(`Error subiendo Mobile a GCS: ${gcsResult.error}`);
        }
        
        // Cargar a BigQuery
        console.log('📊 Cargando datos Mobile a BigQuery...');
        const bigQueryResult = await bigQueryProcessor.processGCSFile(
            gcsResult.gcsUri,
            'mobile',
            { processingId: `mobile_test_${Date.now()}`, recordCount: batch.length }
        );
        
        if (bigQueryResult.success) {
            console.log(`✅ Datos Mobile cargados exitosamente a BigQuery`);
            console.log(`📈 Job ID: ${bigQueryResult.jobId}`);
            console.log(`📊 Registros procesados: ${bigQueryResult.recordsProcessed}`);
            console.log(`📁 Bytes procesados: ${bigQueryResult.bytesProcessed}`);
        } else {
            console.log(`❌ Error cargando Mobile a BigQuery: ${bigQueryResult.error}`);
            if (bigQueryResult.errors) {
                console.log('🔍 Errores detallados:', JSON.stringify(bigQueryResult.errors, null, 2));
            }
        }
        
        // Limpiar queue
        console.log('🗑️ Limpiando queue Mobile...');
        await redisRepo.removeBatch(mobileQueueKey, batch.length);
        const finalLength = await redisRepo.getListLength(mobileQueueKey);
        console.log(`✅ Queue Mobile limpiada (longitud final: ${finalLength})`);
        
    } catch (error) {
        console.error('❌ Error en flujo Mobile:', error.message);
        throw error;
    }
}

async function showFinalStats(redisRepo, gcsAdapter, bigQueryProcessor) {
    try {
        console.log('📊 Obteniendo estadísticas finales...');
        
        // Estadísticas Redis
        const redisConnected = await redisRepo.ping();
        console.log(`🔗 Redis conectado: ${redisConnected ? '✅' : '❌'}`);
        
        // Estadísticas GCS
        const gcsStatus = await gcsAdapter.getStatus();
        console.log(`☁️ GCS inicializado: ${gcsStatus.initialized ? '✅' : '❌'}`);
        console.log(`☁️ GCS modo simulación: ${gcsStatus.simulationMode ? '🔧' : '🌐'}`);
        
        const bucketStats = await gcsAdapter.getBucketStats();
        console.log(`📁 Archivos en bucket: ${bucketStats.totalFiles}`);
        console.log(`💾 Tamaño total bucket: ${Math.round(bucketStats.totalSize / 1024)} KB`);
        
        // Estadísticas BigQuery
        const bqStatus = await bigQueryProcessor.getStatus();
        console.log(`📊 BigQuery inicializado: ${bqStatus.initialized ? '✅' : '❌'}`);
        console.log(`📊 BigQuery modo simulación: ${bqStatus.simulationMode ? '🔧' : '🌐'}`);
        
        const tableStats = await bigQueryProcessor.getTableStats();
        console.log(`📋 Tabla GPS registros: ${tableStats.gps?.numRows || 'N/A'}`);
        console.log(`📋 Tabla Mobile registros: ${tableStats.mobile?.numRows || 'N/A'}`);
        
    } catch (error) {
        console.error('❌ Error obteniendo estadísticas:', error.message);
    }
}

// Ejecutar la prueba
testCompleteFlowEnhanced();