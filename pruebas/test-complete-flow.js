import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import dotenv from 'dotenv';

dotenv.config();

async function testCompleteFlow() {
    console.log('🚀 Iniciando prueba del flujo completo Redis → GCS → BigQuery');
    
    try {
        // 1. Inicializar servicios
        console.log('\n📋 Paso 1: Inicializando servicios...');
        const redisRepo = new RedisRepository();
        const gcsAdapter = new GCSAdapter();
        const bigQueryProcessor = new BigQueryBatchProcessor();
        
        await redisRepo.initialize();
        console.log('✅ Redis inicializado');
        
        // 2. Agregar datos de prueba a Redis
        console.log('\n📋 Paso 2: Agregando datos de prueba a Redis...');
        const testData = [
            {
                deviceId: 'GPS001',
                timestamp: new Date().toISOString(),
                lat: -34.6037,
                lng: -58.3816
            },
            {
                deviceId: 'GPS002', 
                timestamp: new Date().toISOString(),
                lat: -34.6118,
                lng: -58.3960
            },
            {
                deviceId: 'GPS003',
                timestamp: new Date().toISOString(),
                lat: -34.6158,
                lng: -58.3734
            }
        ];
        
        const queueKey = 'gps_data_queue';
        await redisRepo.addMultipleToList(queueKey, testData);
        console.log(`✅ Agregados ${testData.length} registros a Redis queue: ${queueKey}`);
        
        // Verificar datos en Redis
        const queueLength = await redisRepo.getListLength(queueKey);
        console.log(`📊 Longitud actual de la queue: ${queueLength}`);
        
        // 3. Procesar datos de Redis a GCS
        console.log('\n📋 Paso 3: Procesando datos de Redis a GCS...');
        const batchSize = 100;
        const batch = await redisRepo.getBatch(queueKey, batchSize);
        
        if (batch.length === 0) {
            console.log('⚠️ No hay datos en la queue para procesar');
            return;
        }
        
        console.log(`📦 Obtenido batch de ${batch.length} registros de Redis`);
        
        // Subir a GCS
        const bucketName = process.env.GCS_BUCKET_NAME;
        const fileName = `gps_data_${Date.now()}.json`;
        
        // Convertir datos a formato JSON Lines
        const jsonLines = batch.map(item => JSON.stringify(item)).join('\n');
        
        console.log(`📤 Subiendo datos a GCS: ${bucketName}/${fileName}`);
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'gps',
            processingId: `test_${Date.now()}`
        });
        console.log('✅ Datos subidos exitosamente a GCS');
        console.log(`🔗 GCS URI: gs://${bucketName}/${fileName}`);
        
        // 4. Cargar de GCS a BigQuery
        console.log('\n📋 Paso 4: Cargando datos de GCS a BigQuery...');
        const datasetId = process.env.BIGQUERY_DATASET_ID;
        const tableId = process.env.BIGQUERY_GPS_TABLE;
        
        console.log(`📊 Cargando a BigQuery: ${datasetId}.${tableId}`);
        const bigQueryResult = await bigQueryProcessor.loadFromGCS(
            `gs://${bucketName}/${fileName}`,
            datasetId,
            tableId
        );
        
        if (bigQueryResult.success) {
            console.log('✅ Datos cargados exitosamente a BigQuery');
            console.log(`📈 Job ID: ${bigQueryResult.jobId}`);
            console.log(`📊 Registros procesados: ${bigQueryResult.recordsProcessed}`);
        } else {
            console.log('❌ Error cargando datos a BigQuery');
            console.log(`🔍 Error: ${bigQueryResult.error}`);
            if (bigQueryResult.errors) {
                console.log('🔍 Errores detallados:', bigQueryResult.errors);
            }
        }
        
        // 5. Limpiar queue de Redis (simular procesamiento exitoso)
        console.log('\n📋 Paso 5: Limpiando datos procesados de Redis...');
        await redisRepo.removeBatch(queueKey, batch.length);
        const finalQueueLength = await redisRepo.getListLength(queueKey);
        console.log(`✅ Queue limpiada. Longitud final: ${finalQueueLength}`);
        
        console.log('\n🎉 ¡Flujo completo ejecutado exitosamente!');
        console.log('📊 Resumen:');
        console.log(`   • Datos procesados: ${batch.length} registros`);
        console.log(`   • Archivo GCS: gs://${bucketName}/${fileName}`);
        console.log(`   • Tabla BigQuery: ${datasetId}.${tableId}`);
        console.log(`   • Job BigQuery: ${bigQueryResult.jobId || 'Error'}`);
        console.log(`   • Estado BigQuery: ${bigQueryResult.success ? 'Exitoso' : 'Error'}`);
        
    } catch (error) {
        console.error('❌ Error en el flujo completo:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        // Cerrar conexiones
        console.log('\n🔌 Cerrando conexiones...');
        process.exit(0);
    }
}

// Ejecutar la prueba
testCompleteFlow();