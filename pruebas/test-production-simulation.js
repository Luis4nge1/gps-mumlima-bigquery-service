import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import dotenv from 'dotenv';

dotenv.config();

async function simulateProductionLoad() {
    console.log('🏭 SIMULACIÓN DE CARGA PRODUCTIVA');
    console.log('📋 Simulando flujo con volumen realista de datos\n');
    
    let redisRepo, gcsAdapter, bigQueryProcessor;
    
    try {
        // Inicializar servicios
        console.log('📋 Inicializando servicios...');
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        bigQueryProcessor = new BigQueryBatchProcessor();
        
        await redisRepo.initialize();
        console.log('✅ Servicios inicializados\n');
        
        // Simular datos GPS masivos
        console.log('🔄 === SIMULACIÓN GPS MASIVA ===');
        await simulateGPSBatch(redisRepo, gcsAdapter, bigQueryProcessor, 50);
        
        // Simular datos Mobile masivos
        console.log('\n🔄 === SIMULACIÓN MOBILE MASIVA ===');
        await simulateMobileBatch(redisRepo, gcsAdapter, bigQueryProcessor, 30);
        
        // Estadísticas finales
        console.log('\n📊 === ESTADÍSTICAS FINALES ===');
        await showProductionStats(redisRepo, gcsAdapter, bigQueryProcessor);
        
        console.log('\n🎉 ¡Simulación de producción completada exitosamente!');
        
    } catch (error) {
        console.error('❌ Error en simulación de producción:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        console.log('\n🔌 Cerrando conexiones...');
        if (redisRepo) await redisRepo.disconnect();
        if (gcsAdapter) await gcsAdapter.cleanup();
        if (bigQueryProcessor) await bigQueryProcessor.cleanup();
        process.exit(0);
    }
}

async function simulateGPSBatch(redisRepo, gcsAdapter, bigQueryProcessor, recordCount) {
    try {
        console.log(`📍 Generando ${recordCount} registros GPS simulados...`);
        
        // Generar datos GPS realistas para Lima
        const gpsData = [];
        const baseTime = Date.now();
        const devices = ['GPS001', 'GPS002', 'GPS003', 'GPS004', 'GPS005'];
        
        // Coordenadas base de Lima
        const limaLat = -12.0464;
        const limaLng = -77.0428;
        
        for (let i = 0; i < recordCount; i++) {
            const device = devices[i % devices.length];
            const timeOffset = i * 30000; // 30 segundos entre registros
            
            // Generar coordenadas aleatorias alrededor de Lima
            const latOffset = (Math.random() - 0.5) * 0.1; // ±0.05 grados
            const lngOffset = (Math.random() - 0.5) * 0.1;
            
            gpsData.push({
                deviceId: device,
                timestamp: new Date(baseTime - timeOffset).toISOString(),
                lat: limaLat + latOffset,
                lng: limaLng + lngOffset
            });
        }
        
        const queueKey = 'gps_production_queue';
        
        // Agregar datos en lotes para simular llegada gradual
        console.log('📝 Agregando datos GPS en lotes...');
        const batchSize = 10;
        for (let i = 0; i < gpsData.length; i += batchSize) {
            const batch = gpsData.slice(i, i + batchSize);
            await redisRepo.addMultipleToList(queueKey, batch);
            console.log(`   📦 Lote ${Math.floor(i/batchSize) + 1}: ${batch.length} registros`);
        }
        
        const totalInQueue = await redisRepo.getListLength(queueKey);
        console.log(`✅ Total en queue GPS: ${totalInQueue} registros`);
        
        // Procesar en lotes como en producción
        console.log('🔄 Procesando lotes GPS...');
        let processedTotal = 0;
        const processBatchSize = 25; // Procesar de 25 en 25
        
        while (true) {
            const batch = await redisRepo.getBatch(queueKey, processBatchSize);
            if (batch.length === 0) break;
            
            console.log(`📦 Procesando lote GPS de ${batch.length} registros...`);
            
            // Subir a GCS
            const fileName = `gps_production_${Date.now()}_${processedTotal}.json`;
            const jsonLines = batch.map(item => JSON.stringify(item)).join('\n');
            
            const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: 'gps',
                processingId: `production_${Date.now()}`
            });
            
            if (!gcsResult.success) {
                throw new Error(`Error subiendo lote GPS: ${gcsResult.error}`);
            }
            
            // Cargar a BigQuery
            const bigQueryResult = await bigQueryProcessor.processGCSFile(
                gcsResult.gcsUri,
                'gps',
                { processingId: `production_${Date.now()}`, recordCount: batch.length }
            );
            
            if (bigQueryResult.success) {
                console.log(`   ✅ Lote procesado: ${bigQueryResult.recordsProcessed} registros → BigQuery`);
                processedTotal += bigQueryResult.recordsProcessed;
                
                // Limpiar lote procesado
                await redisRepo.removeBatch(queueKey, batch.length);
            } else {
                console.log(`   ❌ Error en lote: ${bigQueryResult.error}`);
            }
        }
        
        console.log(`🎯 GPS Total procesado: ${processedTotal} registros`);
        
    } catch (error) {
        console.error('❌ Error en simulación GPS:', error.message);
        throw error;
    }
}

async function simulateMobileBatch(redisRepo, gcsAdapter, bigQueryProcessor, recordCount) {
    try {
        console.log(`📱 Generando ${recordCount} registros Mobile simulados...`);
        
        // Generar datos Mobile realistas
        const mobileData = [];
        const baseTime = Date.now();
        const users = [
            { id: 'USER001', name: 'Ana García', email: 'ana.garcia@lima.gob.pe' },
            { id: 'USER002', name: 'Carlos Mendoza', email: 'carlos.mendoza@lima.gob.pe' },
            { id: 'USER003', name: 'María Rodríguez', email: 'maria.rodriguez@lima.gob.pe' },
            { id: 'USER004', name: 'José Fernández', email: 'jose.fernandez@lima.gob.pe' },
            { id: 'USER005', name: 'Patricia Silva', email: 'patricia.silva@lima.gob.pe' }
        ];
        
        // Coordenadas base de Lima
        const limaLat = -12.0464;
        const limaLng = -77.0428;
        
        for (let i = 0; i < recordCount; i++) {
            const user = users[i % users.length];
            const timeOffset = i * 45000; // 45 segundos entre registros
            
            // Generar coordenadas aleatorias alrededor de Lima
            const latOffset = (Math.random() - 0.5) * 0.08;
            const lngOffset = (Math.random() - 0.5) * 0.08;
            
            mobileData.push({
                userId: user.id,
                timestamp: new Date(baseTime - timeOffset).toISOString(),
                lat: limaLat + latOffset,
                lng: limaLng + lngOffset,
                name: user.name,
                email: user.email
            });
        }
        
        const queueKey = 'mobile_production_queue';
        
        // Agregar datos en lotes
        console.log('📝 Agregando datos Mobile en lotes...');
        const batchSize = 8;
        for (let i = 0; i < mobileData.length; i += batchSize) {
            const batch = mobileData.slice(i, i + batchSize);
            await redisRepo.addMultipleToList(queueKey, batch);
            console.log(`   📦 Lote ${Math.floor(i/batchSize) + 1}: ${batch.length} registros`);
        }
        
        const totalInQueue = await redisRepo.getListLength(queueKey);
        console.log(`✅ Total en queue Mobile: ${totalInQueue} registros`);
        
        // Procesar en lotes
        console.log('🔄 Procesando lotes Mobile...');
        let processedTotal = 0;
        const processBatchSize = 15; // Procesar de 15 en 15
        
        while (true) {
            const batch = await redisRepo.getBatch(queueKey, processBatchSize);
            if (batch.length === 0) break;
            
            console.log(`📦 Procesando lote Mobile de ${batch.length} registros...`);
            
            // Subir a GCS
            const fileName = `mobile_production_${Date.now()}_${processedTotal}.json`;
            const jsonLines = batch.map(item => JSON.stringify(item)).join('\n');
            
            const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: 'mobile',
                processingId: `production_${Date.now()}`
            });
            
            if (!gcsResult.success) {
                throw new Error(`Error subiendo lote Mobile: ${gcsResult.error}`);
            }
            
            // Cargar a BigQuery
            const bigQueryResult = await bigQueryProcessor.processGCSFile(
                gcsResult.gcsUri,
                'mobile',
                { processingId: `production_${Date.now()}`, recordCount: batch.length }
            );
            
            if (bigQueryResult.success) {
                console.log(`   ✅ Lote procesado: ${bigQueryResult.recordsProcessed} registros → BigQuery`);
                processedTotal += bigQueryResult.recordsProcessed;
                
                // Limpiar lote procesado
                await redisRepo.removeBatch(queueKey, batch.length);
            } else {
                console.log(`   ❌ Error en lote: ${bigQueryResult.error}`);
            }
        }
        
        console.log(`🎯 Mobile Total procesado: ${processedTotal} registros`);
        
    } catch (error) {
        console.error('❌ Error en simulación Mobile:', error.message);
        throw error;
    }
}

async function showProductionStats(redisRepo, gcsAdapter, bigQueryProcessor) {
    try {
        console.log('📊 Estadísticas de la simulación de producción:');
        
        // Redis stats
        const redisConnected = await redisRepo.ping();
        console.log(`🔗 Redis: ${redisConnected ? '✅ Conectado' : '❌ Desconectado'}`);
        
        // GCS stats
        const bucketStats = await gcsAdapter.getBucketStats();
        console.log(`☁️ GCS Archivos: ${bucketStats.totalFiles}`);
        console.log(`💾 GCS Tamaño: ${Math.round(bucketStats.totalSize / 1024)} KB`);
        console.log(`📁 Archivos por tipo:`, bucketStats.filesByType);
        
        // BigQuery stats
        const tableStats = await bigQueryProcessor.getTableStats();
        console.log(`📊 BigQuery GPS: ${tableStats.gps?.numRows || 'N/A'} registros`);
        console.log(`📊 BigQuery Mobile: ${tableStats.mobile?.numRows || 'N/A'} registros`);
        
        // Calcular throughput estimado
        const totalRecords = (tableStats.gps?.numRows || 0) + (tableStats.mobile?.numRows || 0);
        console.log(`🚀 Total registros procesados: ${totalRecords}`);
        console.log(`⚡ Throughput estimado: ~${Math.round(totalRecords / 60)} registros/minuto`);
        
    } catch (error) {
        console.error('❌ Error obteniendo estadísticas de producción:', error.message);
    }
}

// Ejecutar simulación
simulateProductionLoad();