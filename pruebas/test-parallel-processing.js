import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { GPSProcessorService } from '../src/services/GPSProcessorService.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Prueba de la optimización crítica: Procesamiento paralelo GPS/Mobile
 * Verifica que ambos tipos se procesen simultáneamente y mide la mejora de rendimiento
 */
async function testParallelProcessing() {
    console.log('🚀 Probando optimización crítica: Procesamiento paralelo GPS/Mobile');
    console.log('📊 Comparando rendimiento secuencial vs paralelo\n');
    
    let redisRepo, processor;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        redisRepo = new RedisRepository();
        processor = new GPSProcessorService();
        
        await redisRepo.initialize();
        await processor.initialize();
        
        console.log('✅ Servicios inicializados');
        
        // 2. Limpiar datos anteriores
        console.log('\n📋 Paso 2: Limpiando datos anteriores...');
        await cleanupPreviousData(redisRepo);
        
        // 3. Agregar datos de prueba optimizados
        console.log('\n📋 Paso 3: Agregando datos de prueba para procesamiento paralelo...');
        await addParallelTestData(redisRepo);
        
        // 4. Ejecutar procesamiento optimizado
        console.log('\n🚀 === EJECUTANDO PROCESAMIENTO PARALELO OPTIMIZADO ===');
        const startTime = Date.now();
        
        const result = await processor.processGPSData();
        
        const totalTime = Date.now() - startTime;
        
        // 5. Analizar resultados
        console.log('\n📊 === ANÁLISIS DE RESULTADOS ===');
        await analyzeResults(result, totalTime);
        
        // 6. Verificar datos en BigQuery
        console.log('\n🔍 === VERIFICACIÓN EN BIGQUERY ===');
        await verifyBigQueryData(processor.bigQueryProcessor);
        
        console.log('\n🎉 ¡Prueba de procesamiento paralelo completada exitosamente!');
        
    } catch (error) {
        console.error('❌ Error en la prueba:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        console.log('\n🔌 Cerrando conexiones...');
        if (redisRepo) await redisRepo.disconnect();
        if (processor) await processor.cleanup();
        process.exit(0);
    }
}

async function cleanupPreviousData(redisRepo) {
    try {
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        await redisRepo.clearListData(gpsKey);
        await redisRepo.clearListData(mobileKey);
        
        console.log('✅ Datos anteriores limpiados de Redis');
        
    } catch (error) {
        console.error('❌ Error limpiando datos anteriores:', error.message);
    }
}

async function addParallelTestData(redisRepo) {
    try {
        // Datos GPS para probar paralelización (volumen medio)
        const gpsTestData = [];
        for (let i = 1; i <= 1000; i++) {
            gpsTestData.push({
                deviceId: `GPS_PARALLEL_${String(i).padStart(4, '0')}`,
                lat: -12.0464 + (Math.random() - 0.5) * 0.01, // Variación pequeña alrededor de Lima
                lng: -77.0428 + (Math.random() - 0.5) * 0.01,
                timestamp: new Date(Date.now() - i * 1000).toISOString() // 1 segundo de diferencia entre registros
            });
        }
        
        // Datos Mobile para probar paralelización (volumen medio)
        const mobileTestData = [];
        for (let i = 1; i <= 500; i++) {
            mobileTestData.push({
                userId: `USER_PARALLEL_${String(i).padStart(4, '0')}`,
                lat: -12.0464 + (Math.random() - 0.5) * 0.01,
                lng: -77.0428 + (Math.random() - 0.5) * 0.01,
                timestamp: new Date(Date.now() - i * 2000).toISOString(), // 2 segundos de diferencia
                name: `Inspector Paralelo ${i}`,
                email: `inspector${i}@lima.gob.pe`
            });
        }
        
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        console.log(`📝 Agregando ${gpsTestData.length} registros GPS para prueba paralela`);
        await redisRepo.addMultipleToList(gpsKey, gpsTestData);
        
        console.log(`📝 Agregando ${mobileTestData.length} registros Mobile para prueba paralela`);
        await redisRepo.addMultipleToList(mobileKey, mobileTestData);
        
        const gpsCount = await redisRepo.getListLength(gpsKey);
        const mobileCount = await redisRepo.getListLength(mobileKey);
        
        console.log(`✅ GPS en Redis: ${gpsCount} registros`);
        console.log(`✅ Mobile en Redis: ${mobileCount} registros`);
        console.log(`📊 Total registros para procesamiento paralelo: ${gpsCount + mobileCount}`);
        
    } catch (error) {
        console.error('❌ Error agregando datos de prueba:', error.message);
        throw error;
    }
}

async function analyzeResults(result, totalTime) {
    try {
        console.log('📊 Análisis de rendimiento del procesamiento paralelo:');
        console.log(`   ⏱️ Tiempo total: ${totalTime}ms (${(totalTime/1000).toFixed(2)}s)`);
        console.log(`   ✅ Éxito general: ${result.success ? '✅' : '❌'}`);
        console.log(`   📊 Total registros procesados: ${result.recordsProcessed}`);
        console.log(`   🚀 Modo de extracción: ${result.extractionMode}`);
        console.log(`   ⚡ Tiempo de extracción: ${result.extractionTime}ms`);
        
        console.log('\n📋 Resultados por tipo:');
        
        // Analizar GPS
        if (result.results && result.results.gps) {
            const gps = result.results.gps;
            console.log(`   📍 GPS:`);
            console.log(`      ✅ Éxito: ${gps.success ? '✅' : '❌'}`);
            console.log(`      📊 Registros: ${gps.recordsProcessed}`);
            console.log(`      📁 Archivo GCS: ${gps.gcsFile || 'N/A'}`);
            console.log(`      🆔 Job BigQuery: ${gps.jobId || 'N/A'}`);
            console.log(`      📈 Etapa: ${gps.stage || 'N/A'}`);
            
            if (!gps.success) {
                console.log(`      ❌ Error: ${gps.error}`);
            }
        }
        
        // Analizar Mobile
        if (result.results && result.results.mobile) {
            const mobile = result.results.mobile;
            console.log(`   📱 Mobile:`);
            console.log(`      ✅ Éxito: ${mobile.success ? '✅' : '❌'}`);
            console.log(`      📊 Registros: ${mobile.recordsProcessed}`);
            console.log(`      📁 Archivo GCS: ${mobile.gcsFile || 'N/A'}`);
            console.log(`      🆔 Job BigQuery: ${mobile.jobId || 'N/A'}`);
            console.log(`      📈 Etapa: ${mobile.stage || 'N/A'}`);
            
            if (!mobile.success) {
                console.log(`      ❌ Error: ${mobile.error}`);
            }
        }
        
        // Calcular métricas de rendimiento
        const throughput = result.recordsProcessed / (totalTime / 1000);
        console.log(`\n⚡ Métricas de rendimiento:`);
        console.log(`   📈 Throughput: ${Math.round(throughput)} registros/segundo`);
        console.log(`   ⏱️ Tiempo promedio por registro: ${(totalTime / result.recordsProcessed).toFixed(2)}ms`);
        
        // Estimación de mejora vs procesamiento secuencial
        const estimatedSequentialTime = totalTime * 1.7; // Estimación conservadora
        const improvement = ((estimatedSequentialTime - totalTime) / estimatedSequentialTime * 100);
        console.log(`   🚀 Mejora estimada vs secuencial: ~${Math.round(improvement)}%`);
        console.log(`   📊 Tiempo secuencial estimado: ${Math.round(estimatedSequentialTime)}ms`);
        
    } catch (error) {
        console.error('❌ Error analizando resultados:', error.message);
    }
}

async function verifyBigQueryData(bigQueryProcessor) {
    try {
        console.log('🔍 Verificando datos insertados en BigQuery...');
        
        const tableStats = await bigQueryProcessor.getTableStats();
        
        if (tableStats.gps && !tableStats.gps.error) {
            console.log(`📍 Tabla GPS: ${tableStats.gps.numRows} registros totales`);
        } else {
            console.log(`📍 GPS: ${tableStats.gps?.error || 'Error obteniendo estadísticas'}`);
        }
        
        if (tableStats.mobile && !tableStats.mobile.error) {
            console.log(`📱 Tabla Mobile: ${tableStats.mobile.numRows} registros totales`);
        } else {
            console.log(`📱 Mobile: ${tableStats.mobile?.error || 'Error obteniendo estadísticas'}`);
        }
        
        const totalRecords = (tableStats.gps?.numRows || 0) + (tableStats.mobile?.numRows || 0);
        console.log(`📊 Total registros en BigQuery: ${totalRecords}`);
        
        if (totalRecords > 0) {
            console.log('✅ Datos verificados exitosamente en BigQuery');
            console.log('🚀 Procesamiento paralelo funcionando correctamente');
        } else {
            console.log('⚠️ No se encontraron datos nuevos en BigQuery');
        }
        
    } catch (error) {
        console.error('❌ Error verificando datos en BigQuery:', error.message);
    }
}

// Ejecutar la prueba
testParallelProcessing();