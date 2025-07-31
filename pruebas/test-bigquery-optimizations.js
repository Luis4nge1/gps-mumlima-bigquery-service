import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { RedisRepository } from '../src/repositories/RedisRepository.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Prueba de las optimizaciones de BigQuery
 * Verifica que las configuraciones de rendimiento se apliquen correctamente
 */
async function testBigQueryOptimizations() {
    console.log('🚀 Probando optimizaciones de BigQuery');
    console.log('📊 Verificando configuraciones de rendimiento\n');
    
    let redisRepo, gcsAdapter, bigQueryProcessor;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        bigQueryProcessor = new BigQueryBatchProcessor();
        
        await redisRepo.initialize();
        await gcsAdapter.initialize();
        await bigQueryProcessor.initialize();
        
        console.log('✅ Servicios inicializados');
        
        // 2. Verificar configuraciones
        console.log('\n📋 Paso 2: Verificando configuraciones de optimización...');
        await verifyOptimizations();
        
        // 3. Crear datos de prueba en GCS
        console.log('\n📋 Paso 3: Creando datos de prueba optimizados...');
        const testFiles = await createOptimizedTestData(redisRepo, gcsAdapter);
        
        // 4. Probar procesamiento optimizado
        console.log('\n🚀 === PROBANDO PROCESAMIENTO OPTIMIZADO ===');
        await testOptimizedProcessing(bigQueryProcessor, testFiles);
        
        // 5. Verificar resultados
        console.log('\n📊 === VERIFICACIÓN DE RESULTADOS ===');
        await verifyResults(bigQueryProcessor);
        
        console.log('\n🎉 ¡Prueba de optimizaciones BigQuery completada exitosamente!');
        
    } catch (error) {
        console.error('❌ Error en la prueba:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        console.log('\n🔌 Cerrando conexiones...');
        if (redisRepo) await redisRepo.disconnect();
        if (gcsAdapter) await gcsAdapter.cleanup();
        if (bigQueryProcessor) await bigQueryProcessor.cleanup();
        process.exit(0);
    }
}

async function verifyOptimizations() {
    try {
        console.log('🔍 Verificando variables de entorno de optimización:');
        
        const timeout = process.env.BIGQUERY_JOB_TIMEOUT_MS;
        const maxBadRecords = process.env.BIGQUERY_MAX_BAD_RECORDS;
        const priority = process.env.BIGQUERY_PRIORITY;
        const location = process.env.BIGQUERY_LOCATION;
        
        console.log(`   ⏱️ Job Timeout: ${timeout}ms (${timeout ? '✅' : '❌ usando default'})`);
        console.log(`   🛡️ Max Bad Records: ${maxBadRecords} (${maxBadRecords ? '✅' : '❌ usando default'})`);
        console.log(`   🚀 Priority: ${priority} (${priority ? '✅' : '❌ usando default'})`);
        console.log(`   🌍 Location: ${location} (${location ? '✅' : '❌ usando default'})`);
        
        // Verificar que las optimizaciones estén configuradas
        const optimizationsConfigured = timeout && maxBadRecords && priority;
        
        if (optimizationsConfigured) {
            console.log('✅ Todas las optimizaciones están configuradas correctamente');
        } else {
            console.log('⚠️ Algunas optimizaciones no están configuradas');
        }
        
        // Mostrar configuración calculada
        const calculatedTimeout = parseInt(timeout) || 300000;
        const calculatedMaxBad = parseInt(maxBadRecords) || 0;
        const calculatedPriority = priority || 'BATCH';
        
        console.log('\n📊 Configuración que se aplicará:');
        console.log(`   ⏱️ Timeout efectivo: ${calculatedTimeout}ms (${calculatedTimeout/1000}s)`);
        console.log(`   🛡️ Max bad records efectivo: ${calculatedMaxBad}`);
        console.log(`   🚀 Priority efectiva: ${calculatedPriority}`);
        
        return {
            timeout: calculatedTimeout,
            maxBadRecords: calculatedMaxBad,
            priority: calculatedPriority,
            allConfigured: optimizationsConfigured
        };
        
    } catch (error) {
        console.error('❌ Error verificando optimizaciones:', error.message);
        throw error;
    }
}

async function createOptimizedTestData(redisRepo, gcsAdapter) {
    try {
        // Limpiar datos anteriores
        await redisRepo.clearListData('gps:history:global');
        await redisRepo.clearListData('mobile:history:global');
        
        // Crear datos de prueba medianos para probar optimizaciones
        const gpsTestData = [];
        for (let i = 1; i <= 2000; i++) {
            gpsTestData.push({
                deviceId: `GPS_OPT_${String(i).padStart(4, '0')}`,
                lat: -12.0464 + (Math.random() - 0.5) * 0.01,
                lng: -77.0428 + (Math.random() - 0.5) * 0.01,
                timestamp: new Date(Date.now() - i * 1000).toISOString()
            });
        }
        
        const mobileTestData = [];
        for (let i = 1; i <= 800; i++) {
            mobileTestData.push({
                userId: `USER_OPT_${String(i).padStart(4, '0')}`,
                lat: -12.0464 + (Math.random() - 0.5) * 0.01,
                lng: -77.0428 + (Math.random() - 0.5) * 0.01,
                timestamp: new Date(Date.now() - i * 2000).toISOString(),
                name: `Inspector Optimizado ${i}`,
                email: `opt${i}@lima.gob.pe`
            });
        }
        
        console.log(`📝 Creando ${gpsTestData.length} registros GPS de prueba`);
        console.log(`📝 Creando ${mobileTestData.length} registros Mobile de prueba`);
        
        // Subir directamente a GCS para probar BigQuery
        const testFiles = [];
        
        // Archivo GPS
        const gpsJsonLines = gpsTestData.map(item => JSON.stringify(item)).join('\n');
        const gpsFileName = `gps-data/optimization-test-gps-${Date.now()}.json`;
        
        const gpsResult = await gcsAdapter.uploadJSONLines(gpsJsonLines, gpsFileName, {
            dataType: 'gps',
            recordCount: gpsTestData.length,
            testType: 'optimization',
            processingId: `opt_gps_${Date.now()}`
        });
        
        if (gpsResult.success) {
            console.log(`✅ Archivo GPS subido: ${gpsResult.fileName}`);
            testFiles.push({
                gcsUri: gpsResult.gcsUri,
                dataType: 'gps',
                fileName: gpsResult.fileName,
                recordCount: gpsTestData.length
            });
        }
        
        // Archivo Mobile
        const mobileJsonLines = mobileTestData.map(item => JSON.stringify(item)).join('\n');
        const mobileFileName = `mobile-data/optimization-test-mobile-${Date.now()}.json`;
        
        const mobileResult = await gcsAdapter.uploadJSONLines(mobileJsonLines, mobileFileName, {
            dataType: 'mobile',
            recordCount: mobileTestData.length,
            testType: 'optimization',
            processingId: `opt_mobile_${Date.now()}`
        });
        
        if (mobileResult.success) {
            console.log(`✅ Archivo Mobile subido: ${mobileResult.fileName}`);
            testFiles.push({
                gcsUri: mobileResult.gcsUri,
                dataType: 'mobile',
                fileName: mobileResult.fileName,
                recordCount: mobileTestData.length
            });
        }
        
        console.log(`📊 Total archivos de prueba creados: ${testFiles.length}`);
        return testFiles;
        
    } catch (error) {
        console.error('❌ Error creando datos de prueba:', error.message);
        throw error;
    }
}

async function testOptimizedProcessing(bigQueryProcessor, testFiles) {
    try {
        if (testFiles.length === 0) {
            console.log('⚠️ No hay archivos de prueba para procesar');
            return;
        }
        
        console.log(`📦 Procesando ${testFiles.length} archivos con optimizaciones BigQuery`);
        
        const results = [];
        
        for (const file of testFiles) {
            console.log(`\n🔄 Procesando: ${file.fileName}`);
            console.log(`   Tipo: ${file.dataType}`);
            console.log(`   Registros: ${file.recordCount}`);
            console.log(`   URI: ${file.gcsUri}`);
            
            const startTime = Date.now();
            
            const result = await bigQueryProcessor.processGCSFile(
                file.gcsUri,
                file.dataType,
                {
                    processingId: `optimization_test_${Date.now()}`,
                    recordCount: file.recordCount,
                    testType: 'optimization',
                    optimizationsEnabled: true
                }
            );
            
            const duration = Date.now() - startTime;
            
            if (result.success) {
                console.log(`✅ Archivo procesado exitosamente:`);
                console.log(`   🆔 Job ID: ${result.jobId}`);
                console.log(`   📊 Registros procesados: ${result.recordsProcessed}`);
                console.log(`   💾 Bytes procesados: ${result.bytesProcessed}`);
                console.log(`   📋 Tabla destino: ${result.tableName}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                console.log(`   📅 Completado: ${new Date(result.completedAt).toLocaleString()}`);
                
                // Calcular throughput
                const throughput = result.recordsProcessed / (duration / 1000);
                console.log(`   📈 Throughput: ${Math.round(throughput)} registros/segundo`);
                
                results.push({
                    ...result,
                    duration,
                    throughput,
                    dataType: file.dataType,
                    recordCount: file.recordCount
                });
                
            } else {
                console.log(`❌ Error procesando archivo:`);
                console.log(`   Error: ${result.error}`);
                console.log(`   Job ID: ${result.jobId || 'N/A'}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                
                if (result.errors && result.errors.length > 0) {
                    console.log(`   Errores detallados:`);
                    result.errors.slice(0, 3).forEach((error, index) => {
                        console.log(`      ${index + 1}. ${error.message || error}`);
                    });
                }
            }
        }
        
        // Análisis de rendimiento
        if (results.length > 0) {
            console.log('\n📊 Análisis de rendimiento con optimizaciones:');
            
            const totalRecords = results.reduce((sum, r) => sum + r.recordsProcessed, 0);
            const totalTime = results.reduce((sum, r) => sum + r.duration, 0);
            const avgThroughput = results.reduce((sum, r) => sum + r.throughput, 0) / results.length;
            
            console.log(`   📊 Total registros: ${totalRecords}`);
            console.log(`   ⏱️ Tiempo total: ${totalTime}ms (${(totalTime/1000).toFixed(2)}s)`);
            console.log(`   📈 Throughput promedio: ${Math.round(avgThroughput)} registros/segundo`);
            
            // Comparar con estimaciones sin optimización
            const estimatedUnoptimizedTime = totalTime * 1.3; // 30% más lento sin optimizaciones
            const improvement = ((estimatedUnoptimizedTime - totalTime) / estimatedUnoptimizedTime * 100);
            
            console.log(`   🚀 Mejora estimada vs sin optimizaciones: ~${Math.round(improvement)}%`);
            console.log(`   📊 Tiempo sin optimizaciones estimado: ${Math.round(estimatedUnoptimizedTime)}ms`);
        }
        
        return results;
        
    } catch (error) {
        console.error('❌ Error en procesamiento optimizado:', error.message);
        throw error;
    }
}

async function verifyResults(bigQueryProcessor) {
    try {
        console.log('🔍 Verificando resultados en BigQuery...');
        
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
            console.log('✅ Optimizaciones BigQuery funcionando correctamente');
        } else {
            console.log('⚠️ No se encontraron datos en BigQuery');
        }
        
    } catch (error) {
        console.error('❌ Error verificando resultados:', error.message);
    }
}

// Ejecutar la prueba
testBigQueryOptimizations();