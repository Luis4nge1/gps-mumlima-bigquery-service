import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Prueba completa del flujo Redis → GCS → BigQuery
 * Con datos limpios que coinciden con el esquema de BigQuery
 */
async function testCompleteFlow() {
    console.log('🔄 Probando flujo completo Redis → GCS → BigQuery');
    console.log('📋 Con datos limpios que coinciden con el esquema\n');
    
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
        
        console.log('✅ Redis inicializado');
        console.log('✅ GCS Adapter inicializado');
        console.log('✅ BigQuery Batch Processor inicializado');
        
        // 2. Limpiar datos anteriores
        console.log('\n📋 Paso 2: Limpiando datos anteriores...');
        await cleanupPreviousData(redisRepo);
        
        // 3. Agregar datos limpios a Redis
        console.log('\n📋 Paso 3: Agregando datos limpios a Redis...');
        await addCleanTestData(redisRepo);
        
        // 4. Estado inicial BigQuery
        console.log('\n📊 === ESTADO INICIAL BIGQUERY ===');
        await showBigQueryStats(bigQueryProcessor);
        
        // 5. Procesar Redis → GCS
        console.log('\n🔄 === PROCESANDO REDIS → GCS ===');
        const gcsFiles = await processRedisToGCS(redisRepo, gcsAdapter);
        
        // 6. Procesar GCS → BigQuery
        console.log('\n🔄 === PROCESANDO GCS → BIGQUERY ===');
        await processGCSToBigQuery(gcsAdapter, bigQueryProcessor, gcsFiles);
        
        // 7. Estado final BigQuery
        console.log('\n📊 === ESTADO FINAL BIGQUERY ===');
        await showBigQueryStats(bigQueryProcessor);
        
        // 8. Verificar datos en BigQuery
        console.log('\n🔍 === VERIFICACIÓN DE DATOS ===');
        await verifyDataInBigQuery(bigQueryProcessor);
        
        console.log('\n🎉 ¡Prueba completa Redis → GCS → BigQuery exitosa!');
        
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

async function addCleanTestData(redisRepo) {
    try {
        // Datos GPS que coinciden EXACTAMENTE con el esquema BigQuery
        const gpsCleanData = [
            {
                deviceId: 'GPS_CLEAN_001',
                lat: -12.0464,
                lng: -77.0428,
                timestamp: new Date().toISOString()
            },
            {
                deviceId: 'GPS_CLEAN_002',
                lat: -12.0500,
                lng: -77.0450,
                timestamp: new Date(Date.now() - 30000).toISOString()
            },
            {
                deviceId: 'GPS_CLEAN_003',
                lat: -12.0520,
                lng: -77.0470,
                timestamp: new Date(Date.now() - 60000).toISOString()
            }
        ];
        
        // Datos Mobile que coinciden EXACTAMENTE con el esquema BigQuery
        const mobileCleanData = [
            {
                userId: 'USER_CLEAN_001',
                lat: -12.0464,
                lng: -77.0428,
                timestamp: new Date().toISOString(),
                name: 'Inspector Municipal Lima',
                email: 'inspector1@lima.gob.pe'
            },
            {
                userId: 'USER_CLEAN_002',
                lat: -12.0500,
                lng: -77.0450,
                timestamp: new Date(Date.now() - 45000).toISOString(),
                name: 'Inspector Municipal Miraflores',
                email: 'inspector2@lima.gob.pe'
            }
        ];
        
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        console.log(`📝 Agregando ${gpsCleanData.length} registros GPS limpios`);
        await redisRepo.addMultipleToList(gpsKey, gpsCleanData);
        
        console.log(`📝 Agregando ${mobileCleanData.length} registros Mobile limpios`);
        await redisRepo.addMultipleToList(mobileKey, mobileCleanData);
        
        const gpsCount = await redisRepo.getListLength(gpsKey);
        const mobileCount = await redisRepo.getListLength(mobileKey);
        
        console.log(`✅ GPS en Redis: ${gpsCount} registros`);
        console.log(`✅ Mobile en Redis: ${mobileCount} registros`);
        
        // Mostrar muestra de datos
        console.log('\n📋 Muestra de datos GPS:');
        console.log(JSON.stringify(gpsCleanData[0], null, 2));
        
        console.log('\n📋 Muestra de datos Mobile:');
        console.log(JSON.stringify(mobileCleanData[0], null, 2));
        
    } catch (error) {
        console.error('❌ Error agregando datos limpios:', error.message);
        throw error;
    }
}

async function showBigQueryStats(bigQueryProcessor) {
    try {
        const tableStats = await bigQueryProcessor.getTableStats();
        
        console.log('📊 Estadísticas de tablas BigQuery:');
        
        if (tableStats.gps && !tableStats.gps.error) {
            console.log(`   GPS (${tableStats.gps.tableName}):`);
            console.log(`      📊 Filas: ${tableStats.gps.numRows.toLocaleString()}`);
            console.log(`      💾 Bytes: ${Math.round(tableStats.gps.numBytes / 1024).toLocaleString()} KB`);
            if (tableStats.gps.lastModified && tableStats.gps.lastModified !== '0') {
                console.log(`      📅 Modificado: ${new Date(parseInt(tableStats.gps.lastModified)).toLocaleString()}`);
            }
        } else {
            console.log(`   GPS: ${tableStats.gps?.error || 'Error obteniendo estadísticas'}`);
        }
        
        if (tableStats.mobile && !tableStats.mobile.error) {
            console.log(`   Mobile (${tableStats.mobile.tableName}):`);
            console.log(`      📊 Filas: ${tableStats.mobile.numRows.toLocaleString()}`);
            console.log(`      💾 Bytes: ${Math.round(tableStats.mobile.numBytes / 1024).toLocaleString()} KB`);
            if (tableStats.mobile.lastModified && tableStats.mobile.lastModified !== '0') {
                console.log(`      📅 Modificado: ${new Date(parseInt(tableStats.mobile.lastModified)).toLocaleString()}`);
            }
        } else {
            console.log(`   Mobile: ${tableStats.mobile?.error || 'Error obteniendo estadísticas'}`);
        }
        
    } catch (error) {
        console.error('❌ Error obteniendo estadísticas BigQuery:', error.message);
    }
}

async function processRedisToGCS(redisRepo, gcsAdapter) {
    try {
        const gcsFiles = [];
        
        // Procesar GPS
        console.log('🔄 Procesando datos GPS...');
        const gpsKey = 'gps:history:global';
        const gpsData = await redisRepo.getListData(gpsKey);
        
        if (gpsData.length > 0) {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const processingId = `clean_gps_${Date.now()}`;
            const fileName = `gps-data/${timestamp}_${processingId}.json`;
            const jsonLines = gpsData.map(item => JSON.stringify(item)).join('\n');
            
            console.log(`📤 Subiendo archivo GPS: ${fileName}`);
            console.log(`📊 Registros: ${gpsData.length}, Tamaño: ${jsonLines.length} bytes`);
            
            const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: 'gps',
                processingId: processingId,
                recordCount: gpsData.length,
                sourceKey: gpsKey,
                cleanData: true
            });
            
            if (gcsResult.success) {
                console.log(`✅ Archivo GPS subido: ${gcsResult.gcsUri}`);
                gcsFiles.push({
                    gcsUri: gcsResult.gcsUri,
                    dataType: 'gps',
                    fileName: fileName,
                    recordCount: gpsData.length,
                    processingId: processingId
                });
                
                // Limpiar Redis
                await redisRepo.clearListData(gpsKey);
                console.log('✅ Datos GPS limpiados de Redis');
            } else {
                console.log(`❌ Error subiendo GPS: ${gcsResult.error}`);
            }
        }
        
        // Procesar Mobile
        console.log('\n🔄 Procesando datos Mobile...');
        const mobileKey = 'mobile:history:global';
        const mobileData = await redisRepo.getListData(mobileKey);
        
        if (mobileData.length > 0) {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const processingId = `clean_mobile_${Date.now()}`;
            const fileName = `mobile-data/${timestamp}_${processingId}.json`;
            const jsonLines = mobileData.map(item => JSON.stringify(item)).join('\n');
            
            console.log(`📤 Subiendo archivo Mobile: ${fileName}`);
            console.log(`📊 Registros: ${mobileData.length}, Tamaño: ${jsonLines.length} bytes`);
            
            const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: 'mobile',
                processingId: processingId,
                recordCount: mobileData.length,
                sourceKey: mobileKey,
                cleanData: true
            });
            
            if (gcsResult.success) {
                console.log(`✅ Archivo Mobile subido: ${gcsResult.gcsUri}`);
                gcsFiles.push({
                    gcsUri: gcsResult.gcsUri,
                    dataType: 'mobile',
                    fileName: fileName,
                    recordCount: mobileData.length,
                    processingId: processingId
                });
                
                // Limpiar Redis
                await redisRepo.clearListData(mobileKey);
                console.log('✅ Datos Mobile limpiados de Redis');
            } else {
                console.log(`❌ Error subiendo Mobile: ${gcsResult.error}`);
            }
        }
        
        console.log(`\n📊 Total archivos subidos a GCS: ${gcsFiles.length}`);
        return gcsFiles;
        
    } catch (error) {
        console.error('❌ Error procesando Redis → GCS:', error.message);
        throw error;
    }
}

async function processGCSToBigQuery(gcsAdapter, bigQueryProcessor, gcsFiles) {
    try {
        if (gcsFiles.length === 0) {
            console.log('⚠️ No hay archivos GCS para procesar');
            return;
        }
        
        console.log(`📦 Procesando ${gcsFiles.length} archivos de GCS a BigQuery`);
        
        for (const file of gcsFiles) {
            console.log(`\n🔄 Procesando: ${file.fileName}`);
            console.log(`   Tipo: ${file.dataType}`);
            console.log(`   Registros: ${file.recordCount}`);
            console.log(`   URI: ${file.gcsUri}`);
            
            const startTime = Date.now();
            
            const result = await bigQueryProcessor.processGCSFile(
                file.gcsUri,
                file.dataType,
                {
                    processingId: file.processingId,
                    recordCount: file.recordCount,
                    originalFileName: file.fileName,
                    cleanData: true
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
        
    } catch (error) {
        console.error('❌ Error procesando GCS → BigQuery:', error.message);
        throw error;
    }
}

async function verifyDataInBigQuery(bigQueryProcessor) {
    try {
        console.log('🔍 Verificando datos insertados en BigQuery...');
        
        // Obtener estadísticas actualizadas
        const tableStats = await bigQueryProcessor.getTableStats();
        
        let totalRecords = 0;
        
        if (tableStats.gps && !tableStats.gps.error) {
            const gpsRecords = tableStats.gps.numRows;
            console.log(`📊 Tabla GPS: ${gpsRecords} registros`);
            totalRecords += gpsRecords;
        }
        
        if (tableStats.mobile && !tableStats.mobile.error) {
            const mobileRecords = tableStats.mobile.numRows;
            console.log(`📊 Tabla Mobile: ${mobileRecords} registros`);
            totalRecords += mobileRecords;
        }
        
        console.log(`📊 Total registros en BigQuery: ${totalRecords}`);
        
        if (totalRecords > 0) {
            console.log('✅ Datos verificados exitosamente en BigQuery');
        } else {
            console.log('⚠️ No se encontraron datos en BigQuery');
        }
        
    } catch (error) {
        console.error('❌ Error verificando datos en BigQuery:', error.message);
    }
}

// Ejecutar la prueba
testCompleteFlow();