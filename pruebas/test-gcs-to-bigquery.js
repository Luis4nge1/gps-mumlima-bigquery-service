import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Prueba del flujo GCS → BigQuery
 * Procesa archivos existentes en GCS hacia BigQuery
 */
async function testGCSToBigQuery() {
    console.log('🔄 Probando flujo GCS → BigQuery');
    console.log('📋 Procesando archivos existentes en GCS\n');
    
    let gcsAdapter, bigQueryProcessor;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        gcsAdapter = new GCSAdapter();
        bigQueryProcessor = new BigQueryBatchProcessor();
        
        await gcsAdapter.initialize();
        await bigQueryProcessor.initialize();
        
        console.log('✅ GCS Adapter inicializado');
        console.log('✅ BigQuery Batch Processor inicializado');
        
        // 2. Listar archivos disponibles en GCS
        console.log('\n📋 Paso 2: Listando archivos en GCS...');
        await listGCSFiles(gcsAdapter);
        
        // 3. Obtener estado inicial de BigQuery
        console.log('\n📊 === ESTADO INICIAL BIGQUERY ===');
        await showBigQueryStatus(bigQueryProcessor);
        
        // 4. Procesar archivos GPS
        console.log('\n🔄 === PROCESANDO ARCHIVOS GPS ===');
        await processGPSFiles(gcsAdapter, bigQueryProcessor);
        
        // 5. Procesar archivos Mobile
        console.log('\n🔄 === PROCESANDO ARCHIVOS MOBILE ===');
        await processMobileFiles(gcsAdapter, bigQueryProcessor);
        
        // 6. Verificar estado final
        console.log('\n📊 === ESTADO FINAL BIGQUERY ===');
        await showBigQueryStatus(bigQueryProcessor);
        
        // 7. Mostrar jobs recientes
        console.log('\n📋 === JOBS RECIENTES ===');
        await showRecentJobs(bigQueryProcessor);
        
        console.log('\n🎉 ¡Prueba GCS → BigQuery completada exitosamente!');
        
    } catch (error) {
        console.error('❌ Error en la prueba:', error);
        console.error('Stack trace:', error.stack);
    } finally {
        console.log('\n🔌 Cerrando conexiones...');
        if (gcsAdapter) await gcsAdapter.cleanup();
        if (bigQueryProcessor) await bigQueryProcessor.cleanup();
        process.exit(0);
    }
}

async function listGCSFiles(gcsAdapter) {
    try {
        // Listar archivos GPS
        console.log('📁 Archivos GPS en GCS:');
        const gpsFiles = await gcsAdapter.listFiles({
            prefix: 'gps-data/',
            dataType: 'gps'
        });
        
        if (gpsFiles.length === 0) {
            console.log('   ⚠️ No se encontraron archivos GPS');
        } else {
            gpsFiles.forEach((file, index) => {
                console.log(`   ${index + 1}. ${file.name}`);
                console.log(`      📊 Tamaño: ${file.size} bytes`);
                console.log(`      📅 Creado: ${new Date(file.created).toLocaleString()}`);
                console.log(`      📊 Registros: ${file.metadata.recordCount || 'N/A'}`);
            });
        }
        
        // Listar archivos Mobile
        console.log('\n📁 Archivos Mobile en GCS:');
        const mobileFiles = await gcsAdapter.listFiles({
            prefix: 'mobile-data/',
            dataType: 'mobile'
        });
        
        if (mobileFiles.length === 0) {
            console.log('   ⚠️ No se encontraron archivos Mobile');
        } else {
            mobileFiles.forEach((file, index) => {
                console.log(`   ${index + 1}. ${file.name}`);
                console.log(`      📊 Tamaño: ${file.size} bytes`);
                console.log(`      📅 Creado: ${new Date(file.created).toLocaleString()}`);
                console.log(`      📊 Registros: ${file.metadata.recordCount || 'N/A'}`);
            });
        }
        
        console.log(`\n📊 Total archivos encontrados: ${gpsFiles.length + mobileFiles.length}`);
        
    } catch (error) {
        console.error('❌ Error listando archivos GCS:', error.message);
        throw error;
    }
}

async function showBigQueryStatus(bigQueryProcessor) {
    try {
        const status = await bigQueryProcessor.getStatus();
        
        console.log('🔍 Estado del BigQuery Processor:');
        console.log(`   Inicializado: ${status.initialized ? '✅' : '❌'}`);
        console.log(`   Modo simulación: ${status.simulationMode ? '🔧' : '☁️'}`);
        console.log(`   Proyecto: ${status.projectId}`);
        console.log(`   Dataset: ${status.datasetId}`);
        console.log(`   Ubicación: ${status.location}`);
        
        if (status.simulationMode) {
            console.log(`   📝 Nota: ${status.note}`);
        } else {
            console.log(`   Credenciales: ${status.credentialsExist ? '✅' : '❌'}`);
            console.log(`   Dataset existe: ${status.datasetExists ? '✅' : '❌'}`);
        }
        
        console.log('📋 Tablas configuradas:');
        console.log(`   GPS: ${status.tables.gps}`);
        console.log(`   Mobile: ${status.tables.mobile}`);
        
        // Obtener estadísticas de tablas
        try {
            const tableStats = await bigQueryProcessor.getTableStats();
            
            console.log('\n📊 Estadísticas de tablas:');
            if (tableStats.gps && !tableStats.gps.error) {
                console.log(`   GPS (${tableStats.gps.tableName}):`);
                console.log(`      📊 Filas: ${tableStats.gps.numRows.toLocaleString()}`);
                console.log(`      💾 Bytes: ${Math.round(tableStats.gps.numBytes / 1024).toLocaleString()} KB`);
                console.log(`      📅 Modificado: ${new Date(tableStats.gps.lastModified).toLocaleString()}`);
            } else {
                console.log(`   GPS: ${tableStats.gps?.error || 'Error obteniendo estadísticas'}`);
            }
            
            if (tableStats.mobile && !tableStats.mobile.error) {
                console.log(`   Mobile (${tableStats.mobile.tableName}):`);
                console.log(`      📊 Filas: ${tableStats.mobile.numRows.toLocaleString()}`);
                console.log(`      💾 Bytes: ${Math.round(tableStats.mobile.numBytes / 1024).toLocaleString()} KB`);
                console.log(`      📅 Modificado: ${new Date(tableStats.mobile.lastModified).toLocaleString()}`);
            } else {
                console.log(`   Mobile: ${tableStats.mobile?.error || 'Error obteniendo estadísticas'}`);
            }
            
        } catch (statsError) {
            console.log(`   ⚠️ Error obteniendo estadísticas: ${statsError.message}`);
        }
        
    } catch (error) {
        console.error('❌ Error obteniendo estado BigQuery:', error.message);
    }
}

async function processGPSFiles(gcsAdapter, bigQueryProcessor) {
    try {
        // Obtener archivos GPS
        const gpsFiles = await gcsAdapter.listFiles({
            prefix: 'gps-data/',
            dataType: 'gps'
        });
        
        if (gpsFiles.length === 0) {
            console.log('⚠️ No hay archivos GPS para procesar');
            return;
        }
        
        console.log(`📦 Encontrados ${gpsFiles.length} archivos GPS para procesar`);
        
        // Procesar cada archivo GPS
        for (const file of gpsFiles) {
            console.log(`\n🔄 Procesando archivo GPS: ${file.name}`);
            console.log(`   📊 Tamaño: ${file.size} bytes`);
            console.log(`   📊 Registros esperados: ${file.metadata.recordCount || 'N/A'}`);
            
            const startTime = Date.now();
            
            const result = await bigQueryProcessor.processGCSFile(
                file.gcsPath,
                'gps',
                {
                    processingId: file.metadata.processingId || `manual_${Date.now()}`,
                    recordCount: file.metadata.recordCount || 0,
                    originalFileName: file.name
                }
            );
            
            const duration = Date.now() - startTime;
            
            if (result.success) {
                console.log(`✅ Archivo GPS procesado exitosamente:`);
                console.log(`   🆔 Job ID: ${result.jobId}`);
                console.log(`   📊 Registros procesados: ${result.recordsProcessed}`);
                console.log(`   💾 Bytes procesados: ${result.bytesProcessed}`);
                console.log(`   📋 Tabla destino: ${result.tableName}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                console.log(`   📅 Completado: ${new Date(result.completedAt).toLocaleString()}`);
                
                if (result.simulated) {
                    console.log(`   🔧 Modo: Simulación`);
                }
            } else {
                console.log(`❌ Error procesando archivo GPS:`);
                console.log(`   Error: ${result.error}`);
                console.log(`   Job ID: ${result.jobId || 'N/A'}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                
                if (result.errors && result.errors.length > 0) {
                    console.log(`   Errores detallados:`);
                    result.errors.forEach((error, index) => {
                        console.log(`      ${index + 1}. ${error.message || error}`);
                    });
                }
            }
        }
        
    } catch (error) {
        console.error('❌ Error procesando archivos GPS:', error.message);
        throw error;
    }
}

async function processMobileFiles(gcsAdapter, bigQueryProcessor) {
    try {
        // Obtener archivos Mobile
        const mobileFiles = await gcsAdapter.listFiles({
            prefix: 'mobile-data/',
            dataType: 'mobile'
        });
        
        if (mobileFiles.length === 0) {
            console.log('⚠️ No hay archivos Mobile para procesar');
            return;
        }
        
        console.log(`📦 Encontrados ${mobileFiles.length} archivos Mobile para procesar`);
        
        // Procesar cada archivo Mobile
        for (const file of mobileFiles) {
            console.log(`\n🔄 Procesando archivo Mobile: ${file.name}`);
            console.log(`   📊 Tamaño: ${file.size} bytes`);
            console.log(`   📊 Registros esperados: ${file.metadata.recordCount || 'N/A'}`);
            
            const startTime = Date.now();
            
            const result = await bigQueryProcessor.processGCSFile(
                file.gcsPath,
                'mobile',
                {
                    processingId: file.metadata.processingId || `manual_${Date.now()}`,
                    recordCount: file.metadata.recordCount || 0,
                    originalFileName: file.name
                }
            );
            
            const duration = Date.now() - startTime;
            
            if (result.success) {
                console.log(`✅ Archivo Mobile procesado exitosamente:`);
                console.log(`   🆔 Job ID: ${result.jobId}`);
                console.log(`   📊 Registros procesados: ${result.recordsProcessed}`);
                console.log(`   💾 Bytes procesados: ${result.bytesProcessed}`);
                console.log(`   📋 Tabla destino: ${result.tableName}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                console.log(`   📅 Completado: ${new Date(result.completedAt).toLocaleString()}`);
                
                if (result.simulated) {
                    console.log(`   🔧 Modo: Simulación`);
                }
            } else {
                console.log(`❌ Error procesando archivo Mobile:`);
                console.log(`   Error: ${result.error}`);
                console.log(`   Job ID: ${result.jobId || 'N/A'}`);
                console.log(`   ⏱️ Duración: ${duration}ms`);
                
                if (result.errors && result.errors.length > 0) {
                    console.log(`   Errores detallados:`);
                    result.errors.forEach((error, index) => {
                        console.log(`      ${index + 1}. ${error.message || error}`);
                    });
                }
            }
        }
        
    } catch (error) {
        console.error('❌ Error procesando archivos Mobile:', error.message);
        throw error;
    }
}

async function showRecentJobs(bigQueryProcessor) {
    try {
        console.log('🔍 Obteniendo jobs recientes de BigQuery...');
        
        const recentJobs = await bigQueryProcessor.listRecentJobs({
            maxResults: 10
        });
        
        if (recentJobs.length === 0) {
            console.log('⚠️ No se encontraron jobs recientes');
            return;
        }
        
        console.log(`📋 Últimos ${recentJobs.length} jobs:`);
        
        recentJobs.forEach((job, index) => {
            console.log(`\n   ${index + 1}. Job ID: ${job.jobId}`);
            console.log(`      Estado: ${getJobStateIcon(job.state)} ${job.state}`);
            console.log(`      Tipo: ${job.jobType}`);
            console.log(`      Creado: ${new Date(job.createdAt).toLocaleString()}`);
            
            if (job.completedAt) {
                console.log(`      Completado: ${new Date(job.completedAt).toLocaleString()}`);
                const duration = new Date(job.completedAt) - new Date(job.createdAt);
                console.log(`      Duración: ${Math.round(duration / 1000)}s`);
            }
            
            if (job.errors && job.errors.length > 0) {
                console.log(`      ❌ Errores: ${job.errors.length}`);
                job.errors.slice(0, 2).forEach(error => {
                    console.log(`         - ${error.message || error}`);
                });
            }
            
            if (job.simulated) {
                console.log(`      🔧 Simulado`);
            }
        });
        
    } catch (error) {
        console.error('❌ Error obteniendo jobs recientes:', error.message);
    }
}

function getJobStateIcon(state) {
    switch (state) {
        case 'DONE': return '✅';
        case 'RUNNING': return '🔄';
        case 'PENDING': return '⏳';
        case 'ERROR': return '❌';
        default: return '❓';
    }
}

// Ejecutar la prueba
testGCSToBigQuery();