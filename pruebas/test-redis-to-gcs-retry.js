import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import dotenv from 'dotenv';

dotenv.config();

/**
 * Prueba del flujo Redis → GCS con simulación de fallos y reintentos
 * Verifica que el sistema maneja correctamente los fallos y reintenta la subida
 */
async function testRedisToGCSWithRetries() {
    console.log('🔄 Probando flujo Redis → GCS con simulación de fallos y reintentos');
    console.log('📋 Usando keys: gps:history:global y mobile:history:global\n');
    
    let redisRepo, gcsAdapter, backupManager;
    
    try {
        // 1. Inicializar servicios
        console.log('📋 Paso 1: Inicializando servicios...');
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        backupManager = new BackupManager();
        
        await redisRepo.initialize();
        console.log('✅ Redis inicializado');
        console.log('✅ GCS Adapter listo');
        console.log('✅ Backup Manager listo');
        
        // 2. Agregar datos de prueba a Redis
        console.log('\n📋 Paso 2: Agregando datos de prueba a Redis...');
        await addTestDataToRedis(redisRepo);
        
        // 3. Simular fallo en GCS y crear backup local
        console.log('\n🔄 === SIMULANDO FALLO EN GCS ===');
        await simulateGCSFailureAndBackup(redisRepo, gcsAdapter, backupManager);
        
        // 4. Verificar backups pendientes
        console.log('\n📊 === VERIFICANDO BACKUPS PENDIENTES ===');
        await verifyPendingBackups(backupManager);
        
        // 5. Simular reintentos con fallos parciales
        console.log('\n🔄 === SIMULANDO REINTENTOS CON FALLOS ===');
        await simulateRetriesWithPartialFailures(backupManager, gcsAdapter);
        
        // 6. Reintento exitoso final
        console.log('\n✅ === REINTENTO EXITOSO FINAL ===');
        await simulateSuccessfulRetry(backupManager, gcsAdapter);
        
        // 7. Verificar estado final
        console.log('\n📊 === VERIFICACIÓN FINAL ===');
        await verifyFinalState(redisRepo, gcsAdapter, backupManager);
        
        console.log('\n🎉 ¡Prueba de reintentos Redis → GCS completada exitosamente!');
        
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
        // Datos GPS de prueba
        const gpsTestData = [
            {
                deviceId: 'GPS_RETRY_001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,
                lng: -77.0428,
                testType: 'retry_simulation'
            },
            {
                deviceId: 'GPS_RETRY_002',
                timestamp: new Date(Date.now() - 30000).toISOString(),
                lat: -12.0500,
                lng: -77.0450,
                testType: 'retry_simulation'
            }
        ];
        
        // Datos Mobile de prueba
        const mobileTestData = [
            {
                userId: 'USER_RETRY_001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,
                lng: -77.0428,
                name: 'Inspector Retry Test',
                email: 'retry.test@lima.gob.pe',
                testType: 'retry_simulation'
            }
        ];
        
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        console.log(`📝 Agregando ${gpsTestData.length} registros GPS a: ${gpsKey}`);
        await redisRepo.addMultipleToList(gpsKey, gpsTestData);
        
        console.log(`📝 Agregando ${mobileTestData.length} registros Mobile a: ${mobileKey}`);
        await redisRepo.addMultipleToList(mobileKey, mobileTestData);
        
        const gpsCount = await redisRepo.getListLength(gpsKey);
        const mobileCount = await redisRepo.getListLength(mobileKey);
        
        console.log(`✅ GPS en Redis: ${gpsCount} registros`);
        console.log(`✅ Mobile en Redis: ${mobileCount} registros`);
        
    } catch (error) {
        console.error('❌ Error agregando datos de prueba:', error.message);
        throw error;
    }
}

async function simulateGCSFailureAndBackup(redisRepo, gcsAdapter, backupManager) {
    try {
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        // Obtener datos de Redis
        const gpsData = await redisRepo.getListData(gpsKey);
        const mobileData = await redisRepo.getListData(mobileKey);
        
        console.log(`📦 Obtenidos ${gpsData.length} registros GPS y ${mobileData.length} registros Mobile`);
        
        // Simular fallo en GCS creando un mock que siempre falla
        const originalUploadJSONLines = gcsAdapter.uploadJSONLines;
        let failureCount = 0;
        
        gcsAdapter.uploadJSONLines = async (jsonLines, fileName, metadata) => {
            failureCount++;
            console.log(`💥 Simulando fallo ${failureCount} en GCS upload: ${fileName}`);
            return {
                success: false,
                error: `Simulated GCS failure #${failureCount}: Network timeout`,
                fileName
            };
        };
        
        // Intentar procesar GPS (fallará y creará backup)
        console.log('\n🔄 Procesando datos GPS (simulando fallo)...');
        const gpsBackupResult = await processDataWithFailureHandling(
            gpsData, 'gps', gcsAdapter, backupManager, 'gps-data/'
        );
        
        if (gpsBackupResult.backupCreated) {
            console.log(`✅ Backup GPS creado: ${gpsBackupResult.backupId}`);
        }
        
        // Intentar procesar Mobile (fallará y creará backup)
        console.log('\n🔄 Procesando datos Mobile (simulando fallo)...');
        const mobileBackupResult = await processDataWithFailureHandling(
            mobileData, 'mobile', gcsAdapter, backupManager, 'mobile-data/'
        );
        
        if (mobileBackupResult.backupCreated) {
            console.log(`✅ Backup Mobile creado: ${mobileBackupResult.backupId}`);
        }
        
        // Limpiar datos de Redis ya que se crearon backups
        console.log('\n🗑️ Limpiando datos de Redis después de crear backups...');
        await redisRepo.clearListData(gpsKey);
        await redisRepo.clearListData(mobileKey);
        console.log('✅ Datos limpiados de Redis');
        
        // Restaurar función original
        gcsAdapter.uploadJSONLines = originalUploadJSONLines;
        
    } catch (error) {
        console.error('❌ Error simulando fallo en GCS:', error.message);
        throw error;
    }
}

async function processDataWithFailureHandling(data, type, gcsAdapter, backupManager, gcsPrefix) {
    try {
        if (data.length === 0) {
            console.log(`⚠️ No hay datos ${type} para procesar`);
            return { backupCreated: false };
        }
        
        // Crear nombre de archivo
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
        const processingId = `${type}_retry_test_${Date.now()}`;
        const fileName = `${gcsPrefix}${timestamp}_${processingId}.json`;
        
        // Convertir a JSON Lines
        const jsonLines = data.map(item => JSON.stringify(item)).join('\n');
        
        console.log(`📤 Intentando subir archivo ${type}: ${fileName}`);
        
        // Intentar subir a GCS (fallará por el mock)
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: type,
            processingId: processingId,
            recordCount: data.length,
            sourceKey: `${type}:history:global`
        });
        
        if (!gcsResult.success) {
            console.log(`💥 Fallo en GCS como esperado: ${gcsResult.error}`);
            
            // Crear backup local
            console.log(`💾 Creando backup local para ${type}...`);
            const backupResult = await backupManager.saveToLocalBackup(data, type, {
                originalFailureReason: gcsResult.error,
                originalFileName: fileName,
                processingId: processingId
            });
            
            if (backupResult.success) {
                console.log(`✅ Backup local creado: ${backupResult.backupId}`);
                return {
                    backupCreated: true,
                    backupId: backupResult.backupId,
                    filePath: backupResult.filePath
                };
            } else {
                throw new Error(`Error creando backup: ${backupResult.error}`);
            }
        }
        
        return { backupCreated: false };
        
    } catch (error) {
        console.error(`❌ Error procesando datos ${type}:`, error.message);
        throw error;
    }
}

async function verifyPendingBackups(backupManager) {
    try {
        const pendingBackups = await backupManager.getLocalBackupFiles();
        
        console.log(`📋 Backups pendientes encontrados: ${pendingBackups.length}`);
        
        for (const backup of pendingBackups) {
            console.log(`📄 Backup: ${backup.id}`);
            console.log(`   Tipo: ${backup.type}`);
            console.log(`   Registros: ${backup.data?.length || 0}`);
            console.log(`   Reintentos: ${backup.metadata.retryCount}/${backup.metadata.maxRetries}`);
            console.log(`   Estado: ${backup.status}`);
            console.log(`   Edad: ${Math.round((Date.now() - new Date(backup.timestamp).getTime()) / 1000)}s`);
        }
        
        if (pendingBackups.length === 0) {
            console.log('⚠️ No se encontraron backups pendientes');
        }
        
    } catch (error) {
        console.error('❌ Error verificando backups pendientes:', error.message);
        throw error;
    }
}

async function simulateRetriesWithPartialFailures(backupManager, gcsAdapter) {
    try {
        const pendingBackups = await backupManager.getLocalBackupFiles();
        
        if (pendingBackups.length === 0) {
            console.log('⚠️ No hay backups pendientes para reintentar');
            return;
        }
        
        // Crear mock que falla las primeras 2 veces, luego tiene éxito
        let attemptCount = 0;
        const originalUploadJSONLines = gcsAdapter.uploadJSONLines;
        
        gcsAdapter.uploadJSONLines = async (jsonLines, fileName, metadata) => {
            attemptCount++;
            
            if (attemptCount <= 2) {
                console.log(`💥 Simulando fallo ${attemptCount}/2 en reintento: ${fileName}`);
                return {
                    success: false,
                    error: `Simulated retry failure #${attemptCount}: Connection refused`,
                    fileName
                };
            } else {
                console.log(`✅ Simulando éxito en intento ${attemptCount}: ${fileName}`);
                // Llamar a la función original para simular éxito real
                return await originalUploadJSONLines.call(gcsAdapter, jsonLines, fileName, metadata);
            }
        };
        
        // Función mock para upload que simula el comportamiento real
        const mockGcsUpload = async (data, type) => {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const processingId = `retry_success_${Date.now()}`;
            const fileName = `${type}-data/${timestamp}_${processingId}.json`;
            const jsonLines = data.map(item => JSON.stringify(item)).join('\n');
            
            return await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: type,
                processingId: processingId,
                recordCount: data.length,
                retryAttempt: true
            });
        };
        
        // Procesar cada backup pendiente
        for (const backup of pendingBackups) {
            console.log(`\n🔄 Procesando backup: ${backup.id} (${backup.type})`);
            
            // Simular 2 fallos seguidos
            for (let i = 1; i <= 2; i++) {
                console.log(`\n   Intento ${i + backup.metadata.retryCount}/3:`);
                
                const result = await backupManager.processLocalBackupFile(backup, mockGcsUpload);
                
                if (result.success) {
                    console.log(`   ✅ Éxito inesperado en intento ${i}`);
                    break;
                } else {
                    console.log(`   💥 Fallo esperado: ${result.error}`);
                    console.log(`   📊 Reintentos: ${result.retryCount}/${result.maxRetries}`);
                    console.log(`   🔄 Reintentará: ${result.willRetry ? 'Sí' : 'No'}`);
                }
                
                // Pequeña pausa entre reintentos
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        
        // Restaurar función original
        gcsAdapter.uploadJSONLines = originalUploadJSONLines;
        
    } catch (error) {
        console.error('❌ Error simulando reintentos con fallos:', error.message);
        throw error;
    }
}

async function simulateSuccessfulRetry(backupManager, gcsAdapter) {
    try {
        const pendingBackups = await backupManager.getLocalBackupFiles();
        
        console.log(`📋 Backups pendientes para reintento final: ${pendingBackups.length}`);
        
        if (pendingBackups.length === 0) {
            console.log('✅ No hay backups pendientes, todos fueron procesados');
            return;
        }
        
        // Función de upload que siempre tiene éxito
        const successfulGcsUpload = async (data, type) => {
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const processingId = `final_success_${Date.now()}`;
            const fileName = `${type}-data/${timestamp}_${processingId}.json`;
            const jsonLines = data.map(item => JSON.stringify(item)).join('\n');
            
            console.log(`☁️ Subiendo exitosamente: ${fileName}`);
            
            const result = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
                dataType: type,
                processingId: processingId,
                recordCount: data.length,
                finalRetryAttempt: true
            });
            
            if (result.success) {
                console.log(`✅ Upload exitoso: ${result.gcsUri}`);
            }
            
            return result;
        };
        
        // Procesar todos los backups pendientes
        for (const backup of pendingBackups) {
            console.log(`\n🔄 Reintento final para backup: ${backup.id} (${backup.type})`);
            console.log(`   Registros: ${backup.data?.length || 0}`);
            console.log(`   Reintentos previos: ${backup.metadata.retryCount}/${backup.metadata.maxRetries}`);
            
            const result = await backupManager.processLocalBackupFile(backup, successfulGcsUpload);
            
            if (result.success) {
                console.log(`✅ Backup procesado exitosamente: ${backup.id}`);
                console.log(`   📁 Archivo GCS: ${result.gcsFile}`);
                console.log(`   📊 Registros procesados: ${result.recordsProcessed}`);
                console.log(`   🔄 Intento final: ${result.attempt}`);
                console.log(`   ⏱️ Duración upload: ${result.uploadDuration}ms`);
                console.log(`   ⏱️ Duración total: ${result.totalDuration}ms`);
            } else {
                console.log(`❌ Backup falló definitivamente: ${backup.id}`);
                console.log(`   Error: ${result.error}`);
                console.log(`   Reintentos: ${result.retryCount}/${result.maxRetries}`);
            }
        }
        
    } catch (error) {
        console.error('❌ Error en reintento exitoso final:', error.message);
        throw error;
    }
}

async function verifyFinalState(redisRepo, gcsAdapter, backupManager) {
    try {
        console.log('🔍 Verificando estado final después de reintentos...');
        
        // Verificar Redis
        const gpsKey = 'gps:history:global';
        const mobileKey = 'mobile:history:global';
        
        const gpsRemaining = await redisRepo.getListLength(gpsKey);
        const mobileRemaining = await redisRepo.getListLength(mobileKey);
        
        console.log(`📊 Redis GPS restantes: ${gpsRemaining}`);
        console.log(`📊 Redis Mobile restantes: ${mobileRemaining}`);
        
        // Verificar backups pendientes
        const pendingBackups = await backupManager.getLocalBackupFiles();
        console.log(`📋 Backups pendientes restantes: ${pendingBackups.length}`);
        
        // Verificar GCS
        const bucketStats = await gcsAdapter.getBucketStats();
        console.log(`☁️ Total archivos en GCS: ${bucketStats.totalFiles}`);
        console.log(`💾 Tamaño total GCS: ${Math.round(bucketStats.totalSize / 1024)} KB`);
        console.log(`📁 Archivos por tipo:`, bucketStats.filesByType);
        
        // Verificar conexión Redis
        const redisConnected = await redisRepo.ping();
        console.log(`🔗 Redis conectado: ${redisConnected ? '✅' : '❌'}`);
        
        // Mostrar resumen de la prueba
        console.log('\n📊 === RESUMEN DE LA PRUEBA ===');
        console.log('✅ Simulación de fallos en GCS: Completada');
        console.log('✅ Creación de backups locales: Completada');
        console.log('✅ Reintentos con fallos parciales: Completada');
        console.log('✅ Reintento exitoso final: Completada');
        console.log(`📋 Backups procesados exitosamente: ${pendingBackups.length === 0 ? 'Todos' : 'Parcial'}`);
        
    } catch (error) {
        console.error('❌ Error verificando estado final:', error.message);
    }
}

// Ejecutar la prueba
testRedisToGCSWithRetries();