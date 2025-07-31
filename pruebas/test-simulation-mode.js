import { RedisRepository } from '../src/repositories/RedisRepository.js';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';
import dotenv from 'dotenv';

dotenv.config();

async function testSimulationMode() {
    console.log('🔧 PROBANDO MODO SIMULACIÓN vs MODO REAL');
    console.log(`📋 Modo actual: ${process.env.GCP_SIMULATION_MODE === 'true' ? '🔧 SIMULACIÓN' : '🌐 REAL'}\n`);
    
    let redisRepo, gcsAdapter, bigQueryProcessor;
    
    try {
        // Inicializar servicios
        redisRepo = new RedisRepository();
        gcsAdapter = new GCSAdapter();
        bigQueryProcessor = new BigQueryBatchProcessor();
        
        await redisRepo.initialize();
        
        // Mostrar estado de simulación
        console.log('📊 === ESTADO DE SIMULACIÓN ===');
        await showSimulationStatus(gcsAdapter, bigQueryProcessor);
        
        // Agregar datos de prueba
        console.log('\n📝 Agregando datos de prueba...');
        const testData = [
            {
                deviceId: 'SIM001',
                timestamp: new Date().toISOString(),
                lat: -12.0464,
                lng: -77.0428
            }
        ];
        
        await redisRepo.addMultipleToList('simulation_test', testData);
        
        // Probar upload GCS
        console.log('\n☁️ === PROBANDO GCS ===');
        const jsonLines = testData.map(item => JSON.stringify(item)).join('\n');
        const fileName = `simulation_test_${Date.now()}.json`;
        
        const gcsResult = await gcsAdapter.uploadJSONLines(jsonLines, fileName, {
            dataType: 'simulation',
            processingId: `sim_${Date.now()}`
        });
        
        if (gcsResult.success) {
            console.log(`✅ Upload exitoso: ${gcsResult.gcsUri}`);
            console.log(`📊 Tamaño: ${gcsResult.fileSize} bytes`);
            console.log(`🔧 Simulado: ${gcsResult.simulated ? 'SÍ' : 'NO'}`);
            if (gcsResult.localPath) {
                console.log(`📁 Archivo local: ${gcsResult.localPath}`);
            }
        }
        
        // Probar BigQuery
        console.log('\n📊 === PROBANDO BIGQUERY ===');
        const bqResult = await bigQueryProcessor.processGCSFile(
            gcsResult.gcsUri,
            'gps',
            { processingId: `sim_${Date.now()}`, recordCount: 1 }
        );
        
        if (bqResult.success) {
            console.log(`✅ BigQuery exitoso: ${bqResult.jobId}`);
            console.log(`📊 Registros: ${bqResult.recordsProcessed}`);
            console.log(`🔧 Simulado: ${bqResult.simulated ? 'SÍ' : 'NO'}`);
        }
        
        // Mostrar archivos creados
        console.log('\n📁 === ARCHIVOS CREADOS ===');
        await showCreatedFiles();
        
        // Limpiar
        await redisRepo.clearListData('simulation_test');
        
    } catch (error) {
        console.error('❌ Error:', error.message);
    } finally {
        if (redisRepo) await redisRepo.disconnect();
        if (gcsAdapter) await gcsAdapter.cleanup();
        if (bigQueryProcessor) await bigQueryProcessor.cleanup();
        process.exit(0);
    }
}

async function showSimulationStatus(gcsAdapter, bigQueryProcessor) {
    try {
        const gcsStatus = await gcsAdapter.getStatus();
        const bqStatus = await bigQueryProcessor.getStatus();
        
        console.log(`☁️ GCS Simulación: ${gcsStatus.simulationMode ? '🔧 ACTIVADA' : '🌐 DESACTIVADA'}`);
        console.log(`📊 BigQuery Simulación: ${bqStatus.simulationMode ? '🔧 ACTIVADA' : '🌐 DESACTIVADA'}`);
        
        if (gcsStatus.simulationMode) {
            console.log(`📁 Ruta simulación GCS: ${gcsStatus.localStoragePath}`);
        }
        
        if (bqStatus.simulationMode) {
            console.log(`📁 Ruta simulación BigQuery: tmp/bigquery-simulation/`);
        }
        
    } catch (error) {
        console.error('❌ Error obteniendo estado:', error.message);
    }
}

async function showCreatedFiles() {
    try {
        const fs = await import('fs/promises');
        const path = await import('path');
        
        // Verificar archivos de simulación GCS
        const gcsSimPath = 'tmp/gcs-simulation';
        try {
            const gcsFiles = await fs.readdir(gcsSimPath);
            console.log(`📁 Archivos GCS simulados (${gcsFiles.length}):`);
            gcsFiles.forEach(file => {
                if (!file.endsWith('.metadata.json')) {
                    console.log(`   📄 ${file}`);
                }
            });
        } catch (error) {
            console.log('📁 No hay archivos GCS simulados');
        }
        
        // Verificar archivos de simulación BigQuery
        const bqSimPath = 'tmp/bigquery-simulation';
        try {
            const bqFiles = await fs.readdir(bqSimPath);
            console.log(`📁 Archivos BigQuery simulados (${bqFiles.length}):`);
            bqFiles.forEach(file => {
                console.log(`   📄 ${file}`);
            });
        } catch (error) {
            console.log('📁 No hay archivos BigQuery simulados');
        }
        
    } catch (error) {
        console.log('📁 Error verificando archivos simulados');
    }
}

// Ejecutar prueba
testSimulationMode();