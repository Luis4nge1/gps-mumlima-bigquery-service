import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { HybridGPSProcessor } from '../src/services/HybridGPSProcessor.js';
import { migrationConfig } from '../src/config/migrationConfig.js';
import { migrationMetrics } from '../src/utils/MigrationMetrics.js';

describe('HybridGPSProcessor', () => {
  let processor;
  let originalConfig;

  before(async () => {
    // Guardar configuración original
    originalConfig = migrationConfig.getConfig();
    
    // Configurar para tests
    process.env.MIGRATION_ENABLED = 'true';
    process.env.MIGRATION_PHASE = 'hybrid';
    process.env.HYBRID_MODE = 'true';
    process.env.COMPARISON_ENABLED = 'true';
    process.env.GCP_SIMULATION_MODE = 'true';
    
    processor = new HybridGPSProcessor();
  });

  after(async () => {
    // Restaurar configuración original
    if (originalConfig) {
      migrationConfig.setMigrationPhase(originalConfig.migrationPhase);
      migrationConfig.setNewFlowEnabled(originalConfig.newFlowEnabled);
    }
    
    if (processor) {
      await processor.cleanup();
    }
  });

  describe('Inicialización', () => {
    it('debe inicializar correctamente en modo híbrido', async () => {
      await processor.initialize();
      
      assert.ok(processor.newFlowProcessor, 'Debe tener procesador nuevo');
      assert.ok(processor.legacyProcessor, 'Debe tener procesador legacy');
    });

    it('debe determinar correctamente el flujo a usar', () => {
      // Configurar para usar nuevo flujo
      migrationConfig.setNewFlowEnabled(true);
      assert.strictEqual(processor.shouldUseLegacyFlow(), false);
      
      // Configurar para usar flujo legacy
      migrationConfig.setNewFlowEnabled(false);
      assert.strictEqual(processor.shouldUseLegacyFlow(), true);
    });
  });

  describe('Procesamiento por flujo', () => {
    before(async () => {
      await processor.initialize();
    });

    it('debe procesar con nuevo flujo cuando está habilitado', async () => {
      migrationConfig.setMigrationPhase('new');
      migrationConfig.setNewFlowEnabled(true);
      
      const result = await processor.processGPSData();
      
      assert.ok(result, 'Debe retornar resultado');
      assert.strictEqual(result.flowType, 'newFlow', 'Debe usar nuevo flujo');
    });

    it('debe procesar con flujo legacy cuando nuevo flujo está deshabilitado', async () => {
      migrationConfig.setMigrationPhase('legacy');
      migrationConfig.setNewFlowEnabled(false);
      
      const result = await processor.processGPSData();
      
      assert.ok(result, 'Debe retornar resultado');
      assert.strictEqual(result.flowType, 'legacy', 'Debe usar flujo legacy');
    });

    it('debe procesar con comparación cuando está habilitado', async () => {
      migrationConfig.setMigrationPhase('hybrid');
      process.env.COMPARISON_ENABLED = 'true';
      
      const result = await processor.processGPSData();
      
      assert.ok(result, 'Debe retornar resultado');
      assert.strictEqual(result.flowType, 'comparison', 'Debe usar modo comparación');
      assert.ok(result.comparison, 'Debe incluir datos de comparación');
      assert.ok(result.comparison.primary, 'Debe tener resultado primario');
      assert.ok(result.comparison.secondary, 'Debe tener resultado secundario');
    });
  });

  describe('Comparación de flujos', () => {
    before(async () => {
      await processor.initialize();
    });

    it('debe comparar ambos flujos correctamente', async () => {
      process.env.COMPARISON_ENABLED = 'true';
      migrationConfig.setMigrationPhase('hybrid');
      
      const result = await processor.processWithComparison();
      
      assert.ok(result.comparison, 'Debe incluir comparación');
      assert.ok(result.comparison.primary, 'Debe tener resultado primario');
      assert.ok(result.comparison.secondary, 'Debe tener resultado secundario');
      assert.ok(result.comparison.consistency, 'Debe incluir verificación de consistencia');
      
      // Verificar estructura de consistencia
      const consistency = result.comparison.consistency;
      assert.ok(typeof consistency.hasIssues === 'boolean', 'Debe indicar si hay problemas');
      assert.ok(Array.isArray(consistency.issues), 'Debe tener array de problemas');
      assert.ok(typeof consistency.recordDifference === 'number', 'Debe tener diferencia de registros');
    });

    it('debe detectar inconsistencias en los datos', async () => {
      // Simular resultados inconsistentes
      const newFlow = {
        success: true,
        recordsProcessed: 100,
        processingTime: 1000,
        flowType: 'newFlow'
      };
      
      const legacy = {
        success: true,
        recordsProcessed: 150, // Diferencia significativa
        processingTime: 800,
        flowType: 'legacy'
      };
      
      const consistency = processor.checkDataConsistency(newFlow, legacy);
      
      assert.strictEqual(consistency.hasIssues, true, 'Debe detectar inconsistencias');
      assert.ok(consistency.issues.length > 0, 'Debe reportar problemas específicos');
      assert.strictEqual(consistency.recordDifference, 50, 'Debe calcular diferencia correcta');
    });
  });

  describe('Gestión de fases', () => {
    it('debe cambiar fase correctamente', async () => {
      await processor.initialize();
      
      const result = await processor.setMigrationPhase('migration');
      
      assert.strictEqual(result.success, true, 'Debe cambiar fase exitosamente');
      assert.strictEqual(result.newPhase, 'migration', 'Debe confirmar nueva fase');
      
      const status = migrationConfig.getStatus();
      assert.strictEqual(status.currentPhase, 'migration', 'Debe actualizar configuración');
    });

    it('debe ejecutar rollback manual', async () => {
      await processor.initialize();
      
      const reason = 'Test rollback manual';
      const result = await processor.executeManualRollback(reason);
      
      // El resultado depende de la implementación del rollback manager
      assert.ok(result, 'Debe retornar resultado de rollback');
      assert.ok(typeof result.success === 'boolean', 'Debe indicar éxito/fallo');
    });
  });

  describe('Health checks', () => {
    before(async () => {
      await processor.initialize();
    });

    it('debe reportar estado de salud correctamente', async () => {
      const health = await processor.healthCheck();
      
      assert.ok(health, 'Debe retornar estado de salud');
      assert.ok(typeof health.healthy === 'boolean', 'Debe indicar si está saludable');
      assert.ok(health.migration, 'Debe incluir estado de migración');
      assert.ok(health.processor, 'Debe incluir estado del procesador');
      assert.ok(health.timestamp, 'Debe incluir timestamp');
    });

    it('debe obtener estadísticas del procesador', async () => {
      const stats = await processor.getProcessorStats();
      
      assert.ok(stats, 'Debe retornar estadísticas');
      assert.ok(stats.migration, 'Debe incluir estado de migración');
      assert.ok(stats.processor, 'Debe incluir estadísticas del procesador');
      assert.strictEqual(stats.processor.hybridMode, true, 'Debe indicar modo híbrido');
    });
  });

  describe('Logging detallado', () => {
    before(async () => {
      await processor.initialize();
      process.env.COMPARISON_LOGGING = 'true';
    });

    it('debe generar logs detallados de comparación', async () => {
      const newFlow = {
        success: true,
        recordsProcessed: 100,
        processingTime: 1000,
        flowType: 'newFlow'
      };
      
      const legacy = {
        success: true,
        recordsProcessed: 100,
        processingTime: 1200,
        flowType: 'legacy'
      };
      
      const comparison = { recommendations: ['Test recommendation'] };
      
      // Esta función debe ejecutarse sin errores
      assert.doesNotThrow(() => {
        processor.logDetailedComparison(newFlow, legacy, comparison, 2200);
      }, 'No debe fallar al generar logs detallados');
    });
  });

  describe('Prevención de procesamiento concurrente', () => {
    before(async () => {
      await processor.initialize();
    });

    it('debe prevenir procesamiento concurrente', async () => {
      // Iniciar primer procesamiento
      const promise1 = processor.processGPSData();
      
      // Intentar segundo procesamiento inmediatamente
      const result2 = await processor.processGPSData();
      
      // El segundo debe fallar por procesamiento en curso
      assert.strictEqual(result2.success, false, 'Segundo procesamiento debe fallar');
      assert.ok(result2.error.includes('already in progress'), 'Debe indicar procesamiento en curso');
      
      // Esperar que termine el primero
      await promise1;
    });
  });
});