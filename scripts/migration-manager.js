#!/usr/bin/env node

/**
 * Script de gestión de migración gradual
 * Permite cambiar fases, monitorear métricas y ejecutar rollbacks
 */

import { migrationConfig } from '../src/config/migrationConfig.js';
import { migrationMetrics } from '../src/utils/MigrationMetrics.js';
import { rollbackManager } from '../src/utils/RollbackManager.js';
import { logger } from '../src/utils/logger.js';

class MigrationManager {
  constructor() {
    this.baseUrl = process.env.API_BASE_URL || 'http://localhost:3003';
    this.apiBasePath = process.env.API_BASE_PATH || '/api/v3';
    this.fullApiUrl = `${this.baseUrl}${this.apiBasePath}`;
  }

  /**
   * Realiza una petición HTTP a la API
   */
  async makeApiRequest(endpoint, method = 'GET', data = null) {
    try {
      const url = `${this.fullApiUrl}${endpoint}`;
      const options = {
        method,
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': 'Migration-Manager/1.0'
        }
      };

      if (data && (method === 'POST' || method === 'PUT')) {
        options.body = JSON.stringify(data);
      }

      const response = await fetch(url, options);
      const result = await response.json();

      if (!response.ok) {
        throw new Error(`API Error: ${response.status} - ${result.message || 'Unknown error'}`);
      }

      return result;
    } catch (error) {
      if (error.code === 'ECONNREFUSED') {
        throw new Error(`No se puede conectar al servidor en ${this.baseUrl}. ¿Está ejecutándose el servicio?`);
      }
      throw error;
    }
  }

  /**
   * Muestra el estado actual de la migración
   */
  async showStatus() {
    try {
      console.log('📊 === ESTADO DE MIGRACIÓN ===');
      console.log(`🔗 API URL: ${this.fullApiUrl}`);
      
      // Intentar obtener estado desde la API primero
      let status;
      try {
        const apiStatus = await this.makeApiRequest('/hybrid/status');
        status = apiStatus.migration || migrationConfig.getStatus();
        console.log('✅ Estado obtenido desde API');
      } catch (apiError) {
        console.warn('⚠️ No se pudo conectar a la API, usando configuración local');
        status = migrationConfig.getStatus();
      }
      
      console.log(`🔄 Fase actual: ${status.currentPhase}`);
      console.log(`🆕 Nuevo flujo habilitado: ${status.newFlowEnabled}`);
      console.log(`🔄 Modo híbrido: ${status.hybridMode}`);
      console.log(`🔙 Rollback habilitado: ${status.rollbackEnabled}`);
      console.log(`📊 Comparación habilitada: ${status.comparisonEnabled}`);
      
      // Mostrar métricas recientes
      const metrics = migrationMetrics.getStats();
      console.log('\n📈 === MÉTRICAS RECIENTES ===');
      
      if (metrics.legacy.totalExecutions > 0) {
        console.log(`Legacy: ${metrics.legacy.totalExecutions} ejecuciones, ${(metrics.legacy.errorRate * 100).toFixed(1)}% errores`);
        console.log(`  Tiempo promedio: ${metrics.legacy.averageProcessingTime}ms`);
        console.log(`  Registros promedio: ${metrics.legacy.averageRecordsProcessed}`);
      }
      
      if (metrics.newFlow.totalExecutions > 0) {
        console.log(`Nuevo: ${metrics.newFlow.totalExecutions} ejecuciones, ${(metrics.newFlow.errorRate * 100).toFixed(1)}% errores`);
        console.log(`  Tiempo promedio: ${metrics.newFlow.averageProcessingTime}ms`);
        console.log(`  Registros promedio: ${metrics.newFlow.averageRecordsProcessed}`);
      }
      
      if (metrics.comparison.performanceRatio !== 1.0) {
        console.log(`\n⚡ Ratio de rendimiento: ${metrics.comparison.performanceRatio.toFixed(2)}x`);
        console.log(`🎯 Ratio de confiabilidad: ${metrics.comparison.reliabilityRatio.toFixed(2)}x`);
      }
      
      // Mostrar estado de rollback
      const rollbackStatus = rollbackManager.getStatus();
      console.log('\n🔙 === ESTADO DE ROLLBACK ===');
      console.log(`Habilitado: ${rollbackStatus.enabled}`);
      console.log(`En cooldown: ${rollbackStatus.inCooldown}`);
      console.log(`Rollbacks totales: ${rollbackStatus.rollbackCount}`);
      
      if (rollbackStatus.recentReasons.length > 0) {
        console.log('Razones recientes:');
        rollbackStatus.recentReasons.forEach(reason => {
          console.log(`  - ${reason}`);
        });
      }
      
    } catch (error) {
      console.error('❌ Error obteniendo estado:', error.message);
      process.exit(1);
    }
  }

  /**
   * Cambia la fase de migración
   */
  async changePhase(newPhase) {
    try {
      const validPhases = ['legacy', 'hybrid', 'migration', 'new', 'rollback'];
      
      if (!validPhases.includes(newPhase)) {
        console.error(`❌ Fase inválida: ${newPhase}`);
        console.error(`Fases válidas: ${validPhases.join(', ')}`);
        process.exit(1);
      }
      
      const currentPhase = migrationConfig.getConfig().migrationPhase;
      
      if (currentPhase === newPhase) {
        console.log(`ℹ️ Ya estás en la fase: ${newPhase}`);
        return;
      }
      
      console.log(`🔄 Cambiando de fase ${currentPhase} → ${newPhase}...`);
      
      // Validar transición
      if (!this.isValidTransition(currentPhase, newPhase)) {
        console.warn(`⚠️ Transición ${currentPhase} → ${newPhase} no recomendada`);
        console.log('¿Continuar? (y/N)');
        
        // En un script real, aquí se leería input del usuario
        // Para este ejemplo, continuamos
      }
      
      migrationConfig.setMigrationPhase(newPhase);
      
      // Configurar flags adicionales según la fase
      switch (newPhase) {
        case 'legacy':
          migrationConfig.setNewFlowEnabled(false);
          break;
        case 'hybrid':
          migrationConfig.setNewFlowEnabled(true);
          break;
        case 'migration':
        case 'new':
          migrationConfig.setNewFlowEnabled(true);
          break;
      }
      
      console.log(`✅ Fase cambiada exitosamente a: ${newPhase}`);
      console.log('🔄 Reinicia el servicio para aplicar los cambios');
      
    } catch (error) {
      console.error('❌ Error cambiando fase:', error.message);
      process.exit(1);
    }
  }

  /**
   * Valida si una transición de fase es válida
   */
  isValidTransition(from, to) {
    const validTransitions = {
      'legacy': ['hybrid'],
      'hybrid': ['migration', 'legacy'],
      'migration': ['new', 'hybrid'],
      'new': ['rollback'],
      'rollback': ['legacy', 'hybrid']
    };
    
    return validTransitions[from]?.includes(to) || false;
  }

  /**
   * Ejecuta rollback manual
   */
  async executeRollback(reason = 'Rollback manual solicitado') {
    try {
      console.log(`🚨 Ejecutando rollback manual: ${reason}`);
      
      const result = await rollbackManager.forceRollback(reason);
      
      if (result.success) {
        console.log(`✅ Rollback ejecutado exitosamente`);
        console.log(`📋 Nueva fase: ${result.newPhase}`);
        console.log(`🔄 Reinicia el servicio para aplicar los cambios`);
      } else {
        console.error(`❌ Error ejecutando rollback: ${result.error}`);
        process.exit(1);
      }
      
    } catch (error) {
      console.error('❌ Error ejecutando rollback:', error.message);
      process.exit(1);
    }
  }

  /**
   * Monitorea métricas en tiempo real
   */
  async monitor(intervalSeconds = 30) {
    console.log(`📊 Monitoreando métricas cada ${intervalSeconds} segundos...`);
    console.log('Presiona Ctrl+C para detener');
    
    const interval = setInterval(async () => {
      try {
        console.clear();
        console.log(`📊 === MONITOREO EN TIEMPO REAL === ${new Date().toLocaleTimeString()}`);
        
        await this.showStatus();
        
        // Verificar condiciones de rollback
        const rollbackCheck = migrationMetrics.shouldTriggerRollback();
        if (rollbackCheck.shouldRollback) {
          console.log(`\n🚨 ALERTA: Condición de rollback detectada!`);
          console.log(`Razón: ${rollbackCheck.reason}`);
          console.log(`Trigger: ${rollbackCheck.trigger}`);
        }
        
      } catch (error) {
        console.error('❌ Error en monitoreo:', error.message);
      }
    }, intervalSeconds * 1000);
    
    // Manejar Ctrl+C
    process.on('SIGINT', () => {
      clearInterval(interval);
      console.log('\n👋 Monitoreo detenido');
      process.exit(0);
    });
  }

  /**
   * Genera reporte de migración
   */
  async generateReport() {
    try {
      console.log('📋 === REPORTE DE MIGRACIÓN ===');
      
      const status = migrationConfig.getStatus();
      const metrics = migrationMetrics.getStats();
      const rollbackStatus = rollbackManager.getStatus();
      
      const report = {
        timestamp: new Date().toISOString(),
        migration: status,
        metrics: {
          legacy: {
            executions: metrics.legacy.totalExecutions,
            successRate: ((metrics.legacy.totalExecutions - metrics.legacy.failedExecutions) / metrics.legacy.totalExecutions * 100).toFixed(1) + '%',
            avgProcessingTime: metrics.legacy.averageProcessingTime + 'ms',
            avgRecordsProcessed: metrics.legacy.averageRecordsProcessed
          },
          newFlow: {
            executions: metrics.newFlow.totalExecutions,
            successRate: ((metrics.newFlow.totalExecutions - metrics.newFlow.failedExecutions) / metrics.newFlow.totalExecutions * 100).toFixed(1) + '%',
            avgProcessingTime: metrics.newFlow.averageProcessingTime + 'ms',
            avgRecordsProcessed: metrics.newFlow.averageRecordsProcessed
          },
          comparison: {
            performanceRatio: metrics.comparison.performanceRatio,
            reliabilityRatio: metrics.comparison.reliabilityRatio,
            recommendations: metrics.comparison.recommendations
          }
        },
        rollback: {
          enabled: rollbackStatus.enabled,
          totalRollbacks: rollbackStatus.rollbackCount,
          inCooldown: rollbackStatus.inCooldown,
          recentReasons: rollbackStatus.recentReasons
        }
      };
      
      console.log(JSON.stringify(report, null, 2));
      
      // Guardar reporte en archivo
      const fs = await import('fs/promises');
      const reportPath = `tmp/migration-report-${Date.now()}.json`;
      await fs.writeFile(reportPath, JSON.stringify(report, null, 2));
      console.log(`\n💾 Reporte guardado en: ${reportPath}`);
      
    } catch (error) {
      console.error('❌ Error generando reporte:', error.message);
      process.exit(1);
    }
  }

  /**
   * Muestra ayuda
   */
  showHelp() {
    console.log(`
📋 === GESTOR DE MIGRACIÓN GRADUAL ===

Uso: node scripts/migration-manager.js <comando> [opciones]

Comandos disponibles:

  status                    Muestra el estado actual de migración
  phase <fase>             Cambia la fase de migración
  rollback [razón]         Ejecuta rollback manual
  monitor [intervalo]      Monitorea métricas en tiempo real
  report                   Genera reporte de migración
  help                     Muestra esta ayuda

Fases válidas:
  legacy                   Solo flujo legacy
  hybrid                   Ambos flujos para comparación
  migration               Nuevo flujo con rollback automático
  new                     Solo nuevo flujo
  rollback                Rollback temporal

Ejemplos:
  node scripts/migration-manager.js status
  node scripts/migration-manager.js phase hybrid
  node scripts/migration-manager.js rollback "Problemas de rendimiento"
  node scripts/migration-manager.js monitor 60
  node scripts/migration-manager.js report
`);
  }
}

// Función principal
async function main() {
  const manager = new MigrationManager();
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    manager.showHelp();
    return;
  }
  
  const command = args[0];
  
  try {
    switch (command) {
      case 'status':
        await manager.showStatus();
        break;
        
      case 'phase':
        if (args.length < 2) {
          console.error('❌ Especifica la nueva fase');
          process.exit(1);
        }
        await manager.changePhase(args[1]);
        break;
        
      case 'rollback':
        const reason = args[1] || 'Rollback manual solicitado';
        await manager.executeRollback(reason);
        break;
        
      case 'monitor':
        const interval = parseInt(args[1]) || 30;
        await manager.monitor(interval);
        break;
        
      case 'report':
        await manager.generateReport();
        break;
        
      case 'help':
      case '--help':
      case '-h':
        manager.showHelp();
        break;
        
      default:
        console.error(`❌ Comando desconocido: ${command}`);
        manager.showHelp();
        process.exit(1);
    }
  } catch (error) {
    console.error('❌ Error ejecutando comando:', error.message);
    process.exit(1);
  }
}

// Ejecutar si es el módulo principal
if (process.argv[1].endsWith('migration-manager.js')) {
  main().catch(error => {
    console.error('❌ Error fatal:', error.message);
    process.exit(1);
  });
}

export { MigrationManager };