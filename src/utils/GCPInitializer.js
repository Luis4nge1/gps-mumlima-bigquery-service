import { gcpConfig } from '../config/gcpConfig.js';

/**
 * Utilidad para inicializar configuración GCP
 */
export class GCPInitializer {
  constructor() {
    this.config = gcpConfig;
  }

  /**
   * Inicializa la configuración GCP completa
   */
  async initialize() {
    console.log('🚀 Inicializando configuración GCP...');
    
    try {
      // Validar configuración
      const validation = this.config.validateAndThrow();
      
      // Inicializar directorios de simulación si es necesario
      if (this.config.isSimulationMode()) {
        this.config.initializeSimulationDirectories();
        console.log('📁 Directorios de simulación inicializados');
      }

      // Mostrar resumen de configuración
      this.logConfigurationSummary(validation);
      
      return {
        success: true,
        mode: validation.mode,
        message: 'Configuración GCP inicializada correctamente'
      };

    } catch (error) {
      console.error('❌ Error al inicializar configuración GCP:', error.message);
      
      // Proporcionar sugerencias de solución
      this.logTroubleshootingTips(error);
      
      return {
        success: false,
        error: error.message,
        message: 'Error en inicialización GCP'
      };
    }
  }

  /**
   * Muestra resumen de configuración
   */
  logConfigurationSummary(validation) {
    const status = this.config.getStatus();
    
    console.log('\n📋 Resumen de Configuración GCP:');
    console.log(`   Modo: ${validation.mode}`);
    console.log(`   Proyecto: ${status.projectId}`);
    console.log(`   Bucket GCS: ${status.gcs.bucketName}`);
    console.log(`   Dataset BigQuery: ${status.bigQuery.datasetId}`);
    
    if (status.simulationMode) {
      console.log('   🔧 Ejecutándose en modo simulación');
    } else {
      console.log(`   🌐 Conectado a GCP (${validation.clientEmail})`);
    }
  }

  /**
   * Muestra consejos de solución de problemas
   */
  logTroubleshootingTips(error) {
    console.log('\n💡 Consejos para solucionar el problema:');
    
    if (error.message.includes('service-account.json')) {
      console.log('   1. Coloca tu archivo service-account.json en la raíz del proyecto');
      console.log('   2. O configura GOOGLE_APPLICATION_CREDENTIALS con la ruta correcta');
      console.log('   3. O activa el modo simulación: GCP_SIMULATION_MODE=true');
    }
    
    if (error.message.includes('Variables de entorno')) {
      console.log('   1. Configura las variables de entorno requeridas en .env');
      console.log('   2. Copia .env.example a .env y completa los valores');
      console.log('   3. Ejecuta: npm run validate-gcp para verificar');
    }
    
    if (error.message.includes('inválido')) {
      console.log('   1. Verifica que el service-account.json sea válido');
      console.log('   2. Descarga un nuevo archivo desde Google Cloud Console');
      console.log('   3. Asegúrate de que tenga los permisos necesarios');
    }
    
    console.log('\n🔍 Para más detalles ejecuta: npm run validate-gcp');
  }

  /**
   * Verifica si la configuración está lista para usar
   */
  isReady() {
    try {
      this.config.validateAndThrow();
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Obtiene información de estado para health checks
   */
  getHealthStatus() {
    const status = this.config.getStatus();
    
    return {
      gcp: {
        configured: status.credentialsValid || status.simulationMode,
        mode: status.simulationMode ? 'simulation' : 'production',
        project: status.projectId,
        credentials: status.credentialsValid ? 'valid' : 'invalid',
        services: {
          gcs: {
            bucket: status.gcs.bucketName,
            region: status.gcs.region
          },
          bigquery: {
            dataset: status.bigQuery.datasetId,
            location: status.bigQuery.location
          }
        }
      }
    };
  }

  /**
   * Fallback graceful cuando faltan credenciales
   */
  async gracefulFallback() {
    if (this.isReady()) {
      return { fallback: false, message: 'Configuración GCP lista' };
    }

    console.warn('⚠️  Configuración GCP no disponible, usando fallback');
    
    // Activar modo simulación automáticamente
    process.env.GCP_SIMULATION_MODE = 'true';
    
    try {
      // Reinicializar configuración en modo simulación
      const gcpModule = await import('../config/gcpConfig.js');
      const newConfig = new gcpModule.GCPConfig();
      newConfig.initializeSimulationDirectories();
      
      console.log('🔧 Modo simulación activado automáticamente');
      
      return {
        fallback: true,
        mode: 'simulation',
        message: 'Fallback a modo simulación activado'
      };
      
    } catch (error) {
      console.error('❌ Error en fallback graceful:', error.message);
      
      return {
        fallback: true,
        mode: 'disabled',
        message: 'GCP deshabilitado - funcionalidad limitada'
      };
    }
  }
}

/**
 * Instancia singleton del inicializador
 */
export const gcpInitializer = new GCPInitializer();

/**
 * Función de conveniencia para inicializar GCP
 */
export async function initializeGCP() {
  return await gcpInitializer.initialize();
}

/**
 * Función de conveniencia para verificar estado
 */
export function getGCPHealthStatus() {
  return gcpInitializer.getHealthStatus();
}