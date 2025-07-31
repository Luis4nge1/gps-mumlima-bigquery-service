import { test, describe, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Importar después de configurar el entorno
let GCPConfig, gcpConfig, validateGCPConfig, getGCPConfig, getGCPStatus;

describe('GCPConfig', () => {
  let originalEnv;
  let testServiceAccountPath;
  let validServiceAccount;

  beforeEach(() => {
    // Guardar variables de entorno originales
    originalEnv = { ...process.env };
    
    // Limpiar variables de entorno relacionadas con GCP
    delete process.env.GCP_SIMULATION_MODE;
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    delete process.env.GCS_KEY_FILE;
    delete process.env.BIGQUERY_KEY_FILE;
    delete process.env.GCP_PROJECT_ID;
    delete process.env.GCS_BUCKET_NAME;

    // Crear service account de prueba
    validServiceAccount = {
      type: 'service_account',
      project_id: 'test-project-123',
      private_key_id: 'test-key-id',
      private_key: '-----BEGIN PRIVATE KEY-----\ntest-private-key\n-----END PRIVATE KEY-----\n',
      client_email: 'test@test-project-123.iam.gserviceaccount.com',
      client_id: '123456789',
      auth_uri: 'https://accounts.google.com/o/oauth2/auth',
      token_uri: 'https://oauth2.googleapis.com/token'
    };

    testServiceAccountPath = path.join(__dirname, 'test-service-account.json');
  });

  afterEach(async () => {
    // Restaurar variables de entorno
    process.env = originalEnv;
    
    // Limpiar archivos de prueba
    if (fs.existsSync(testServiceAccountPath)) {
      fs.unlinkSync(testServiceAccountPath);
    }

    // Limpiar directorios de simulación de prueba
    const testDirs = [
      'tmp/test-gcs-simulation',
      'tmp/test-bigquery-simulation'
    ];
    
    testDirs.forEach(dir => {
      if (fs.existsSync(dir)) {
        fs.rmSync(dir, { recursive: true, force: true });
      }
    });

    // Limpiar cache de módulos para próximas pruebas
    // En ES modules no hay require.cache, pero podemos forzar reimportación
    delete import.meta.cache;
  });

  test('debe crear configuración en modo simulación', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';
    process.env.GCS_LOCAL_SIMULATION_PATH = 'tmp/test-gcs-simulation/';
    process.env.BIGQUERY_LOCAL_SIMULATION_PATH = 'tmp/test-bigquery-simulation/';

    // Importar después de configurar el entorno
    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    
    assert.strictEqual(config.isSimulationMode(), true);
    
    const gcpConfig = config.getConfig();
    assert.strictEqual(gcpConfig.simulation.enabled, true);
    assert.strictEqual(gcpConfig.gcs.keyFilename, null);
    assert.strictEqual(gcpConfig.bigQuery.keyFilename, null);
  });

  test('debe validar credenciales correctamente en modo simulación', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const validation = config.validateCredentials();
    
    assert.strictEqual(validation.valid, true);
    assert.strictEqual(validation.mode, 'simulation');
    assert.ok(validation.message.includes('simulación'));
  });

  test('debe fallar validación cuando no hay service account', async () => {
    process.env.GCP_SIMULATION_MODE = 'false';
    // Asegurar que no hay service account disponible
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    delete process.env.GCS_KEY_FILE;
    delete process.env.BIGQUERY_KEY_FILE;

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const validation = config.validateCredentials();
    
    assert.strictEqual(validation.valid, false);
    assert.strictEqual(validation.mode, 'missing');
    assert.ok(validation.message.includes('service-account.json no encontrado'));
  });

  test('debe validar service account válido', async () => {
    // Crear archivo de service account válido
    fs.writeFileSync(testServiceAccountPath, JSON.stringify(validServiceAccount, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_SIMULATION_MODE = 'false';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const validation = config.validateCredentials();
    
    assert.strictEqual(validation.valid, true);
    assert.strictEqual(validation.mode, 'production');
    assert.strictEqual(validation.projectId, 'test-project-123');
    assert.strictEqual(validation.clientEmail, 'test@test-project-123.iam.gserviceaccount.com');
  });

  test('debe fallar con service account inválido', async () => {
    // Crear archivo de service account inválido (sin campos requeridos)
    const invalidServiceAccount = {
      type: 'service_account',
      project_id: 'test-project'
      // Faltan private_key y client_email
    };
    
    fs.writeFileSync(testServiceAccountPath, JSON.stringify(invalidServiceAccount, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_SIMULATION_MODE = 'false';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const validation = config.validateCredentials();
    
    assert.strictEqual(validation.valid, false);
    assert.strictEqual(validation.mode, 'invalid');
    assert.ok(validation.message.includes('Campos faltantes'));
  });

  test('debe fallar con archivo JSON malformado', async () => {
    // Crear archivo JSON malformado
    fs.writeFileSync(testServiceAccountPath, '{ invalid json }');
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_SIMULATION_MODE = 'false';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const validation = config.validateCredentials();
    
    assert.strictEqual(validation.valid, false);
    assert.strictEqual(validation.mode, 'error');
    assert.ok(validation.message.includes('Error al leer'));
  });

  test('debe construir configuración con valores por defecto', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const gcpConfig = config.getConfig();
    
    assert.strictEqual(gcpConfig.gcs.bucketName, 'gps-mobile-data-bucket');
    assert.strictEqual(gcpConfig.gcs.region, 'us-central1');
    assert.strictEqual(gcpConfig.gcs.prefixes.gps, 'gps-data/');
    assert.strictEqual(gcpConfig.gcs.prefixes.mobile, 'mobile-data/');
    assert.strictEqual(gcpConfig.bigQuery.datasetId, 'location_data');
    assert.strictEqual(gcpConfig.bigQuery.location, 'US');
    assert.strictEqual(gcpConfig.bigQuery.tables.gps, 'gps_records');
    assert.strictEqual(gcpConfig.bigQuery.tables.mobile, 'mobile_records');
  });

  test('debe usar variables de entorno personalizadas', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';
    process.env.GCP_PROJECT_ID = 'custom-project';
    process.env.GCS_BUCKET_NAME = 'custom-bucket';
    process.env.GCS_REGION = 'europe-west1';
    process.env.GCS_GPS_PREFIX = 'custom-gps/';
    process.env.GCS_MOBILE_PREFIX = 'custom-mobile/';
    process.env.BIGQUERY_DATASET_ID = 'custom_dataset';
    process.env.BIGQUERY_LOCATION = 'EU';
    process.env.BIGQUERY_GPS_TABLE_ID = 'custom_gps_table';
    process.env.BIGQUERY_MOBILE_TABLE_ID = 'custom_mobile_table';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const gcpConfig = config.getConfig();
    
    assert.strictEqual(gcpConfig.projectId, 'custom-project');
    assert.strictEqual(gcpConfig.gcs.bucketName, 'custom-bucket');
    assert.strictEqual(gcpConfig.gcs.region, 'europe-west1');
    assert.strictEqual(gcpConfig.gcs.prefixes.gps, 'custom-gps/');
    assert.strictEqual(gcpConfig.gcs.prefixes.mobile, 'custom-mobile/');
    assert.strictEqual(gcpConfig.bigQuery.datasetId, 'custom_dataset');
    assert.strictEqual(gcpConfig.bigQuery.location, 'EU');
    assert.strictEqual(gcpConfig.bigQuery.tables.gps, 'custom_gps_table');
    assert.strictEqual(gcpConfig.bigQuery.tables.mobile, 'custom_mobile_table');
  });

  test('debe inicializar directorios de simulación', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';
    process.env.GCS_LOCAL_SIMULATION_PATH = 'tmp/test-gcs-simulation/';
    process.env.BIGQUERY_LOCAL_SIMULATION_PATH = 'tmp/test-bigquery-simulation/';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    config.initializeSimulationDirectories();
    
    const gcpConfig = config.getConfig();
    
    // Verificar que los directorios se crearon
    assert.ok(fs.existsSync(gcpConfig.simulation.paths.gcs));
    assert.ok(fs.existsSync(gcpConfig.simulation.paths.bigQuery));
    assert.ok(fs.existsSync(path.join(gcpConfig.simulation.paths.gcs, 'gps-data/')));
    assert.ok(fs.existsSync(path.join(gcpConfig.simulation.paths.gcs, 'mobile-data/')));
  });

  test('debe obtener estado completo de configuración', async () => {
    fs.writeFileSync(testServiceAccountPath, JSON.stringify(validServiceAccount, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_PROJECT_ID = 'test-project-123';
    process.env.GCP_SIMULATION_MODE = 'false';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    const status = config.getStatus();
    
    assert.strictEqual(status.simulationMode, false);
    assert.strictEqual(status.credentialsValid, true);
    assert.strictEqual(status.credentialsMode, 'production');
    assert.strictEqual(status.projectId, 'test-project-123');
    assert.ok(status.serviceAccountPath.includes('test-service-account.json'));
    assert.strictEqual(typeof status.gcs, 'object');
    assert.strictEqual(typeof status.bigQuery, 'object');
  });

  test('debe lanzar error en validateAndThrow con credenciales inválidas', async () => {
    process.env.GCP_SIMULATION_MODE = 'false';
    // No configurar service account ni project ID
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    delete process.env.GCS_KEY_FILE;
    delete process.env.BIGQUERY_KEY_FILE;
    delete process.env.GCP_PROJECT_ID;

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    
    assert.throws(() => {
      config.validateAndThrow();
    }, /Variables de entorno requeridas faltantes|Configuración GCP inválida/);
  });

  test('debe lanzar error con configuración faltante', async () => {
    fs.writeFileSync(testServiceAccountPath, JSON.stringify(validServiceAccount, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_SIMULATION_MODE = 'false';
    // No configurar GCP_PROJECT_ID

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    
    assert.throws(() => {
      config.validateAndThrow();
    }, /Variables de entorno requeridas faltantes/);
  });

  test('debe pasar validateAndThrow en modo simulación', async () => {
    process.env.GCP_SIMULATION_MODE = 'true';
    process.env.GCP_PROJECT_ID = 'test-project';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    
    // No debe lanzar error
    assert.doesNotThrow(() => {
      config.validateAndThrow();
    });
  });

  test('debe resolver múltiples rutas de service account', async () => {
    // Crear archivo en ruta alternativa
    const altPath = path.join(__dirname, 'alt-service-account.json');
    fs.writeFileSync(altPath, JSON.stringify(validServiceAccount, null, 2));
    
    process.env.GCS_KEY_FILE = altPath;
    process.env.GCP_SIMULATION_MODE = 'false';

    const module = await import('../src/config/gcpConfig.js');
    GCPConfig = module.GCPConfig;

    const config = new GCPConfig();
    
    assert.ok(config.serviceAccountPath.includes('alt-service-account.json'));
    
    // Limpiar
    fs.unlinkSync(altPath);
  });

  test('debe manejar fallback graceful cuando faltan credenciales', async () => {
    process.env.GCP_SIMULATION_MODE = 'false';
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    delete process.env.GCS_KEY_FILE;
    delete process.env.BIGQUERY_KEY_FILE;
    delete process.env.GCP_PROJECT_ID;

    const { GCPInitializer } = await import('../src/utils/GCPInitializer.js');
    const initializer = new GCPInitializer();
    
    const fallbackResult = await initializer.gracefulFallback();
    
    assert.strictEqual(fallbackResult.fallback, true);
    assert.ok(['simulation', 'disabled'].includes(fallbackResult.mode));
    assert.ok(fallbackResult.message.includes('simulación') || fallbackResult.message.includes('deshabilitado'));
  });

  test('debe validar configuración completa en startup', async () => {
    // Configurar entorno válido
    fs.writeFileSync(testServiceAccountPath, JSON.stringify(validServiceAccount, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = testServiceAccountPath;
    process.env.GCP_PROJECT_ID = 'test-project-123';
    process.env.GCS_BUCKET_NAME = 'test-bucket';
    process.env.GCP_SIMULATION_MODE = 'false';

    const { initializeGCP } = await import('../src/utils/GCPInitializer.js');
    
    const result = await initializeGCP();
    
    assert.strictEqual(result.success, true);
    assert.strictEqual(result.mode, 'production');
    assert.ok(result.message.includes('correctamente'));
  });

  test('debe activar modo simulación automáticamente en fallback', async () => {
    process.env.GCP_SIMULATION_MODE = 'false';
    delete process.env.GOOGLE_APPLICATION_CREDENTIALS;
    delete process.env.GCS_KEY_FILE;
    delete process.env.BIGQUERY_KEY_FILE;
    process.env.GCP_PROJECT_ID = 'test-project';

    const { GCPInitializer } = await import('../src/utils/GCPInitializer.js');
    const initializer = new GCPInitializer();
    
    const fallbackResult = await initializer.gracefulFallback();
    
    assert.strictEqual(fallbackResult.fallback, true);
    assert.strictEqual(fallbackResult.mode, 'simulation');
    assert.ok(fallbackResult.message.includes('simulación'));
  });
});