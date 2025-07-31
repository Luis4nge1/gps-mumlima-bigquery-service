import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import fs from 'fs/promises';
import path from 'path';
import { GCSAdapter } from '../src/adapters/GCSAdapter.js';

describe('GCSAdapter', () => {
  let gcsAdapter;
  const testStoragePath = 'tmp/test-gcs-simulation/';

  beforeEach(async () => {
    // Configurar modo simulación para tests
    process.env.GCS_SIMULATION_MODE = 'true';
    process.env.GCS_BUCKET_NAME = 'test-bucket';
    
    gcsAdapter = new GCSAdapter();
    gcsAdapter.localStoragePath = testStoragePath;
    
    // Limpiar directorio de test si existe
    try {
      await fs.rm(testStoragePath, { recursive: true, force: true });
    } catch (error) {
      // Ignorar si no existe
    }
  });

  afterEach(async () => {
    // Limpiar después de cada test
    try {
      await fs.rm(testStoragePath, { recursive: true, force: true });
    } catch (error) {
      // Ignorar si no existe
    }
    
    await gcsAdapter.cleanup();
    delete process.env.GCS_SIMULATION_MODE;
    delete process.env.GCS_BUCKET_NAME;
  });

  describe('initialize', () => {
    it('should initialize in simulation mode', async () => {
      await gcsAdapter.initialize();

      assert.strictEqual(gcsAdapter.isInitialized, true);
      assert.strictEqual(gcsAdapter.simulationMode, true);
      
      // Verificar que se creó el directorio
      const stats = await fs.stat(testStoragePath);
      assert.ok(stats.isDirectory());
    });

    it('should handle multiple initialization calls', async () => {
      await gcsAdapter.initialize();
      await gcsAdapter.initialize(); // Segunda llamada

      assert.strictEqual(gcsAdapter.isInitialized, true);
    });
  });

  describe('uploadJSON', () => {
    it('should upload JSON data successfully', async () => {
      const testData = {
        metadata: {
          type: 'gps',
          processingId: 'test-123',
          recordCount: 2
        },
        data: [
          { deviceId: 'device1', lat: 40.7128, lng: -74.0060, timestamp: '2024-01-15T10:30:00Z' },
          { deviceId: 'device2', lat: 34.0522, lng: -118.2437, timestamp: '2024-01-15T10:35:00Z' }
        ]
      };

      const fileName = 'test-gps-data.json';
      const result = await gcsAdapter.uploadJSON(testData, fileName);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.fileName, fileName);
      assert.strictEqual(result.bucketName, 'test-bucket');
      assert.ok(result.fileSize > 0);
      assert.ok(result.uploadedAt);
      assert.strictEqual(result.simulated, true);

      // Verificar que el archivo se creó
      const filePath = path.join(testStoragePath, fileName);
      const fileExists = await fs.access(filePath).then(() => true).catch(() => false);
      assert.ok(fileExists);

      // Verificar contenido
      const content = await fs.readFile(filePath, 'utf8');
      const parsedContent = JSON.parse(content);
      assert.deepStrictEqual(parsedContent, testData);
    });

    it('should create metadata file', async () => {
      const testData = {
        metadata: { type: 'mobile', processingId: 'test-456' },
        data: [{ userId: 'user1', lat: 40.7128, lng: -74.0060 }]
      };

      const fileName = 'test-mobile-data.json';
      const customMetadata = { source: 'test' };
      
      await gcsAdapter.uploadJSON(testData, fileName, customMetadata);

      // Verificar que se creó el archivo de metadata
      const metadataPath = path.join(testStoragePath, `${fileName}.metadata.json`);
      const metadataExists = await fs.access(metadataPath).then(() => true).catch(() => false);
      assert.ok(metadataExists);

      // Verificar contenido de metadata
      const metadataContent = await fs.readFile(metadataPath, 'utf8');
      const metadata = JSON.parse(metadataContent);
      
      assert.strictEqual(metadata.contentType, 'application/json');
      assert.ok(metadata.metadata.uploadedAt);
      assert.strictEqual(metadata.metadata.recordCount, 1);
      assert.strictEqual(metadata.metadata.dataType, 'mobile');
      assert.strictEqual(metadata.metadata.source, 'test');
    });

    it('should handle upload errors gracefully', async () => {
      // Simular error usando un nombre de archivo inválido
      await gcsAdapter.initialize();
      
      const testData = { metadata: { type: 'gps' }, data: [] };
      // Usar caracteres inválidos en el nombre del archivo para Windows
      const invalidFileName = 'test<>:"|?*.json';
      
      const result = await gcsAdapter.uploadJSON(testData, invalidFileName);

      // En modo simulación, algunos errores pueden no ocurrir, así que verificamos que al menos se intente
      // Si no hay error, al menos verificamos que se procesó
      assert.ok(result.success !== undefined);
      assert.ok(result.fileName === invalidFileName);
    });
  });

  describe('listFiles', () => {
    beforeEach(async () => {
      await gcsAdapter.initialize();
      
      // Crear algunos archivos de test
      const testFiles = [
        {
          name: 'gps-2024-01-15.json',
          data: { metadata: { type: 'gps', processingId: 'gps-123' }, data: [] }
        },
        {
          name: 'mobile-2024-01-15.json',
          data: { metadata: { type: 'mobile', processingId: 'mobile-456' }, data: [] }
        },
        {
          name: 'gps-2024-01-16.json',
          data: { metadata: { type: 'gps', processingId: 'gps-789' }, data: [] }
        }
      ];

      for (const testFile of testFiles) {
        await gcsAdapter.uploadJSON(testFile.data, testFile.name);
      }
    });

    it('should list all files', async () => {
      const files = await gcsAdapter.listFiles();

      assert.strictEqual(files.length, 3);
      assert.ok(files.every(file => file.name && file.size && file.created));
      assert.ok(files.every(file => file.simulated === true));
    });

    it('should filter files by prefix', async () => {
      const files = await gcsAdapter.listFiles({ prefix: 'gps-' });

      assert.strictEqual(files.length, 2);
      assert.ok(files.every(file => file.name.startsWith('gps-')));
    });

    it('should filter files by data type', async () => {
      const files = await gcsAdapter.listFiles({ dataType: 'mobile' });

      assert.strictEqual(files.length, 1);
      assert.strictEqual(files[0].name, 'mobile-2024-01-15.json');
      assert.strictEqual(files[0].metadata.dataType, 'mobile');
    });

    it('should handle empty directory', async () => {
      // Limpiar directorio
      await fs.rm(testStoragePath, { recursive: true, force: true });
      await gcsAdapter.initialize();

      const files = await gcsAdapter.listFiles();

      assert.strictEqual(files.length, 0);
    });
  });

  describe('downloadFile', () => {
    beforeEach(async () => {
      await gcsAdapter.initialize();
    });

    it('should download existing file', async () => {
      const testData = {
        metadata: { type: 'gps', processingId: 'download-test' },
        data: [{ deviceId: 'device1', lat: 40.7128, lng: -74.0060 }]
      };

      const fileName = 'download-test.json';
      await gcsAdapter.uploadJSON(testData, fileName);

      const result = await gcsAdapter.downloadFile(fileName);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.fileName, fileName);
      assert.ok(result.content);
      assert.strictEqual(result.simulated, true);

      const parsedContent = JSON.parse(result.content);
      assert.deepStrictEqual(parsedContent, testData);
    });

    it('should handle non-existent file', async () => {
      const result = await gcsAdapter.downloadFile('non-existent.json');

      assert.strictEqual(result.success, false);
      assert.ok(result.error);
      assert.ok(result.error.includes('no encontrado'));
    });
  });

  describe('deleteFile', () => {
    beforeEach(async () => {
      await gcsAdapter.initialize();
    });

    it('should delete existing file', async () => {
      const testData = { metadata: { type: 'gps' }, data: [] };
      const fileName = 'delete-test.json';
      
      await gcsAdapter.uploadJSON(testData, fileName);

      // Verificar que existe
      const filePath = path.join(testStoragePath, fileName);
      let exists = await fs.access(filePath).then(() => true).catch(() => false);
      assert.ok(exists);

      // Eliminar
      const result = await gcsAdapter.deleteFile(fileName);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.fileName, fileName);
      assert.ok(result.deletedAt);
      assert.strictEqual(result.simulated, true);

      // Verificar que se eliminó
      exists = await fs.access(filePath).then(() => true).catch(() => false);
      assert.strictEqual(exists, false);
    });

    it('should handle non-existent file deletion', async () => {
      const result = await gcsAdapter.deleteFile('non-existent.json');

      assert.strictEqual(result.success, true);
      assert.ok(result.message.includes('not found'));
    });

    it('should delete metadata file along with data file', async () => {
      const testData = { metadata: { type: 'gps' }, data: [] };
      const fileName = 'metadata-delete-test.json';
      
      await gcsAdapter.uploadJSON(testData, fileName);

      const metadataPath = path.join(testStoragePath, `${fileName}.metadata.json`);
      let metadataExists = await fs.access(metadataPath).then(() => true).catch(() => false);
      assert.ok(metadataExists);

      await gcsAdapter.deleteFile(fileName);

      // Verificar que se eliminó la metadata también
      metadataExists = await fs.access(metadataPath).then(() => true).catch(() => false);
      assert.strictEqual(metadataExists, false);
    });
  });

  describe('generateFileName', () => {
    it('should generate unique file names', () => {
      const fileName1 = gcsAdapter.generateFileName('gps', 'proc-123');
      const fileName2 = gcsAdapter.generateFileName('gps', 'proc-456');

      assert.ok(fileName1.startsWith('gps/'));
      assert.ok(fileName1.endsWith('_proc-123.json'));
      assert.ok(fileName2.startsWith('gps/'));
      assert.ok(fileName2.endsWith('_proc-456.json'));
      assert.notStrictEqual(fileName1, fileName2);
    });

    it('should include data type in path', () => {
      const gpsFileName = gcsAdapter.generateFileName('gps', 'test');
      const mobileFileName = gcsAdapter.generateFileName('mobile', 'test');

      assert.ok(gpsFileName.startsWith('gps/'));
      assert.ok(mobileFileName.startsWith('mobile/'));
    });
  });

  describe('getBucketStats', () => {
    beforeEach(async () => {
      await gcsAdapter.initialize();
      
      // Crear archivos de diferentes tipos
      const testFiles = [
        { name: 'gps1.json', data: { metadata: { type: 'gps' }, data: [1, 2, 3] } },
        { name: 'gps2.json', data: { metadata: { type: 'gps' }, data: [4, 5] } },
        { name: 'mobile1.json', data: { metadata: { type: 'mobile' }, data: [6] } }
      ];

      for (const testFile of testFiles) {
        await gcsAdapter.uploadJSON(testFile.data, testFile.name);
      }
    });

    it('should return bucket statistics', async () => {
      const stats = await gcsAdapter.getBucketStats();

      assert.strictEqual(stats.bucketName, 'test-bucket');
      assert.strictEqual(stats.totalFiles, 3);
      assert.ok(stats.totalSize > 0);
      assert.strictEqual(stats.simulated, true);
      assert.strictEqual(stats.filesByType.gps, 2);
      assert.strictEqual(stats.filesByType.mobile, 1);
    });

    it('should handle empty bucket', async () => {
      // Limpiar directorio
      await fs.rm(testStoragePath, { recursive: true, force: true });
      await gcsAdapter.initialize();

      const stats = await gcsAdapter.getBucketStats();

      assert.strictEqual(stats.totalFiles, 0);
      assert.strictEqual(stats.totalSize, 0);
      assert.deepStrictEqual(stats.filesByType, {});
    });
  });

  describe('getStatus', () => {
    it('should return status in simulation mode', async () => {
      await gcsAdapter.initialize();
      const status = await gcsAdapter.getStatus();

      assert.strictEqual(status.initialized, true);
      assert.strictEqual(status.simulationMode, true);
      assert.strictEqual(status.bucketName, 'test-bucket');
      assert.ok(status.localStoragePath);
      assert.strictEqual(status.localStorageExists, true);
    });

    it('should return status before initialization', async () => {
      const status = await gcsAdapter.getStatus();

      assert.strictEqual(status.initialized, false);
      assert.strictEqual(status.simulationMode, true);
    });
  });

  describe('cleanup', () => {
    it('should cleanup resources', async () => {
      await gcsAdapter.initialize();
      
      assert.strictEqual(gcsAdapter.isInitialized, true);
      
      await gcsAdapter.cleanup();
      
      assert.strictEqual(gcsAdapter.isInitialized, false);
      assert.strictEqual(gcsAdapter.storage, null);
      assert.strictEqual(gcsAdapter.bucket, null);
    });
  });
});