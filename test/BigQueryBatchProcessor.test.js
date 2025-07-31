import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { BigQueryBatchProcessor } from '../src/services/BigQueryBatchProcessor.js';

describe('BigQueryBatchProcessor', () => {
  let processor;

  beforeEach(() => {
    // Configurar modo simulación para tests
    process.env.BIGQUERY_SIMULATION_MODE = 'true';
    process.env.BIGQUERY_PROJECT_ID = 'test-project';
    process.env.BIGQUERY_DATASET_ID = 'location_data';
    
    processor = new BigQueryBatchProcessor();
    // Sobrescribir configuración después de crear la instancia
    processor.projectId = 'test-project';
    processor.datasetId = 'location_data';
  });

  afterEach(async () => {
    await processor.cleanup();
    delete process.env.BIGQUERY_SIMULATION_MODE;
    delete process.env.BIGQUERY_PROJECT_ID;
    delete process.env.BIGQUERY_DATASET_ID;
  });

  describe('initialize', () => {
    it('should initialize in simulation mode', async () => {
      await processor.initialize();

      assert.strictEqual(processor.isInitialized, true);
      assert.strictEqual(processor.simulationMode, true);
    });

    it('should handle multiple initialization calls', async () => {
      await processor.initialize();
      await processor.initialize(); // Segunda llamada

      assert.strictEqual(processor.isInitialized, true);
    });
  });

  describe('processGCSFile', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should process GPS file successfully', async () => {
      const gcsUri = 'gs://test-bucket/gps/2024-01-15_gps_123.json';
      const dataType = 'gps';
      const metadata = {
        processingId: 'gps_test_123',
        recordCount: 100
      };

      const result = await processor.processGCSFile(gcsUri, dataType, metadata);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.dataType, 'gps');
      assert.strictEqual(result.gcsUri, gcsUri);
      assert.ok(result.jobId);
      assert.ok(result.recordsProcessed > 0);
      assert.ok(result.bytesProcessed > 0);
      assert.strictEqual(result.simulated, true);
    });

    it('should process Mobile file successfully', async () => {
      const gcsUri = 'gs://test-bucket/mobile/2024-01-15_mobile_456.json';
      const dataType = 'mobile';
      const metadata = {
        processingId: 'mobile_test_456',
        recordCount: 50
      };

      const result = await processor.processGCSFile(gcsUri, dataType, metadata);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.dataType, 'mobile');
      assert.strictEqual(result.gcsUri, gcsUri);
      assert.ok(result.jobId);
      assert.ok(result.recordsProcessed > 0);
      assert.ok(result.bytesProcessed > 0);
      assert.strictEqual(result.simulated, true);
    });

    it('should handle unsupported data type', async () => {
      const gcsUri = 'gs://test-bucket/unknown/test.json';
      const dataType = 'unknown';

      const result = await processor.processGCSFile(gcsUri, dataType);

      assert.strictEqual(result.success, false);
      assert.ok(result.error);
      assert.ok(result.error.includes('no soportado'));
    });

    it('should generate unique job IDs', async () => {
      const gcsUri1 = 'gs://test-bucket/gps/file1.json';
      const gcsUri2 = 'gs://test-bucket/gps/file2.json';
      
      const result1 = await processor.processGCSFile(gcsUri1, 'gps', { processingId: 'test1' });
      const result2 = await processor.processGCSFile(gcsUri2, 'gps', { processingId: 'test2' });

      assert.strictEqual(result1.success, true);
      assert.strictEqual(result2.success, true);
      assert.notStrictEqual(result1.jobId, result2.jobId);
    });
  });

  describe('processBatch', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should process multiple files successfully', async () => {
      const files = [
        {
          gcsUri: 'gs://test-bucket/gps/file1.json',
          dataType: 'gps',
          metadata: { processingId: 'gps1', recordCount: 100 }
        },
        {
          gcsUri: 'gs://test-bucket/mobile/file1.json',
          dataType: 'mobile',
          metadata: { processingId: 'mobile1', recordCount: 50 }
        },
        {
          gcsUri: 'gs://test-bucket/gps/file2.json',
          dataType: 'gps',
          metadata: { processingId: 'gps2', recordCount: 75 }
        }
      ];

      const result = await processor.processBatch(files);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.totalFiles, 3);
      assert.strictEqual(result.successfulFiles, 3);
      assert.strictEqual(result.failedFiles, 0);
      assert.ok(result.totalRecords > 0);
      assert.strictEqual(result.results.length, 3);
      assert.strictEqual(result.errors.length, 0);
    });

    it('should handle mixed success and failure', async () => {
      const files = [
        {
          gcsUri: 'gs://test-bucket/gps/file1.json',
          dataType: 'gps',
          metadata: { processingId: 'gps1' }
        },
        {
          gcsUri: 'gs://test-bucket/unknown/file2.json',
          dataType: 'unknown', // Tipo no soportado
          metadata: { processingId: 'unknown1' }
        }
      ];

      const result = await processor.processBatch(files, { continueOnError: true });

      assert.strictEqual(result.success, true); // continueOnError = true
      assert.strictEqual(result.totalFiles, 2);
      assert.strictEqual(result.successfulFiles, 1);
      assert.strictEqual(result.failedFiles, 1);
      assert.strictEqual(result.errors.length, 1);
    });

    it('should respect maxConcurrency option', async () => {
      const files = Array.from({ length: 5 }, (_, i) => ({
        gcsUri: `gs://test-bucket/gps/file${i}.json`,
        dataType: 'gps',
        metadata: { processingId: `gps${i}` }
      }));

      const result = await processor.processBatch(files, { maxConcurrency: 2 });

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.totalFiles, 5);
      assert.strictEqual(result.successfulFiles, 5);
      assert.strictEqual(result.results.length, 5);
    });

    it('should handle empty file list', async () => {
      const result = await processor.processBatch([]);

      assert.strictEqual(result.success, true);
      assert.strictEqual(result.totalFiles, 0);
      assert.strictEqual(result.successfulFiles, 0);
      assert.strictEqual(result.failedFiles, 0);
      assert.strictEqual(result.totalRecords, 0);
    });
  });

  describe('getJobStatus', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should return job status in simulation mode', async () => {
      const jobId = 'test_job_123';
      const status = await processor.getJobStatus(jobId);

      assert.strictEqual(status.jobId, jobId);
      assert.strictEqual(status.state, 'DONE');
      assert.strictEqual(status.simulated, true);
      assert.ok(status.completedAt);
    });
  });

  describe('listRecentJobs', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should list recent jobs in simulation mode', async () => {
      const jobs = await processor.listRecentJobs();

      assert.ok(Array.isArray(jobs));
      assert.ok(jobs.length > 0);
      assert.ok(jobs.every(job => job.jobId && job.state && job.jobType));
      assert.ok(jobs.every(job => job.simulated === true));
    });

    it('should respect maxResults option', async () => {
      const jobs = await processor.listRecentJobs({ maxResults: 3 });

      assert.ok(Array.isArray(jobs));
      assert.ok(jobs.length <= 3);
    });
  });

  describe('generateJobId', () => {
    it('should generate unique job IDs', () => {
      const id1 = processor.generateJobId('gps', 'proc123');
      const id2 = processor.generateJobId('mobile', 'proc456');

      assert.ok(id1.startsWith('load_gps_proc123_'));
      assert.ok(id2.startsWith('load_mobile_proc456_'));
      assert.notStrictEqual(id1, id2);
    });

    it('should handle missing processingId', () => {
      const id = processor.generateJobId('gps');

      assert.ok(id.startsWith('load_gps_'));
      assert.ok(id.length > 'load_gps_'.length);
    });
  });

  describe('getTableStats', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should return table statistics in simulation mode', async () => {
      const stats = await processor.getTableStats();

      assert.ok(stats.gps);
      assert.ok(stats.mobile);
      assert.ok(typeof stats.gps.numRows === 'number');
      assert.ok(typeof stats.gps.numBytes === 'number');
      assert.ok(typeof stats.mobile.numRows === 'number');
      assert.ok(typeof stats.mobile.numBytes === 'number');
      assert.strictEqual(stats.gps.simulated, true);
      assert.strictEqual(stats.mobile.simulated, true);
    });
  });

  describe('getStatus', () => {
    it('should return status in simulation mode', async () => {
      await processor.initialize();
      const status = await processor.getStatus();

      assert.strictEqual(status.initialized, true);
      assert.strictEqual(status.simulationMode, true);
      assert.strictEqual(status.projectId, 'test-project');
      assert.strictEqual(status.datasetId, 'location_data');
      assert.ok(status.tables);
      assert.ok(status.note);
    });

    it('should return status before initialization', async () => {
      const status = await processor.getStatus();

      assert.strictEqual(status.initialized, false);
      assert.strictEqual(status.simulationMode, true);
    });
  });

  describe('cleanup', () => {
    it('should cleanup resources', async () => {
      await processor.initialize();
      
      assert.strictEqual(processor.isInitialized, true);
      
      await processor.cleanup();
      
      assert.strictEqual(processor.isInitialized, false);
      assert.strictEqual(processor.bigQuery, null);
      assert.strictEqual(processor.dataset, null);
    });
  });

  describe('error handling', () => {
    it('should handle initialization errors gracefully', async () => {
      // Crear un nuevo processor sin modo simulación
      const testProcessor = new BigQueryBatchProcessor();
      testProcessor.simulationMode = false;
      testProcessor.keyFilename = '/invalid/path/credentials.json';

      await testProcessor.initialize();

      // Debería hacer fallback a modo simulación
      assert.strictEqual(testProcessor.simulationMode, true);
      assert.strictEqual(testProcessor.isInitialized, true);
      
      await testProcessor.cleanup();
    });
  });

  describe('integration scenarios', () => {
    beforeEach(async () => {
      await processor.initialize();
    });

    it('should handle complete workflow: process file and check status', async () => {
      // Procesar archivo
      const gcsUri = 'gs://test-bucket/gps/workflow-test.json';
      const result = await processor.processGCSFile(gcsUri, 'gps', {
        processingId: 'workflow_test',
        recordCount: 200
      });

      assert.strictEqual(result.success, true);
      
      // Verificar estado del job
      const status = await processor.getJobStatus(result.jobId);
      assert.strictEqual(status.jobId, result.jobId);
      assert.strictEqual(status.state, 'DONE');
    });

    it('should handle batch processing with job monitoring', async () => {
      const files = [
        {
          gcsUri: 'gs://test-bucket/gps/batch1.json',
          dataType: 'gps',
          metadata: { processingId: 'batch_gps_1', recordCount: 100 }
        },
        {
          gcsUri: 'gs://test-bucket/mobile/batch1.json',
          dataType: 'mobile',
          metadata: { processingId: 'batch_mobile_1', recordCount: 50 }
        }
      ];

      // Procesar batch
      const batchResult = await processor.processBatch(files);
      assert.strictEqual(batchResult.success, true);

      // Verificar estadísticas de tablas
      const tableStats = await processor.getTableStats();
      assert.ok(tableStats.gps.numRows > 0);
      assert.ok(tableStats.mobile.numRows > 0);

      // Listar jobs recientes
      const recentJobs = await processor.listRecentJobs({ maxResults: 5 });
      assert.ok(recentJobs.length > 0);
    });
  });
});