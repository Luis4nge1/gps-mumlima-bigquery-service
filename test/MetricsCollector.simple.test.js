import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert';
import { MetricsCollector } from '../src/utils/MetricsCollector.js';
import { FileUtils } from '../src/utils/FileUtils.js';

describe('MetricsCollector - Enhanced GCS/BigQuery Metrics', () => {
  let metricsCollector;
  const testMetricsFile = 'tmp/test-metrics-enhanced.json';

  beforeEach(async () => {
    // Crear nueva instancia para cada test
    MetricsCollector.instance = null;
    MetricsCollector.isLoaded = false;
    
    metricsCollector = new MetricsCollector();
    metricsCollector.metricsFile = testMetricsFile;
    
    // Limpiar archivo de métricas de prueba
    const exists = await FileUtils.pathExists(testMetricsFile);
    if (exists) {
      await FileUtils.deleteFile(testMetricsFile);
    }
  });

  afterEach(async () => {
    // Limpiar después de cada test
    const exists = await FileUtils.pathExists(testMetricsFile);
    if (exists) {
      await FileUtils.deleteFile(testMetricsFile);
    }
    
    MetricsCollector.instance = null;
    MetricsCollector.isLoaded = false;
  });

  describe('GCS Metrics', () => {
    it('should record GCS upload metrics for GPS data', async () => {
      await metricsCollector.recordGCSOperation('gps', 1024, 500, true);

      const metrics = await metricsCollector.getMetrics();
      
      assert.strictEqual(metrics.gcs.uploads.gps.total, 1);
      assert.strictEqual(metrics.gcs.uploads.gps.successful, 1);
      assert.strictEqual(metrics.gcs.uploads.gps.failed, 0);
      assert.strictEqual(metrics.gcs.uploads.gps.totalSize, 1024);
      assert.strictEqual(metrics.gcs.uploads.gps.totalTime, 500);
      assert.strictEqual(metrics.gcs.uploads.gps.avgSize, 1024);
      assert.strictEqual(metrics.gcs.uploads.gps.avgTime, 500);
      assert.ok(metrics.gcs.lastUpload);
    });

    it('should record failed GCS uploads', async () => {
      await metricsCollector.recordGCSOperation('gps', 1024, 500, false, 'Network error');

      const metrics = await metricsCollector.getMetrics();
      
      assert.strictEqual(metrics.gcs.uploads.gps.total, 1);
      assert.strictEqual(metrics.gcs.uploads.gps.successful, 0);
      assert.strictEqual(metrics.gcs.uploads.gps.failed, 1);
      assert.ok(metrics.gcs.lastError);
      assert.strictEqual(metrics.gcs.lastError.message, 'Network error');
      assert.strictEqual(metrics.gcs.lastError.dataType, 'gps');
    });

    it('should get GCS specific metrics', async () => {
      await metricsCollector.recordGCSOperation('gps', 1024, 500, true);
      await metricsCollector.recordGCSOperation('mobile', 2048, 750, true);
      await metricsCollector.recordGCSOperation('gps', 512, 300, false, 'Upload failed');

      const gcsMetrics = await metricsCollector.getGCSMetrics();
      
      assert.strictEqual(gcsMetrics.uploads.gps.total, 2);
      assert.strictEqual(gcsMetrics.uploads.gps.successful, 1);
      assert.strictEqual(gcsMetrics.uploads.mobile.total, 1);
      assert.strictEqual(gcsMetrics.summary.totalUploads, 3);
      assert.strictEqual(gcsMetrics.summary.successfulUploads, 2);
      assert.strictEqual(gcsMetrics.summary.failedUploads, 1);
      assert.strictEqual(gcsMetrics.summary.successRate, '66.67');
    });
  });

  describe('BigQuery Batch Metrics', () => {
    it('should record BigQuery batch job metrics for GPS', async () => {
      await metricsCollector.recordBigQueryBatchJob('gps', 1000, 5000, true, 'job_gps_001');

      const metrics = await metricsCollector.getMetrics();
      
      assert.strictEqual(metrics.bigquery.batchJobs.gps.total, 1);
      assert.strictEqual(metrics.bigquery.batchJobs.gps.successful, 1);
      assert.strictEqual(metrics.bigquery.batchJobs.gps.totalRecords, 1000);
      assert.strictEqual(metrics.bigquery.batchJobs.gps.totalTime, 5000);
      assert.strictEqual(metrics.bigquery.batchJobs.gps.avgRecords, 1000);
      assert.strictEqual(metrics.bigquery.batchJobs.gps.avgTime, 5000);
      assert.strictEqual(metrics.bigquery.lastBatchJob.jobId, 'job_gps_001');
    });

    it('should record failed BigQuery batch jobs', async () => {
      await metricsCollector.recordBigQueryBatchJob('mobile', 500, 3000, false, 'job_mobile_001', 'Schema mismatch');

      const metrics = await metricsCollector.getMetrics();
      
      assert.strictEqual(metrics.bigquery.batchJobs.mobile.total, 1);
      assert.strictEqual(metrics.bigquery.batchJobs.mobile.successful, 0);
      assert.strictEqual(metrics.bigquery.batchJobs.mobile.failed, 1);
      assert.strictEqual(metrics.bigquery.lastError.jobId, 'job_mobile_001');
      assert.strictEqual(metrics.bigquery.lastError.message, 'Schema mismatch');
    });
  });

  describe('Cost Tracking', () => {
    it('should update GCP cost estimates', async () => {
      await metricsCollector.updateGCPCosts(10.5, 2.3); // 10.5 GB storage, 2.3 TB processed

      const metrics = await metricsCollector.getMetrics();
      
      assert.strictEqual(metrics.costs.estimatedGCSCost, 0.21); // 10.5 * 0.020
      assert.strictEqual(metrics.costs.estimatedBigQueryCost, 11.5); // 2.3 * 5.00
      assert.strictEqual(metrics.costs.totalEstimatedCost, 11.71);
      assert.ok(metrics.costs.lastCostUpdate);
    });
  });

  describe('Utility Methods', () => {
    it('should calculate success rate correctly', () => {
      assert.strictEqual(metricsCollector.calculateSuccessRate(8, 10), '80.00');
      assert.strictEqual(metricsCollector.calculateSuccessRate(0, 0), '0.00');
      assert.strictEqual(metricsCollector.calculateSuccessRate(5, 5), '100.00');
    });

    it('should format bytes correctly', () => {
      assert.strictEqual(metricsCollector.formatBytes(0), '0 B');
      assert.strictEqual(metricsCollector.formatBytes(1024), '1 KB');
      assert.strictEqual(metricsCollector.formatBytes(1048576), '1 MB');
      assert.strictEqual(metricsCollector.formatBytes(1073741824), '1 GB');
      assert.strictEqual(metricsCollector.formatBytes(1536), '1.5 KB');
    });
  });
});