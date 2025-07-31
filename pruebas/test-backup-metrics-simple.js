import { MetricsCollector } from '../src/utils/MetricsCollector.js';
import { BackupManager } from '../src/utils/BackupManager.js';
import { FileUtils } from '../src/utils/FileUtils.js';

async function testBackupMetrics() {
  console.log('🧪 Testing Backup Metrics Integration...\n');

  // Clean up any existing metrics
  MetricsCollector.instance = null;
  MetricsCollector.isLoaded = false;

  const metricsCollector = new MetricsCollector();
  
  // Set up test environment
  process.env.BACKUP_STORAGE_PATH = 'tmp/test-backup-metrics/';
  process.env.BACKUP_MAX_RETRIES = '3';
  process.env.BACKUP_RETENTION_HOURS = '24';
  
  const backupManager = new BackupManager();
  
  // Ensure test directory exists and is clean
  await FileUtils.ensureDirectoryExists('tmp/test-backup-metrics/');
  
  try {
    // Test 1: Create a backup and verify metrics
    console.log('📊 Test 1: Creating backup and checking metrics...');
    
    const testData = [
      { id: 1, lat: 10.123, lng: -74.456, timestamp: '2025-01-25T10:00:00Z' },
      { id: 2, lat: 10.124, lng: -74.457, timestamp: '2025-01-25T10:01:00Z' }
    ];

    const backupResult = await backupManager.saveToLocalBackup(testData, 'gps', {
      source: 'test'
    });

    console.log('   ✅ Backup created:', backupResult.success);
    console.log('   📝 Backup ID:', backupResult.backupId);
    console.log('   📊 Record count:', backupResult.recordCount);

    // Check metrics
    const backupMetrics = await metricsCollector.getBackupMetrics();
    
    console.log('\n📈 Backup Metrics after creation:');
    console.log('   Total backups:', backupMetrics.local.total);
    console.log('   Pending backups:', backupMetrics.local.pending);
    console.log('   Total records:', backupMetrics.local.totalRecords);
    console.log('   GPS backups:', backupMetrics.local.byType.gps.total);
    console.log('   Success rate:', backupMetrics.summary.successRate + '%');

    // Test 2: Simulate backup processing
    console.log('\n📊 Test 2: Simulating backup processing...');
    
    // Mock GCS upload function that succeeds
    const mockGcsUpload = async (data, type) => {
      console.log(`   🔄 Mock GCS upload for ${type} with ${data.length} records`);
      return {
        success: true,
        gcsFile: `test-${type}-${Date.now()}.json`,
        fileName: `test-${type}-${Date.now()}.json`
      };
    };

    const backupFiles = await backupManager.getLocalBackupFiles();
    console.log('   📋 Found pending backups:', backupFiles.length);

    if (backupFiles.length > 0) {
      const processResult = await backupManager.processLocalBackupFile(
        backupFiles[0], 
        mockGcsUpload
      );

      console.log('   ✅ Processing result:', processResult.success);
      console.log('   📊 Records processed:', processResult.recordsProcessed);

      // Check updated metrics
      const updatedMetrics = await metricsCollector.getBackupMetrics();
      
      console.log('\n📈 Backup Metrics after processing:');
      console.log('   Total backups:', updatedMetrics.local.total);
      console.log('   Pending backups:', updatedMetrics.local.pending);
      console.log('   Completed backups:', updatedMetrics.local.completed);
      console.log('   Success rate:', updatedMetrics.summary.successRate + '%');
      console.log('   Avg retry time:', updatedMetrics.local.avgRetryTime + 'ms');
    }

    // Test 3: Test retry time recording
    console.log('\n📊 Test 3: Testing retry time recording...');
    
    await metricsCollector.recordBackupRetryTime(1500); // 1.5 seconds
    await metricsCollector.recordBackupRetryTime(2000); // 2 seconds
    await metricsCollector.recordBackupRetryTime(1000); // 1 second

    const retryMetrics = await metricsCollector.getBackupMetrics();
    console.log('   ⏱️ Average retry time:', retryMetrics.local.avgRetryTime + 'ms');

    // Test 4: Test alerts
    console.log('\n📊 Test 4: Testing backup alerts...');
    
    await metricsCollector.recordBackupAlert('maxRetriesExceeded', {
      backupId: 'test-backup-123',
      type: 'gps',
      retryCount: 3,
      timestamp: new Date().toISOString(),
      error: 'Test alert'
    });

    const alertMetrics = await metricsCollector.getBackupMetrics();
    console.log('   🚨 Max retries exceeded alerts:', alertMetrics.alerts.maxRetriesExceeded.length);
    console.log('   📊 Total alerts:', alertMetrics.summary.alertsCount.maxRetriesExceeded);

    // Test 5: Test backup stats update
    console.log('\n📊 Test 5: Testing backup stats update...');
    
    const mockStats = {
      total: 5,
      pending: 2,
      processing: 0,
      completed: 2,
      failed: 1,
      totalRecords: 100,
      oldestPending: new Date(Date.now() - 30 * 60 * 1000).toISOString() // 30 minutes ago
    };

    await metricsCollector.updateBackupMetrics(mockStats);
    const statsMetrics = await metricsCollector.getBackupMetrics();
    
    console.log('   📊 Updated total backups:', statsMetrics.local.total);
    console.log('   📊 Updated pending backups:', statsMetrics.local.pending);
    console.log('   📊 Updated completed backups:', statsMetrics.local.completed);
    console.log('   📊 Updated failed backups:', statsMetrics.local.failed);

    console.log('\n✅ All backup metrics tests completed successfully!');
    
    // Display final summary
    const finalMetrics = await metricsCollector.getBackupMetrics();
    console.log('\n📋 Final Backup Metrics Summary:');
    console.log('   📊 Total backups:', finalMetrics.local.total);
    console.log('   ⏳ Pending backups:', finalMetrics.local.pending);
    console.log('   ✅ Completed backups:', finalMetrics.local.completed);
    console.log('   ❌ Failed backups:', finalMetrics.local.failed);
    console.log('   📈 Success rate:', finalMetrics.summary.successRate + '%');
    console.log('   ⏱️ Avg retry time:', finalMetrics.local.avgRetryTime + 'ms');
    console.log('   🚨 Total alerts:', finalMetrics.summary.alertsCount.maxRetriesExceeded);

  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error.stack);
  } finally {
    // Cleanup
    try {
      const files = await FileUtils.readdir('tmp/test-backup-metrics/');
      for (const file of files) {
        await FileUtils.unlink(`tmp/test-backup-metrics/${file}`);
      }
      await FileUtils.rmdir('tmp/test-backup-metrics/');
    } catch (cleanupError) {
      // Ignore cleanup errors
    }
  }
}

// Run the test
testBackupMetrics().catch(console.error);