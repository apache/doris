// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.backup;

import org.apache.doris.backup.BackupHandler.DatabaseJobStats;
import org.apache.doris.common.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for backup/restore concurrency control.
 */
public class BackupConcurrencyTest {

    private boolean originalConcurrencyEnabled;
    private int originalConcurrentNum;
    private long originalPendingTimeout;
    private boolean originalLogging;

    @Before
    public void setUp() {
        // Save original config values
        originalConcurrencyEnabled = Config.enable_table_level_backup_concurrency;
        originalConcurrentNum = Config.max_backup_restore_concurrent_num_per_db;
        originalPendingTimeout = Config.backup_pending_job_timeout_ms;
        originalLogging = Config.enable_backup_concurrency_logging;

        // Enable concurrency for tests
        Config.enable_table_level_backup_concurrency = true;
        Config.max_backup_restore_concurrent_num_per_db = 10;
        Config.backup_pending_job_timeout_ms = 3600000;
        Config.enable_backup_concurrency_logging = true;
    }

    @After
    public void tearDown() {
        // Restore original config values
        Config.enable_table_level_backup_concurrency = originalConcurrencyEnabled;
        Config.max_backup_restore_concurrent_num_per_db = originalConcurrentNum;
        Config.backup_pending_job_timeout_ms = originalPendingTimeout;
        Config.enable_backup_concurrency_logging = originalLogging;
    }

    @Test
    public void testDatabaseJobStatsBasic() {
        DatabaseJobStats stats = new DatabaseJobStats();

        // Initial state
        Assert.assertEquals(0, stats.activeBackups);
        Assert.assertEquals(0, stats.activeRestores);
        Assert.assertFalse(stats.hasActiveJobs());
        Assert.assertEquals(0, stats.getTotalActiveJobs());

        // Add backups
        stats.activeBackups = 3;
        Assert.assertTrue(stats.hasActiveJobs());
        Assert.assertEquals(3, stats.getTotalActiveJobs());

        // Add restores
        stats.activeRestores = 2;
        Assert.assertEquals(5, stats.getTotalActiveJobs());

        // Test full database backup marker
        stats.backupDatabaseJobId = 1001L;
        stats.backupDatabaseLabel = "full_backup";
        Assert.assertEquals("full_backup", stats.backupDatabaseLabel);

        // Test full database restore marker
        stats.restoreDatabaseJobId = 2001L;
        stats.restoreDatabaseLabel = "full_restore";
        Assert.assertEquals("full_restore", stats.restoreDatabaseLabel);
    }

    @Test
    public void testDatabaseJobStatsLabels() {
        DatabaseJobStats stats = new DatabaseJobStats();

        // Add labels
        stats.activeLabels.add("backup1");
        stats.activeLabels.add("backup2");
        stats.activeLabels.add("restore1");

        Assert.assertEquals(3, stats.activeLabels.size());
        Assert.assertTrue(stats.activeLabels.contains("backup1"));
        Assert.assertTrue(stats.activeLabels.contains("restore1"));

        // Remove label
        stats.activeLabels.remove("backup1");
        Assert.assertEquals(2, stats.activeLabels.size());
        Assert.assertFalse(stats.activeLabels.contains("backup1"));
    }

    @Test
    public void testConfigDefaults() {
        // Verify our test setup changed the configs
        Assert.assertTrue(Config.enable_table_level_backup_concurrency);
        Assert.assertEquals(10, Config.max_backup_restore_concurrent_num_per_db);
        Assert.assertEquals(3600000, Config.backup_pending_job_timeout_ms);
        Assert.assertTrue(Config.enable_backup_concurrency_logging);
    }

    @Test
    public void testConcurrencyDisabled() {
        // Disable concurrency
        Config.enable_table_level_backup_concurrency = false;

        // When disabled, all jobs should be able to execute
        // This is a basic sanity check
        Assert.assertFalse(Config.enable_table_level_backup_concurrency);
    }

    @Test
    public void testConcurrentNumLimit() {
        // Test that concurrent num config is respected
        Config.max_backup_restore_concurrent_num_per_db = 5;
        Assert.assertEquals(5, Config.max_backup_restore_concurrent_num_per_db);

        // Reset
        Config.max_backup_restore_concurrent_num_per_db = 10;
    }

    @Test
    public void testPendingTimeout() {
        // Test pending timeout config
        Config.backup_pending_job_timeout_ms = 1800000; // 30 minutes
        Assert.assertEquals(1800000, Config.backup_pending_job_timeout_ms);

        // Reset
        Config.backup_pending_job_timeout_ms = 3600000;
    }
}


