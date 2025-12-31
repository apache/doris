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

package backup_restore

/**
 * Unified backup and restore test helper class
 * 
 * Provides:
 * 1. S3 repository operations
 * 2. Local repository operations
 * 3. Table creation and data insertion
 * 4. Data verification
 * 5. Debug Point management
 */
class BackupRestoreTestHelper {
    private static final String LOCAL_REPO_NAME = "__keep_on_local__"
    
    private def sql
    private def syncer
    private def logger
    
    // Debug Point related
    private String feHost
    private int fePort
    
    /**
     * Constructor
     */
    BackupRestoreTestHelper(sql, syncer, String feHost = null, int fePort = 0) {
        this.sql = sql
        this.syncer = syncer
        this.logger = org.apache.logging.log4j.LogManager.getLogger(this.class)
        this.feHost = feHost
        this.fePort = fePort
    }
    
    // ============================================================
    // S3 Repository Operations
    // ============================================================
    
    /**
     * Backup to S3 repository
     */
    boolean backupToS3(String dbName, String snapshotName, String repoName, String tableName) {
        return backupToS3(dbName, snapshotName, repoName, [tableName])
    }
    
    /**
     * Backup multiple tables to S3 repository
     */
    boolean backupToS3(String dbName, String snapshotName, String repoName, List<String> tableNames) {
        try {
            String tables = tableNames.join(", ")
            logger.info("S3 BACKUP: ${dbName}.${snapshotName} ON (${tables})")
            
            sql """
                BACKUP SNAPSHOT ${dbName}.${snapshotName}
                TO `${repoName}`
                ON (${tables})
            """
            
            syncer.waitSnapshotFinish(dbName)
            logger.info("S3 BACKUP completed: ${dbName}.${snapshotName}")
            return true
        } catch (Exception e) {
            logger.error("S3 BACKUP failed: ${dbName}.${snapshotName}", e)
            return false
        }
    }
    
    /**
     * Restore from S3 repository
     */
    boolean restoreFromS3(String dbName, String snapshotName, String repoName, 
                          String tableName, Map<String, String> properties = [:]) {
        try {
            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            if (snapshot == null) {
                logger.error("Snapshot not found: ${snapshotName}")
                return false
            }
            
            logger.info("S3 RESTORE: ${dbName}.${snapshotName} ON (${tableName})")
            
            def propStr = properties.collect { k, v -> "\"${k}\" = \"${v}\"" }.join(",\n")
            if (propStr) {
                propStr = "\"backup_timestamp\" = \"${snapshot}\",\n" + propStr
            } else {
                propStr = "\"backup_timestamp\" = \"${snapshot}\""
            }
            
            sql """
                RESTORE SNAPSHOT ${dbName}.${snapshotName} FROM `${repoName}`
                ON (`${tableName}`)
                PROPERTIES (
                    ${propStr}
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            logger.info("S3 RESTORE completed: ${dbName}.${snapshotName}")
            return true
        } catch (Exception e) {
            logger.error("S3 RESTORE failed: ${dbName}.${snapshotName}", e)
            return false
        }
    }
    
    // ============================================================
    // Local Repository Operations (for Thrift RPC coverage)
    // ============================================================
    
    /**
     * Backup to local repository
     * Used to test Syncer API and Thrift RPC path
     */
    boolean backupToLocal(String dbName, String snapshotName, String tableName) {
        try {
            logger.info("LOCAL BACKUP: ${dbName}.${snapshotName} ON (${tableName})")
            
            sql """
                BACKUP SNAPSHOT ${dbName}.${snapshotName}
                TO `${LOCAL_REPO_NAME}`
                ON (${tableName})
            """
            
            syncer.waitSnapshotFinish(dbName)
            logger.info("LOCAL BACKUP completed: ${dbName}.${snapshotName}")
            return true
        } catch (Exception e) {
            logger.error("LOCAL BACKUP failed: ${dbName}.${snapshotName}", e)
            return false
        }
    }
    
    /**
     * Restore from local repository using Syncer API
     * This tests the Thrift RPC path: Syncer -> FrontendServiceImpl.restoreSnapshot
     * 
     * IMPORTANT: This is the ONLY way to test the new storage_medium and 
     * medium_allocation_mode parameters in FrontendServiceImpl.restoreSnapshot,
     * as SQL RESTORE command does NOT call this RPC method.
     */
    boolean restoreFromLocal(String dbName, String snapshotName, String tableName, 
                             Map<String, String> properties = [:]) {
        try {
            logger.info("LOCAL RESTORE (Syncer API): ${dbName}.${snapshotName} ON (${tableName})")
            logger.info("Properties: ${properties}")
            
            // Get snapshot via Syncer API
            if (!syncer.getSnapshot(snapshotName, tableName)) {
                logger.error("Failed to get snapshot: ${snapshotName}")
                return false
            }
            
            // Extract storage_medium and medium_allocation_mode from properties
            String storageMedium = properties.get("storage_medium")
            String mediumAllocationMode = properties.get("medium_allocation_mode")
            
            if (storageMedium != null) {
                logger.info("Setting storage_medium for RPC: ${storageMedium}")
            }
            if (mediumAllocationMode != null) {
                logger.info("Setting medium_allocation_mode for RPC: ${mediumAllocationMode}")
            }
            
            // Restore via Syncer API with parameters (calls Thrift RPC)
            // This will test FrontendServiceImpl.restoreSnapshot
            if (!syncer.restoreSnapshot(false, storageMedium, mediumAllocationMode)) {
                logger.error("Failed to restore snapshot: ${snapshotName}")
                return false
            }
            
            syncer.waitAllRestoreFinish(dbName)
            logger.info("LOCAL RESTORE completed: ${dbName}.${snapshotName}")
            return true
        } catch (Exception e) {
            logger.error("LOCAL RESTORE failed: ${dbName}.${snapshotName}", e)
            return false
        }
    }
    
    // ============================================================
    // Table Operations
    // ============================================================
    
    /**
     * Create simple table
     */
    void createSimpleTable(String dbName, String tableName, Map<String, String> properties = [:]) {
        def propStr = properties.collect { k, v -> "\"${k}\" = \"${v}\"" }.join(",\n")
        if (!propStr) {
            propStr = "\"replication_num\" = \"1\""
        } else if (!propStr.contains("replication_num")) {
            propStr += ",\n\"replication_num\" = \"1\""
        }
        
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                ${propStr}
            )
        """
        logger.info("Created table: ${dbName}.${tableName}")
    }
    
    /**
     * Create partition table
     */
    void createPartitionTable(String dbName, String tableName, Map<String, String> properties = [:]) {
        def propStr = properties.collect { k, v -> "\"${k}\" = \"${v}\"" }.join(",\n")
        if (!propStr) {
            propStr = "\"replication_num\" = \"1\""
        } else if (!propStr.contains("replication_num")) {
            propStr += ",\n\"replication_num\" = \"1\""
        }
        
        sql """
            CREATE TABLE ${dbName}.${tableName} (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                ${propStr}
            )
        """
        logger.info("Created partition table: ${dbName}.${tableName}")
    }
    
    /**
     * Insert test data
     */
    void insertData(String dbName, String tableName, List<String> values) {
        sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(',')}"
        logger.info("Inserted ${values.size()} rows into ${dbName}.${tableName}")
    }
    
    /**
     * Truncate table
     */
    void truncateTable(String dbName, String tableName) {
        sql "TRUNCATE TABLE ${dbName}.${tableName}"
        logger.info("Truncated table: ${dbName}.${tableName}")
    }
    
    /**
     * Drop table
     */
    void dropTable(String dbName, String tableName, boolean force = true) {
        try {
            if (force) {
                sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            } else {
                sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
            }
            logger.info("Dropped table: ${dbName}.${tableName}")
        } catch (Exception e) {
            logger.warn("Failed to drop table: ${dbName}.${tableName}", e)
        }
    }
    
    // ============================================================
    // Data Verification
    // ============================================================
    
    /**
     * Verify row count
     */
    boolean verifyRowCount(String dbName, String tableName, int expectedCount) {
        def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}"
        def actualCount = result[0][0]
        if (actualCount == expectedCount) {
            logger.info("✓ Row count verified: ${actualCount} == ${expectedCount}")
            return true
        } else {
            logger.error("✗ Row count mismatch: ${actualCount} != ${expectedCount}")
            return false
        }
    }
    
    /**
     * Verify data exists
     */
    boolean verifyDataExists(String dbName, String tableName, String whereClause, boolean shouldExist = true) {
        def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName} WHERE ${whereClause}"
        def count = result[0][0]
        
        if (shouldExist && count > 0) {
            logger.info("✓ Data exists: ${whereClause}")
            return true
        } else if (!shouldExist && count == 0) {
            logger.info("✓ Data not exists: ${whereClause}")
            return true
        } else {
            logger.error("✗ Data verification failed: ${whereClause}, count=${count}, shouldExist=${shouldExist}")
            return false
        }
    }
    
    /**
     * Verify table property
     */
    boolean verifyTableProperty(String dbName, String tableName, String propertyName) {
        def result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
        def createTableStr = result[0][1]
        
        if (createTableStr.contains(propertyName)) {
            logger.info("✓ Table property exists: ${propertyName}")
            return true
        } else {
            logger.error("✗ Table property not found: ${propertyName}")
            return false
        }
    }
    
    // ============================================================
    // Debug Point Management
    // ============================================================
    
    /**
     * Enable Debug Point
     */
    void enableDebugPoint(String debugPointName) {
        if (feHost == null || fePort == 0) {
            logger.warn("FE host/port not configured, skipping debug point: ${debugPointName}")
            return
        }
        
        try {
            org.apache.doris.regression.util.DebugPoint.enableDebugPoint(
                feHost, fePort, 
                org.apache.doris.regression.util.NodeType.FE, 
                debugPointName
            )
            logger.info("Enabled debug point: ${debugPointName}")
        } catch (Exception e) {
            logger.error("Failed to enable debug point: ${debugPointName}", e)
            throw e
        }
    }
    
    /**
     * Disable Debug Point
     */
    void disableDebugPoint(String debugPointName) {
        if (feHost == null || fePort == 0) {
            logger.warn("FE host/port not configured, skipping debug point: ${debugPointName}")
            return
        }
        
        try {
            org.apache.doris.regression.util.DebugPoint.disableDebugPoint(
                feHost, fePort, 
                org.apache.doris.regression.util.NodeType.FE, 
                debugPointName
            )
            logger.info("Disabled debug point: ${debugPointName}")
        } catch (Exception e) {
            logger.warn("Failed to disable debug point: ${debugPointName}", e)
        }
    }
    
    /**
     * Execute code within Debug Point environment
     */
    void withDebugPoint(String debugPointName, Closure closure) {
        try {
            enableDebugPoint(debugPointName)
            closure()
        } finally {
            disableDebugPoint(debugPointName)
        }
    }
    
    // ============================================================
    // Utility Methods
    // ============================================================
    
    /**
     * Get local repository name
     */
    static String getLocalRepoName() {
        return LOCAL_REPO_NAME
    }
    
    /**
     * Log separator
     */
    void logSeparator(String title) {
        logger.info("=" * 60)
        logger.info("  ${title}")
        logger.info("=" * 60)
    }
    
    /**
     * Log test start
     */
    void logTestStart(String testName) {
        logger.info(">>> Test Start: ${testName}")
    }
    
    /**
     * Log test end
     */
    void logTestEnd(String testName, boolean success = true) {
        if (success) {
            logger.info("<<< Test Passed: ${testName}")
        } else {
            logger.error("<<< Test Failed: ${testName}")
        }
    }
}

