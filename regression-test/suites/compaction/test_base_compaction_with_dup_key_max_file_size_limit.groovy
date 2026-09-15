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

suite("test_base_compaction_with_dup_key_max_file_size_limit", "p2") {
    def tableName = "test_base_compaction_with_dup_key_max_file_size_limit"
    def originalDisableAutoCompaction = null
    def originalBaseCompactionFileSizeLimit = null

    // Use customer table of tpch_sf100. The table is recreated for every run, so each call must
    // load one complete copy and advance the exact expected row count.
    def rowsPerLoad = 15000000
    def expectedRows = 0
    def compactionTimeoutSeconds = 1200
    def load_tpch_sf100_customer = {
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def s3BucketName = getS3BucketName()
        def s3WithProperties = """WITH S3 (
            |"AWS_ACCESS_KEY" = "${getS3AK()}",
            |"AWS_SECRET_KEY" = "${getS3SK()}",
            |"AWS_ENDPOINT" = "${getS3Endpoint()}",
            |"AWS_REGION" = "${getS3Region()}",
            |"provider" = "${getS3Provider()}")
            |PROPERTIES(
            |"exec_mem_limit" = "8589934592",
            |"load_parallelism" = "3")""".stripMargin()
        sql "ADMIN SET FRONTEND CONFIG ('max_bytes_per_broker_scanner' = '161061273600')"
        def loadLabel = tableName + "_" + uniqueID
        // load data from cos
        def loadSql = """
            LOAD LABEL ${loadLabel}(
                DATA INFILE("s3://${s3BucketName}/regression/tpch/sf100/customer.tbl")
                INTO TABLE ${tableName}
                COLUMNS TERMINATED BY "|"
                (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
            )
            """
        loadSql = loadSql + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            logger.info("load result is ${stateResult}")
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                expectedRows += rowsPerLoad
                break
            }
            sleep(5000)
        }
    }
    try {
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        originalDisableAutoCompaction = get_be_param("disable_auto_compaction")
        originalBaseCompactionFileSizeLimit = get_be_param("base_compaction_dup_key_max_file_size_mbytes")
        set_be_param("disable_auto_compaction", "true")
        // The first base compaction builds the large base rowset used by the assertion below.
        // Keep the limit out of the way during setup; otherwise cloud base compaction filters the
        // large input rowsets before it can build that base rowset.
        set_be_param("base_compaction_dup_key_max_file_size_mbytes", "10240")

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
            )
            DUPLICATE KEY(C_CUSTKEY, C_NAME)
            DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1", "disable_auto_compaction" = "true"
            )
        """

        def tablet = (sql_return_maparray """ show tablets from ${tableName}; """)[0]
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G overlapping
        // cp: -1
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        // cp: 3
        trigger_and_wait_compaction(tableName, "cumulative", compactionTimeoutSeconds)

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        //      [3-3] 1G overlapping
        // cp: 3
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-1] 0
        //      [2-2] 1G nooverlapping
        //      [3-3] 1G nooverlapping
        // cp: 4
        trigger_and_wait_compaction(tableName, "cumulative", compactionTimeoutSeconds)

        // Build the large base rowset while the temporary 10GB limit keeps the size gate disabled.
        // rowsets:
        //      [0-3] 2G nooverlapping
        // cp: 4
        trigger_and_wait_compaction(tableName, "base", compactionTimeoutSeconds)

        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G overlapping
        // cp: 4
        load_tpch_sf100_customer.call();

        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G nooverlapping
        // cp: 5
        trigger_and_wait_compaction(tableName, "cumulative", compactionTimeoutSeconds)

        // The rowset layout is complete. Enable the limit only for the operation under test so it
        // cannot interfere with construction of the large base rowset.
        set_be_param("base_compaction_dup_key_max_file_size_mbytes", "512")

        // The first input rowset is now larger than the 512MB limit, so the size gate filters it
        // and manual base compaction must be rejected with E-808.
        // rowsets:
        //      [0-3] 2G nooverlapping
        //      [4-4] 1G nooverlapping
        // cp: 5
        def (compactionCode, compactionOut, compactionErr) = be_run_base_compaction(
                backendId_to_backendIP[trigger_backend_id],
                backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
        logger.info("Run expected-to-fail base compaction: code=${compactionCode}, out=${compactionOut}, err=${compactionErr}")
        assertEquals(0, compactionCode)
        assertTrue(compactionOut.contains("E-808"), "Expected E-808, actual response: ${compactionOut}")

        def rowCount = sql "select count(*) from ${tableName}"
        assertEquals(expectedRows as long, rowCount[0][0] as long)
    } finally {
        if (originalBaseCompactionFileSizeLimit != null) {
            set_original_be_param("base_compaction_dup_key_max_file_size_mbytes", originalBaseCompactionFileSizeLimit)
        }
        if (originalDisableAutoCompaction != null) {
            set_original_be_param("disable_auto_compaction", originalDisableAutoCompaction)
        }
    }
}
