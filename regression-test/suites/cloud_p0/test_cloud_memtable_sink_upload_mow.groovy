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

import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.suite.ClusterOptions
import java.util.concurrent.TimeUnit

suite("test_cloud_memtable_sink_upload_mow", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.beConfigs += ['share_delta_writers=false', 'enable_packed_file=true',
                          'small_file_threshold_bytes=1048576',
                          'enable_merge_on_write_correctness_check=true']
    options.feConfigs += ['cloud_stream_load_default_memtable_sink_upload=true',
                          'min_bytes_per_broker_scanner=100']
    docker(options) {
        sql "SET enable_sql_cache=false"
        def checkUniqueKeys = { table ->
            sql """
                SELECT assert_true(COUNT(*) = 0, '${table} contains duplicate keys') FROM (
                    SELECT k, COUNT(*) AS a FROM ${table} GROUP BY k HAVING a > 1
                ) duplicates
            """
        }
        sql "DROP TABLE IF EXISTS cloud_mow_source"
        sql "DROP TABLE IF EXISTS cloud_mow_direct"
        sql "DROP TABLE IF EXISTS cloud_mow_direct_seq"
        sql "DROP TABLE IF EXISTS cloud_mow_broker"
        sql """
            CREATE TABLE cloud_mow_source (n BIGINT NOT NULL)
            DUPLICATE KEY(n) DISTRIBUTED BY HASH(n) BUCKETS 12
            PROPERTIES ("replication_num"="1")
        """
        sql """
            CREATE TABLE cloud_mow_direct (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                        "disable_auto_compaction"="true")
        """
        sql """
            CREATE TABLE cloud_mow_direct_seq (
                k BIGINT NOT NULL, v BIGINT, seq BIGINT NOT NULL, INDEX idx_k(k) USING INVERTED
            ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                        "function_column.sequence_col"="seq", "inverted_index_storage_format"="V2",
                        "disable_auto_compaction"="true")
        """
        sql "SET enable_memtable_on_sink_node=false"
        sql "INSERT INTO cloud_mow_source SELECT number FROM numbers('number'='12000')"
        // Two versions leave deleted old rows in the snapshot.
        sql "INSERT INTO cloud_mow_direct VALUES (0,-1),(1,-1),(2,-1),(3,-1)"
        sql "INSERT INTO cloud_mow_direct VALUES (0,0),(1,0),(2,0),(3,0)"
        sql "INSERT INTO cloud_mow_direct_seq VALUES (0,-1,-1),(1,-1,-1),(2,-1,-1),(3,-1,-1)"
        sql "INSERT INTO cloud_mow_direct_seq VALUES (0,0,0),(1,0,0),(2,0,0),(3,0,0)"
        sql "SET enable_memtable_on_sink_node=true"
        sql "SET enable_cloud_memtable_sink_upload=true"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET enable_file_cache=false"
        sql "SET profile_level=2"
        sql "SET enable_profile=true"
        GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
        GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.sink_upload.duplicate_result")
        sql """
            /* cloud_mow_direct_profile */
            INSERT INTO cloud_mow_direct SELECT n%4,n%4*10 FROM cloud_mow_source
        """
        sql """
            /* cloud_mow_direct_seq_profile */
            INSERT INTO cloud_mow_direct_seq SELECT n%4,n*10,n+1 FROM cloud_mow_source
        """
        ['cloud_mow_direct_profile', 'cloud_mow_direct_seq_profile'].each { tag ->
            new ProfileAction(context).getProfileBySql(tag,
                    ["CloudMemtableSinkUpload: true", "CloudMemtableMowBitmap: true"])
        }
        sql "SET enable_profile=false"
        GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_upload.duplicate_result")
        order_qt_initial "SELECT * FROM cloud_mow_direct"
        order_qt_initial_seq "SELECT * FROM cloud_mow_direct_seq"
        order_qt_index "SELECT * FROM cloud_mow_direct_seq WHERE k IN (1,3)"
        ['cloud_mow_direct', 'cloud_mow_direct_seq'].each { table ->
            checkUniqueKeys(table)
            def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
            def partition = sql_return_maparray("SHOW PARTITIONS FROM ${table}")[0]
            def ms = cluster.getAllMetaservices()[0]
            getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                code, body ->
                    assertEquals(200, code)
                    def meta = parseJson(body)
                    logger.info("MOW rowset layout: {}", meta)
                    def ids = meta.segment_ids ?: []
                    quickTest("layout_${table}", """
                        SELECT ${ids.size() == (meta.num_segments as int)},
                            ${ids.toSet().size() == ids.size()},
                            ${ids.any { (it as long) >= 1000 }},
                            ${(meta.packed_slice_locations ?: [:]).size() > 0}
                    """, true)
            }
        }
        sql "INSERT INTO cloud_mow_direct_seq VALUES (0,-1,1)"
        order_qt_lower_seq "SELECT * FROM cloud_mow_direct_seq"
        sql "INSERT INTO cloud_mow_direct VALUES (0,99)"
        order_qt_new_version "SELECT * FROM cloud_mow_direct"
        sql "INSERT INTO cloud_mow_direct_seq SELECT n%4,n*10,n FROM cloud_mow_source WHERE n<0"
        order_qt_empty "SELECT * FROM cloud_mow_direct_seq"
        sql "INSERT INTO cloud_mow_direct_seq (k,v,seq,__DORIS_DELETE_SIGN__) VALUES (2,130000,13000,1)"
        order_qt_delete "SELECT * FROM cloud_mow_direct_seq"
        sql "INSERT INTO cloud_mow_direct_seq VALUES (2,-1,1)"
        order_qt_lower_after_delete "SELECT * FROM cloud_mow_direct_seq"
        sql "INSERT INTO cloud_mow_direct_seq VALUES (2,140000,14000)"
        order_qt_reinsert "SELECT * FROM cloud_mow_direct_seq"
        trigger_and_wait_compaction("cloud_mow_direct", "full")
        trigger_and_wait_compaction("cloud_mow_direct_seq", "full")
        order_qt_compacted "SELECT * FROM cloud_mow_direct"
        order_qt_compacted_seq "SELECT * FROM cloud_mow_direct_seq"
        checkUniqueKeys("cloud_mow_direct")
        checkUniqueKeys("cloud_mow_direct_seq")

        streamLoad {
            table "cloud_mow_direct_seq"
            set "column_separator", ","
            set "memtable_on_sink_node", "true"
            inputStream new ByteArrayInputStream("0,150000,15000\n3,-1,1\n".getBytes())
            check { result, exception, startTime, endTime ->
                if (exception != null) { throw exception }
                assertEquals("Success", parseJson(result).Status)
            }
        }
        order_qt_stream "SELECT * FROM cloud_mow_direct_seq"

        // Freeze the snapshot, then replace its rowsets with a concurrent load and compaction.
        def block = "CloudRowsetBuilder.sink_mow.snapshot_ready"
        def backends = sql_return_maparray("SHOW BACKENDS")
        GetDebugPoint().enableDebugPointForAllBEs(block, [timeout: "120"])
        def pending = thread {
            sql "SET enable_memtable_on_sink_node=true"
            sql "SET enable_cloud_memtable_sink_upload=true"
            sql "INSERT INTO cloud_mow_direct_seq VALUES (0,200000,20000)"
        }
        try {
            awaitUntil(60) {
                backends.any { be ->
                    def (code, out, err) = curl("GET",
                            "http://${be.Host}:${be.BrpcPort}/vars/cloud_memtable_mow_snapshot_waiters")
                    assertEquals(0, code, err)
                    def ready = out =~ /cloud_memtable_mow_snapshot_waiters\s*:\s*(\d+)/
                    ready.find() && ready.group(1).toLong() > 0
                }
            }
            sql "SET enable_memtable_on_sink_node=false"
            sql "INSERT INTO cloud_mow_direct_seq VALUES (0,210000,21000)"
            trigger_and_wait_compaction("cloud_mow_direct_seq", "full")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(block)
            sql "SET enable_memtable_on_sink_node=true"
        }
        pending.get(120, TimeUnit.SECONDS)
        order_qt_concurrent_compaction "SELECT * FROM cloud_mow_direct_seq"
        checkUniqueKeys("cloud_mow_direct_seq")

        GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.sink_mow.after_bitmap_failure")
        try {
            test {
                sql "INSERT INTO cloud_mow_direct_seq VALUES (99,990000,99000)"
                exception "injected failure after sink MOW bitmap calculation"
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_mow.after_bitmap_failure")
        }
        order_qt_failed_invisible "SELECT * FROM cloud_mow_direct_seq"

        // Cluster keys encode a physical row ID in the primary-key index.
        sql "DROP TABLE IF EXISTS cloud_mow_cluster"
        sql """
            CREATE TABLE cloud_mow_cluster (k BIGINT NOT NULL, v BIGINT NOT NULL, seq BIGINT NOT NULL)
            UNIQUE KEY(k) ORDER BY(v) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                        "function_column.sequence_col"="seq", "disable_auto_compaction"="true")
        """
        sql "INSERT INTO cloud_mow_cluster SELECT n%4,-n,n+1 FROM cloud_mow_source"
        order_qt_cluster "SELECT * FROM cloud_mow_cluster"
        sql "INSERT INTO cloud_mow_cluster VALUES (0,999,20000),(1,999,1)"
        order_qt_cluster_update "SELECT * FROM cloud_mow_cluster"
        trigger_and_wait_compaction("cloud_mow_cluster", "full")
        order_qt_cluster_compacted "SELECT * FROM cloud_mow_cluster"
        checkUniqueKeys("cloud_mow_cluster")

        setBeConfigTemporary(['enable_packed_file': 'false']) {
            sql "DROP TABLE IF EXISTS cloud_mow_plain_reference"
            sql "DROP TABLE IF EXISTS cloud_mow_plain"
            sql "CREATE TABLE cloud_mow_plain_reference LIKE cloud_mow_direct_seq"
            sql "CREATE TABLE cloud_mow_plain LIKE cloud_mow_direct_seq"
            try {
                sql "SET enable_memtable_on_sink_node=false"
                sql "INSERT INTO cloud_mow_plain_reference VALUES (0,-1,-1),(1,-1,-1)"
                sql "INSERT INTO cloud_mow_plain VALUES (0,-1,-1),(1,-1,-1)"
                sql "INSERT INTO cloud_mow_plain_reference SELECT n%4,n*10,n+1 FROM cloud_mow_source"
                def expected = sql "SELECT k,v,seq FROM cloud_mow_plain_reference ORDER BY k"
                sql "SET enable_memtable_on_sink_node=true"
                sql "SET enable_profile=true"
                sql """
                    /* cloud_mow_plain_profile */
                    INSERT INTO cloud_mow_plain SELECT n%4,n*10,n+1 FROM cloud_mow_source
                """
                new ProfileAction(context).getProfileBySql("cloud_mow_plain_profile",
                        ["CloudMemtableSinkUpload: true", "CloudMemtableMowBitmap: true"])
                sql "SET enable_profile=false"
                checkUniqueKeys("cloud_mow_plain")
                assertEquals(expected, sql("SELECT k,v,seq FROM cloud_mow_plain ORDER BY k"))
                def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_mow_plain")[0]
                def partition = sql_return_maparray("SHOW PARTITIONS FROM cloud_mow_plain")[0]
                def ms = cluster.getAllMetaservices()[0]
                getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                    code, body ->
                        assertEquals(200, code)
                        def meta = parseJson(body)
                        assertTrue((meta.packed_slice_locations ?: [:]).isEmpty(),
                                "Unpacked MOW rowset contains packed mappings: ${meta}")
                }
                trigger_and_wait_compaction("cloud_mow_plain", "full")
                checkUniqueKeys("cloud_mow_plain")
                assertEquals(expected, sql("SELECT k,v,seq FROM cloud_mow_plain ORDER BY k"))
            } finally {
                sql "SET enable_profile=false"
                sql "SET enable_memtable_on_sink_node=true"
            }
        }

        sql """
            CREATE TABLE cloud_mow_broker (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true")
        """
        // Switch the same table from sink upload to forwarding and overwrite a sink-uploaded key.
        [true, false].each { sinkUpload ->
            if (!sinkUpload) {
                sql "INSERT INTO cloud_mow_broker VALUES (50,-1)"
                GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            }
            sql "SET enable_cloud_memtable_sink_upload=${sinkUpload}"
            def label = "mow_broker_" + UUID.randomUUID().toString().replace('-', '_')
            sql "SET enable_profile=true"
            sql """
                LOAD LABEL ${label} (
                    DATA INFILE("s3://${getS3BucketName()}/regression/load/data/basic_data.csv")
                    INTO TABLE cloud_mow_broker COLUMNS TERMINATED BY "|" FORMAT AS "CSV"
                    (k, c01, c02, c03, c04, c05, c06, c07, c08, c09,
                        c10, c11, c12, c13, c14, c15, c16, c17, c18) SET (v=k*2)
                ) WITH S3 (
                    "AWS_ACCESS_KEY"="${getS3AK()}", "AWS_SECRET_KEY"="${getS3SK()}",
                    "AWS_ENDPOINT"="${getS3Endpoint()}", "AWS_REGION"="${getS3Region()}",
                    "provider"="${getS3Provider()}"
                ) PROPERTIES ("load_parallelism"="1")
            """
            waitForBrokerLoadDone(label)
            def load = sql_return_maparray("SHOW LOAD WHERE LABEL = '${label}'")[0]
            assertEquals("FINISHED", load.State, "Broker load did not finish: ${load}")
            new ProfileAction(context).getProfile(load.JobId.toString(),
                    sinkUpload ? ["CloudMemtableSinkUpload: true", "CloudMemtableMowBitmap: true"]
                           : ["DeltaWriterV2"])
            sql "SET enable_profile=false"
            quickTest("broker_${sinkUpload}", "SELECT * FROM cloud_mow_broker", true)
            checkUniqueKeys("cloud_mow_broker")
        }
    }
}
