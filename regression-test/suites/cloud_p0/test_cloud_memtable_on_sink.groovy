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

suite("test_cloud_memtable_on_sink", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    // Split the small S3 CSV across all three BEs with load_parallelism = 1.
    options.feConfigs += ['min_bytes_per_broker_scanner = 100']
    // The segment-count checks require the configured four-row CSV batches.
    options.beConfigs += ['enable_packed_file=true', 'small_file_threshold_bytes=1048576',
                          'enable_adaptive_batch_size=false']

    // Ordinary Stream Load inherits the FE default unless its header overrides it.
    options.feConfigs += ['cloud_stream_load_default_memtable_sink_upload = true']
    docker(options) {
        def loadS3 = { table, label ->
            sql """
                LOAD LABEL ${label} (
                    DATA INFILE("s3://${getS3BucketName()}/regression/load/data/basic_data.csv")
                    INTO TABLE ${table} COLUMNS TERMINATED BY "|" FORMAT AS "CSV"
                    (k, c01, c02, c03, c04, c05, c06, c07, c08, c09,
                        c10, c11, c12, c13, c14, c15, c16, c17, c18)
                    SET (v = k * 2)
                ) WITH S3 (
                    "AWS_ACCESS_KEY"="${getS3AK()}", "AWS_SECRET_KEY"="${getS3SK()}",
                    "AWS_ENDPOINT"="${getS3Endpoint()}", "AWS_REGION"="${getS3Region()}",
                    "provider"="${getS3Provider()}"
                ) PROPERTIES ("load_parallelism"="1")
            """
            waitForBrokerLoadDone(label)
            return sql_return_maparray("SHOW LOAD WHERE LABEL = '${label}'")[0]
        }

        def testDuplicate = {
            [false, true].each { sinkUpload ->
                sql "SET enable_cloud_memtable_sink_upload = ${sinkUpload}"
                if (sinkUpload) {
                    GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
                    GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.sink_upload.duplicate_result")
                }

                sql "SET enable_sql_cache = false"
                sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink_source"
                sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink"

                sql """
                    CREATE TABLE test_cloud_duplicate_memtable_on_sink_source (
                        k BIGINT NOT NULL,
                        v BIGINT NOT NULL
                    )
                    DUPLICATE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 3
                    PROPERTIES ("replication_num" = "1")
                """
                sql """
                    CREATE TABLE test_cloud_duplicate_memtable_on_sink (
                        k BIGINT NOT NULL,
                        v BIGINT NOT NULL
                    )
                    DUPLICATE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num" = "1")
                """

                sql "SET enable_memtable_on_sink_node = false"
                sql """
                    INSERT INTO test_cloud_duplicate_memtable_on_sink_source
                    SELECT number, number * 2 FROM numbers("number" = "100000")
                """

                try {
                    sql "SET enable_memtable_on_sink_node = true"
                    sql "SET profile_level = 2"
                    sql "SET enable_profile = true"
                    sql """
                        /* cloud_duplicate_memtable_on_sink_profile_${sinkUpload} */
                        INSERT INTO test_cloud_duplicate_memtable_on_sink
                        SELECT k, v FROM test_cloud_duplicate_memtable_on_sink_source
                    """
                    def required = ["DeltaWriterV2"]
                    if (sinkUpload) {
                        required += "CloudMemtableSinkUpload: true"
                    }
                    def profileString = new ProfileAction(context).getProfileBySql(
                            "cloud_duplicate_memtable_on_sink_profile_${sinkUpload}", required)
                    logger.info("memtable-on-sink profile:\n{}", profileString)
                } finally {
                    sql "SET enable_profile = false"
                    sql "SET enable_memtable_on_sink_node = false"
                }

                sql """
                    SELECT assert_true(
                        COUNT(*) = 100000
                            AND SUM(k) = 4999950000
                            AND SUM(v) = 9999900000,
                        'cloud duplicate memtable-on-sink result mismatch')
                    FROM test_cloud_duplicate_memtable_on_sink
                """

                sql "DROP TABLE IF EXISTS test_cloud_duplicate_memtable_on_sink_s3"
                sql """
                    CREATE TABLE test_cloud_duplicate_memtable_on_sink_s3 (
                        k BIGINT NOT NULL,
                        v BIGINT NOT NULL,
                        INDEX idx_v (v) USING INVERTED
                    )
                    DUPLICATE KEY(k)
                    DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES (
                        "replication_num" = "1",
                        "inverted_index_storage_format" = "V2",
                        "disable_auto_compaction" = "true"
                    )
                """

                def label = "cloud_duplicate_memtable_on_sink_s3_" + UUID.randomUUID().toString().replace('-', '_')
                try {
                    sql "SET enable_memtable_on_sink_node = true"
                    sql "SET enable_profile = true"
                    // The three scanners read 6, 6, and 8 rows. Flush after the first four rows on
                    // each BE, then flush the remaining rows at close to produce two segments each.
                    sql "SET broker_load_batch_size = 4"
                    GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush", [execute: 1])
                    def load = loadS3("test_cloud_duplicate_memtable_on_sink_s3", label)
                    assertEquals("FINISHED", load.State, "S3 load did not finish: ${load}")

                    def profileString = new ProfileAction(context).getProfile(
                            load.JobId.toString(), ["DeltaWriterV2", "NumScanners"])
                    logger.info("S3 memtable-on-sink profile:\n{}", profileString)

                    // Only inspect per-BE pipelines, excluding the merged profile's duplicate counters.
                    def pipelines = profileString.split(/(?m)(?=^[ \t]*(?:Pipeline \d+|FragmentLevelProfile:)\(host=)/)
                            .findAll { it.trim().startsWith("Pipeline ") }
                    def backends = sql_return_maparray("SHOW BACKENDS")
                    backends.each { backend ->
                        def pipeline = pipelines.find {
                            it.readLines()[0].contains("hostname:${backend.Host},") && it.contains("DeltaWriterV2")
                        }
                        assertNotNull(pipeline, "Missing S3 sink writer on BE ${backend.Host}")
                        assertTrue(pipeline.contains("FILE_SCAN_OPERATOR"), "Missing S3 scanner on BE ${backend.Host}")
                        assertTrue((pipeline =~ /(?m)^\s*- NumScanners: 1\s*$/).find(),
                                "Expected one S3 scanner on BE ${backend.Host}")
                        assertTrue((pipeline =~ /(?m)^\s*- SegmentNum: 2\s*$/).find(),
                                "Expected two flushed segments on BE ${backend.Host}")
                    }
                } finally {
                    GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
                    sql "SET enable_profile = false"
                    sql "SET enable_memtable_on_sink_node = false"
                }

                // Read both segment data and V2 indexes without the uploader's file cache.
                sql "SET enable_file_cache = false"
                quickTest("s3_rows_${sinkUpload}", """
                    SELECT COUNT(*), SUM(k), SUM(v)
                    FROM test_cloud_duplicate_memtable_on_sink_s3
                """, true)
                quickTest("s3_index_${sinkUpload}", """
                    SELECT k, v FROM test_cloud_duplicate_memtable_on_sink_s3 WHERE v IN (62, 100, 114)
                """, true)
                def tablet = sql_return_maparray("SHOW TABLETS FROM test_cloud_duplicate_memtable_on_sink_s3")[0]
                def partition = sql_return_maparray("SHOW PARTITIONS FROM test_cloud_duplicate_memtable_on_sink_s3")[0]
                def ms = cluster.getAllMetaservices()[0]
                getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                    responseCode, body ->
                        assertEquals(200, responseCode)
                        logger.info("S3 memtable-on-sink rowset meta: {}", body)
                        def rowsetMeta = parseJson(body)
                        def locations = rowsetMeta.packed_slice_locations
                        def segmentIds = rowsetMeta.segment_ids ?: (0..<(rowsetMeta.num_segments as int)).toList()
                        logger.info("S3 rowset layout (sinkUpload={}): {}", sinkUpload, [
                            tablet_id: tablet.TabletId, version: partition.VisibleVersion,
                            rowset_id: rowsetMeta.rowset_id_v2, segment_ids: segmentIds,
                            num_segment_rows: rowsetMeta.num_segment_rows,
                            segments_file_size: rowsetMeta.segments_file_size,
                            packed_files: locations.keySet().sort()
                        ])
                        // Only the destination rowset's first segment and its V2 index are packed.
                        // The other five segments retain independent files.
                        quickTest("s3_packed_meta_${sinkUpload}", """
                            SELECT ${rowsetMeta.num_segments as int}, ${rowsetMeta.num_rows as long},
                                ${locations.size()},
                                ${locations.keySet().count { it.endsWith('_0.dat') }},
                                ${locations.keySet().count { it.endsWith('_0.idx') }},
                                ${segmentIds.count { (it as long) >= 1000 }}, '${segmentIds.join(",")}'
                        """, true)
                }
            }

            GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_upload.duplicate_result")

            // Stream Load inherits the FE sink-upload default independently of SQL session options.
            // This hook is only reached by the memtable-on-sink receiving writer.
            // A successful fallback would fail this check instead of silently passing.
            def failClose = "LoadStreamWriter.close.cancelled"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(failClose)
                streamLoad {
                    table "test_cloud_duplicate_memtable_on_sink_s3"
                    set "column_separator", ","
                    set "memtable_on_sink_node", "true"
                    set "group_commit", "off_mode"
                    inputText "100,200\n101,202\n"
                    check { result, exception, startTime, endTime ->
                        if (exception != null) { throw exception }
                        def response = parseJson(result)
                        sql "SELECT assert_true('${response.Status}' = 'Fail', 'Stream Load should fail at receiver close')"
                    }
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(failClose)
            }
            sql """
                SELECT assert_true(COUNT(*) = 20 AND SUM(k) = 1133 AND SUM(v) = 2266, 'failed Stream Load changed visible rows')
                FROM test_cloud_duplicate_memtable_on_sink_s3
            """

            def failUpload = "DeltaWriterV2.sink_upload.after_upload_failure"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(failUpload)
                [null, "true"].each { sinkUploadHeader ->
                    streamLoad {
                        table "test_cloud_duplicate_memtable_on_sink_s3"
                        set "column_separator", ","
                        set "memtable_on_sink_node", "true"
                        if (sinkUploadHeader != null) {
                            set "cloud_memtable_sink_upload", sinkUploadHeader
                        }
                        set "group_commit", "off_mode"
                        inputText "100,200\n101,202\n"
                        check { result, exception, startTime, endTime ->
                            if (exception != null) { throw exception }
                            def response = parseJson(result)
                            sql "SELECT assert_true('${response.Status}' = 'Fail', 'Stream Load should reach sink upload')"
                            assertTrue(response.Message.contains("injected failure after sink upload"))
                        }
                    }
                }
                // Allow file bytes while keeping the sink-upload failure hook enabled.
                GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
                streamLoad {
                    table "test_cloud_duplicate_memtable_on_sink_s3"
                    set "column_separator", ","
                    set "memtable_on_sink_node", "true"
                    set "cloud_memtable_sink_upload", "false"
                    set "group_commit", "off_mode"
                    inputStream new ByteArrayInputStream("100,200\n101,202\n".getBytes())
                    time 30000
                    check { result, exception, startTime, endTime ->
                        if (exception != null) {
                            throw exception
                        }
                        assertEquals("Success", parseJson(result).Status)
                    }
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(failUpload)
                GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            }
            order_qt_stream_rows """
                SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_duplicate_memtable_on_sink_s3
            """

            sql "SET enable_memtable_on_sink_node = true"
            sql "SET enable_cloud_memtable_sink_upload = true"
            // An uploaded partial rowset must remain invisible if its result cannot be sent.
            try {
                GetDebugPoint().enableDebugPointForAllBEs("DeltaWriterV2.sink_upload.after_upload_failure")
                test {
                    sql "INSERT INTO test_cloud_duplicate_memtable_on_sink_s3 VALUES (999, 1998)"
                    exception "injected failure after sink upload"
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_upload.after_upload_failure")
            }
            order_qt_failed_upload_rows """
                SELECT COUNT(*), SUM(k), SUM(v) FROM test_cloud_duplicate_memtable_on_sink_s3
            """
        }

        def testAggregateAndMor = {
            sql "DROP TABLE IF EXISTS cloud_memtable_models_source"
            sql """
                CREATE TABLE cloud_memtable_models_source (n BIGINT NOT NULL)
                DUPLICATE KEY(n) DISTRIBUTED BY HASH(n) BUCKETS 12
                PROPERTIES ("replication_num"="1")
            """
            sql "SET enable_memtable_on_sink_node=false"
            sql "INSERT INTO cloud_memtable_models_source SELECT number FROM numbers('number'='12000')"
            sql "SET enable_memtable_on_sink_node=true"
            sql "SET parallel_pipeline_task_num=4"
            sql "SET profile_level=2"
            sql "SET enable_file_cache=false"

            def aggQuery = """
                SELECT k, s, lo, hi, bitmap_count(b), hll_cardinality(h), r, rn
                FROM cloud_memtable_agg
            """
            def morQuery = "SELECT k, v FROM cloud_memtable_mor"
            def seqQuery = "SELECT k, v, seq FROM cloud_memtable_mor_seq"
            def checkLayout = { table, tag ->
                def tablet = sql_return_maparray("SHOW TABLETS FROM ${table}")[0]
                def partition = sql_return_maparray("SHOW PARTITIONS FROM ${table}")[0]
                def ms = cluster.getAllMetaservices()[0]
                getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                    code, body ->
                        assertEquals(200, code)
                        def meta = parseJson(body)
                        logger.info("{} rowset layout: {}", table, meta)
                        def ids = meta.segment_ids ?: []
                        quickTest(tag, """
                            SELECT ${meta.segments_overlap_pb == 'OVERLAPPING'},
                                ${ids.size() == (meta.num_segments as int)},
                                ${ids.toSet().size() == ids.size()},
                                ${ids.any { (it as long) >= 1000 }},
                                ${(meta.packed_slice_locations ?: [:]).size() > 0}
                        """, true)
                }
            }
            def loadModelS3 = { table, sinkUpload ->
                def label = "agg_mor_" + UUID.randomUUID().toString().replace('-', '_')
                sql "SET enable_profile=true"
                def load = loadS3(table, label)
                def required = sinkUpload ? ["DeltaWriterV2", "CloudMemtableSinkUpload: true"] : ["DeltaWriterV2"]
                new ProfileAction(context).getProfile(load.JobId.toString(), required)
                sql "SET enable_profile=false"
            }

            [false, true].each { sinkUpload ->
                sql "SET enable_cloud_memtable_sink_upload=${sinkUpload}"
                if (sinkUpload) {
                    GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
                }
                sql "DROP TABLE IF EXISTS cloud_memtable_agg"
                sql "DROP TABLE IF EXISTS cloud_memtable_mor"
                sql "DROP TABLE IF EXISTS cloud_memtable_mor_seq"
                sql "DROP TABLE IF EXISTS cloud_memtable_agg_broker"
                sql "DROP TABLE IF EXISTS cloud_memtable_mor_broker"
                sql """
                    CREATE TABLE cloud_memtable_agg (
                        k BIGINT NOT NULL, s BIGINT SUM, lo BIGINT MIN, hi BIGINT MAX,
                        b BITMAP BITMAP_UNION, h HLL HLL_UNION,
                        r BIGINT REPLACE, rn BIGINT REPLACE_IF_NOT_NULL
                    ) AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
                """
                sql """
                    CREATE TABLE cloud_memtable_mor (k BIGINT NOT NULL, v BIGINT)
                    UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                                "disable_auto_compaction"="true")
                """
                sql """
                    CREATE TABLE cloud_memtable_mor_seq (
                        k BIGINT NOT NULL, v BIGINT, seq BIGINT NOT NULL,
                        INDEX idx_k (k) USING INVERTED
                    ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                                "function_column.sequence_col"="seq", "inverted_index_storage_format"="V2",
                                "disable_auto_compaction"="true")
                """
                sql "SET enable_profile=true"
                sql """
                    /* cloud_memtable_agg_${sinkUpload} */
                    INSERT INTO cloud_memtable_agg
                    SELECT n % 4, n, n, n, to_bitmap(n), hll_hash(CAST(n % 4 AS STRING)),
                        n % 4 + 10, IF(n % 8 < 4, n % 4 + 20, NULL)
                    FROM cloud_memtable_models_source
                """
                sql """
                    /* cloud_memtable_mor_${sinkUpload} */
                    INSERT INTO cloud_memtable_mor SELECT n % 4, n % 4 * 10 FROM cloud_memtable_models_source
                """
                sql """
                    /* cloud_memtable_mor_seq_${sinkUpload} */
                    INSERT INTO cloud_memtable_mor_seq SELECT n % 4, n * 10, n FROM cloud_memtable_models_source
                """
                ['agg', 'mor', 'mor_seq'].each { model ->
                    def required = sinkUpload ? ["DeltaWriterV2", "CloudMemtableSinkUpload: true"] : ["DeltaWriterV2"]
                    new ProfileAction(context).getProfileBySql("cloud_memtable_${model}_${sinkUpload}", required)
                    if (sinkUpload) {
                        checkLayout("cloud_memtable_${model}", "layout_${model}")
                    }
                }
                sql "SET enable_profile=false"
                quickTest("agg_${sinkUpload}", aggQuery, true)
                quickTest("mor_${sinkUpload}", morQuery, true)
                quickTest("seq_${sinkUpload}", seqQuery, true)
                quickTest("seq_index_${sinkUpload}", seqQuery + " WHERE k IN (1,3)", true)

                // A newer transaction with a lower Sequence must not replace the business-newer row.
                sql "INSERT INTO cloud_memtable_mor_seq VALUES (0,-1,1)"
                quickTest("seq_lower_${sinkUpload}", seqQuery, true)
                sql "INSERT INTO cloud_memtable_mor VALUES (0,99)"
                quickTest("mor_new_version_${sinkUpload}", morQuery, true)
                sql """
                    INSERT INTO cloud_memtable_agg
                    VALUES (0,10,-1,13000,bitmap_empty(),hll_empty(),99,NULL)
                """
                quickTest("agg_new_version_${sinkUpload}", aggQuery, true)
                sql "INSERT INTO cloud_memtable_mor_seq SELECT n % 4,n*10,n FROM cloud_memtable_models_source WHERE n<0"
                quickTest("seq_empty_${sinkUpload}", seqQuery, true)
                ['agg', 'mor', 'mor_seq'].each { model ->
                    trigger_and_wait_compaction("cloud_memtable_${model}", "full")
                }
                quickTest("agg_compacted_${sinkUpload}", aggQuery, true)
                quickTest("mor_compacted_${sinkUpload}", morQuery, true)
                quickTest("seq_compacted_${sinkUpload}", seqQuery, true)

                sql """
                    CREATE TABLE cloud_memtable_agg_broker (k BIGINT NOT NULL, v BIGINT SUM)
                    AGGREGATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
                """
                sql """
                    CREATE TABLE cloud_memtable_mor_broker (k BIGINT NOT NULL, v BIGINT)
                    UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="false",
                                "disable_auto_compaction"="true")
                """
                loadModelS3("cloud_memtable_agg_broker", sinkUpload)
                loadModelS3("cloud_memtable_mor_broker", sinkUpload)
                quickTest("agg_broker_${sinkUpload}", "SELECT k,v FROM cloud_memtable_agg_broker", true)
                quickTest("mor_broker_${sinkUpload}", "SELECT k,v FROM cloud_memtable_mor_broker", true)
            }

            streamLoad {
                table "cloud_memtable_agg_broker"
                set "column_separator", ","
                set "memtable_on_sink_node", "true"
                inputStream new ByteArrayInputStream("50,1\n50,2\n".getBytes())
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    assertEquals("Success", parseJson(result).Status)
                }
            }
            order_qt_agg_stream "SELECT k,v FROM cloud_memtable_agg_broker WHERE k=50"
            streamLoad {
                table "cloud_memtable_mor_seq"
                set "column_separator", ","
                set "memtable_on_sink_node", "true"
                inputStream new ByteArrayInputStream("0,1,1\n1,130010,13001\n".getBytes())
                check { result, exception, startTime, endTime ->
                    if (exception != null) { throw exception }
                    assertEquals("Success", parseJson(result).Status)
                }
            }
            order_qt_mor_stream seqQuery
            sql "INSERT INTO cloud_memtable_mor_seq (k,v,seq,__DORIS_DELETE_SIGN__) VALUES (2,140000,14000,1)"
            order_qt_mor_delete seqQuery
            sql "INSERT INTO cloud_memtable_mor_seq VALUES (2,150000,15000)"
            order_qt_mor_reinsert seqQuery
            trigger_and_wait_compaction("cloud_memtable_mor_seq", "full")
            order_qt_mor_final_compacted seqQuery
        }

        def testVariant = {
            sql "DROP TABLE IF EXISTS cloud_memtable_variant_reference"
            sql "DROP TABLE IF EXISTS cloud_memtable_variant"
            sql """
                CREATE TABLE cloud_memtable_variant_reference (k BIGINT NOT NULL, v VARIANT)
                DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES ("replication_num"="1", "disable_auto_compaction"="true")
            """
            sql "CREATE TABLE cloud_memtable_variant LIKE cloud_memtable_variant_reference"
            def input = """
                SELECT n, parse_to_variant(IF(n % 2 = 0,
                    CONCAT('{"a":', n, ',"nested":{"b":', n * 2, '}}'),
                    CONCAT('{"a":', n, ',"extra":"', n, '"}')))
                FROM cloud_memtable_models_source WHERE n < 1200
            """
            def newPath = """SELECT 1200, parse_to_variant('{"a":1200,"later":2400}')"""
            def projection = """
                SELECT k, CAST(v['a'] AS BIGINT), CAST(v['nested']['b'] AS BIGINT),
                    CAST(v['extra'] AS STRING), CAST(v['later'] AS BIGINT)
            """
            sql "SET enable_memtable_on_sink_node=false"
            sql "INSERT INTO cloud_memtable_variant_reference ${input}"
            sql "INSERT INTO cloud_memtable_variant_reference ${newPath}"
            def expected = sql "${projection} FROM cloud_memtable_variant_reference ORDER BY k"
            sql "SET enable_memtable_on_sink_node=true"
            sql "SET enable_cloud_memtable_sink_upload=true"
            sql "SET enable_profile=true"
            GetDebugPoint().enableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            try {
                sql """
                    /* cloud_memtable_variant_profile */
                    INSERT INTO cloud_memtable_variant ${input}
                """
                new ProfileAction(context).getProfileBySql("cloud_memtable_variant_profile",
                        ["DeltaWriterV2", "CloudMemtableSinkUpload: true"])
                sql "SET enable_profile=false"
                def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_memtable_variant")[0]
                def partition = sql_return_maparray("SHOW PARTITIONS FROM cloud_memtable_variant")[0]
                def ms = cluster.getAllMetaservices()[0]
                getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                    code, body ->
                        assertEquals(200, code)
                        def ids = parseJson(body).segment_ids
                        assertTrue(ids.any { (it as long) >= 1000 },
                                "VARIANT schema assembly must include multiple sink writers: ${ids}")
                }
                // A later rowset introduces another extracted path before compaction.
                sql "INSERT INTO cloud_memtable_variant ${newPath}"
                assertEquals(expected, sql("${projection} FROM cloud_memtable_variant ORDER BY k"))
                trigger_and_wait_compaction("cloud_memtable_variant", "full")
                assertEquals(expected, sql("${projection} FROM cloud_memtable_variant ORDER BY k"))
            } finally {
                sql "SET enable_profile=false"
                GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            }
        }

        def originalBatchSize = sql("SHOW VARIABLES LIKE 'broker_load_batch_size'")[0][1]
        try {
            testDuplicate()
        } finally {
            sql "SET broker_load_batch_size=${originalBatchSize}"
            sql "SET enable_profile=false"
            GetDebugPoint().disableDebugPointForAllBEs("MemTable.need_flush")
            GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
            GetDebugPoint().disableDebugPointForAllBEs("DeltaWriterV2.sink_upload.duplicate_result")
        }
        try {
            testAggregateAndMor()
        } finally {
            sql "SET enable_profile=false"
            GetDebugPoint().disableDebugPointForAllBEs("LoadStreamWriter.append_data.unexpected_transfer")
        }
        testVariant()
    }
}
