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

import org.junit.Assert

suite("test_flexible_partial_update_variant_publish_conflict") {
    if (isCloudMode()) {
        logger.info("skip test_flexible_partial_update_variant_publish_conflict in cloud mode")
        return
    }

    sql "set default_variant_enable_doc_mode = false"

    def dbName = context.config.getDbNameByFile(context.file)

    def do_streamload_2pc_commit = { tableName, txnId ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${dbName}/${tableName}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        def code = process.waitFor()
        def out = process.text
        def json2pc = parseJson(out)
        log.info("http_stream 2pc result: ${out}".toString())
        assertEquals(0, code)
        assertEquals("success", json2pc.status.toLowerCase())
    }

    def wait_for_publish = { txnId, waitSecond ->
        String st = "PREPARE"
        while (!st.equalsIgnoreCase("VISIBLE") && !st.equalsIgnoreCase("ABORTED") && waitSecond > 0) {
            Thread.sleep(1000)
            waitSecond -= 1
            def result = sql_return_maparray "show transaction from ${dbName} where id = ${txnId}"
            assertNotNull(result)
            st = result[0].TransactionStatus
        }
        log.info("Stream load with txn ${txnId} is ${st}")
        assertEquals("VISIBLE", st)
    }

    def prepare_streamload = { tableName, loadBody ->
        def txnId = null
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'two_phase_commit', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(loadBody.getBytes("UTF-8"))
            time 40000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }

                def json = parseJson(result)
                txnId = json.TxnId
                assertEquals("success", json.Status.toLowerCase())
            }
        }
        return txnId
    }

    def streamload = { tableName, loadBody ->
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream new ByteArrayInputStream(loadBody.getBytes("UTF-8"))
            time 20000
        }
    }

    for (def use_row_store : [false, true]) {
        def tableName = "t_fpu_var_pub_conf_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
        sql """
            CREATE TABLE ${tableName} (
                `k` int NOT NULL,
                `c` int NULL,
                `v` variant NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "store_row_column" = "${use_row_store}");
        """

        sql """ INSERT INTO ${tableName} VALUES (1, 1, '{"a":1,"b":1}') """

        def txnA = prepare_streamload(tableName, """{"k":1,"v":{"a":2}}\n""")
        def txnB = prepare_streamload(tableName, """{"k":1,"c":9,"v":{"b":3}}\n""")
        do_streamload_2pc_commit(tableName, txnB)
        wait_for_publish(txnB, 60)
        do_streamload_2pc_commit(tableName, txnA)
        wait_for_publish(txnA, 60)

        def disjointPathRows = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), c
            FROM ${tableName} ORDER BY k
        """
        assertEquals("[[1, 2, 3, 9]]", disjointPathRows.toString())

        def txnC = prepare_streamload(tableName, """{"k":1,"v":{"a":4}}\n""")
        def txnD = prepare_streamload(tableName, """{"k":1,"v":{"a":5}}\n""")
        do_streamload_2pc_commit(tableName, txnD)
        wait_for_publish(txnD, 60)
        do_streamload_2pc_commit(tableName, txnC)
        wait_for_publish(txnC, 60)

        def samePathRows = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), c
            FROM ${tableName} ORDER BY k
        """
        assertEquals("[[1, 4, 3, 9]]", samePathRows.toString())

        sql """ INSERT INTO ${tableName} VALUES (3, 3, '{"nested":{"x":1},"b":1}') """
        def txnEmptyObject = prepare_streamload(tableName, """{"k":3,"v":{"nested":{}}}\n""")
        def txnOtherPath = prepare_streamload(tableName, """{"k":3,"v":{"b":3}}\n""")
        do_streamload_2pc_commit(tableName, txnOtherPath)
        wait_for_publish(txnOtherPath, 60)
        do_streamload_2pc_commit(tableName, txnEmptyObject)
        wait_for_publish(txnEmptyObject, 60)

        def emptyObjectRows = sql """
            SELECT k, v['nested']['x'] IS NULL, cast(v['b'] as int), c
            FROM ${tableName} WHERE k = 3 ORDER BY k
        """
        assertEquals("[[3, true, 3, 3]]", emptyObjectRows.toString())

        sql """ INSERT INTO ${tableName} VALUES (4, 4, '{"a":{"c":0},"x":1}') """
        def txnParentThenChild = prepare_streamload(tableName,
                """{"k":4,"v":{"a":{}}}
{"k":4,"v":{"a":{"b":1}}}
""")
        def txnConcurrentChild = prepare_streamload(tableName, """{"k":4,"v":{"a":{"c":9}}}\n""")
        do_streamload_2pc_commit(tableName, txnConcurrentChild)
        wait_for_publish(txnConcurrentChild, 60)
        do_streamload_2pc_commit(tableName, txnParentThenChild)
        wait_for_publish(txnParentThenChild, 60)

        def parentThenChildRows = sql """
            SELECT k, cast(v['a']['b'] as int), v['a']['c'] IS NULL, cast(v['x'] as int)
            FROM ${tableName} WHERE k = 4 ORDER BY k
        """
        assertEquals("[[4, 1, true, 1]]", parentThenChildRows.toString())

        def multiTableName = "t_fpu_var_pub_multi_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${multiTableName} FORCE """
        sql """
            CREATE TABLE ${multiTableName} (
                `k` int NOT NULL,
                `v1` variant NULL,
                `v2` variant NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "store_row_column" = "${use_row_store}");
        """

        sql """ INSERT INTO ${multiTableName} VALUES (1, '{"a":1,"b":1}', '{"a":1,"b":1}') """
        def txnMultiA = prepare_streamload(multiTableName,
                """{"k":1,"v1":{"a":2},"v2":{"b":8}}\n""")
        def txnMultiB = prepare_streamload(multiTableName, """{"k":1,"v1":{"b":9}}\n""")
        do_streamload_2pc_commit(multiTableName, txnMultiB)
        wait_for_publish(txnMultiB, 60)
        do_streamload_2pc_commit(multiTableName, txnMultiA)
        wait_for_publish(txnMultiA, 60)

        def multiVariantRows = sql """
            SELECT k, cast(v1['a'] as int), cast(v1['b'] as int),
                   cast(v2['a'] as int), cast(v2['b'] as int)
            FROM ${multiTableName} ORDER BY k
        """
        assertEquals("[[1, 2, 9, 1, 8]]", multiVariantRows.toString())

        sql """ INSERT INTO ${tableName} VALUES (2, 1, '{"a":1,"b":1}') """
        def txnPatchAfterDelete = prepare_streamload(tableName, """{"k":2,"v":{"a":2}}\n""")
        streamload(tableName, """{"k":2,"__DORIS_DELETE_SIGN__":1}\n""")
        do_streamload_2pc_commit(tableName, txnPatchAfterDelete)
        wait_for_publish(txnPatchAfterDelete, 60)

        def patchAfterDeleteRows = sql """
            SELECT k, cast(v['a'] as int), v['b'] IS NULL, c
            FROM ${tableName} WHERE k = 2 ORDER BY k
        """
        assertEquals("[[2, 2, true, null]]", patchAfterDeleteRows.toString())

        def sparseTableName = "t_fpu_var_pub_sparse_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${sparseTableName} FORCE """
        sql """
            CREATE TABLE ${sparseTableName} (
                `k` int NOT NULL,
                `v` variant<properties("variant_max_subcolumns_count" = "1")> NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "store_row_column" = "${use_row_store}");
        """

        sql """ INSERT INTO ${sparseTableName} VALUES (1, '{"a":1,"b":1,"c":1}') """
        def txnSparseA = prepare_streamload(sparseTableName, """{"k":1,"v":{"b":2}}\n""")
        def txnSparseB = prepare_streamload(sparseTableName, """{"k":1,"v":{"c":3}}\n""")
        do_streamload_2pc_commit(sparseTableName, txnSparseB)
        wait_for_publish(txnSparseB, 60)
        do_streamload_2pc_commit(sparseTableName, txnSparseA)
        wait_for_publish(txnSparseA, 60)

        def sparseRows = sql """
            SELECT k, cast(v['a'] as int), cast(v['b'] as int), cast(v['c'] as int)
            FROM ${sparseTableName} ORDER BY k
        """
        assertEquals("[[1, 1, 2, 3]]", sparseRows.toString())
    }

    for (def use_row_store : [false, true]) {
        def seqTableName = "t_fpu_var_pub_seq_${use_row_store}"
        sql """ DROP TABLE IF EXISTS ${seqTableName} FORCE """
        sql """
            CREATE TABLE ${seqTableName} (
                `k` int NOT NULL,
                `seq` int NULL,
                `v` variant NULL,
                `c` int NULL
            ) UNIQUE KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "function_column.sequence_col" = "seq",
                "store_row_column" = "${use_row_store}");
        """

        sql """ INSERT INTO ${seqTableName} VALUES (1, 10, '{"a":1,"b":1}', 1) """

        def txnLowSeq = prepare_streamload(seqTableName, """{"k":1,"seq":5,"v":{"a":2}}\n""")
        def txnHighSeq = prepare_streamload(seqTableName, """{"k":1,"seq":20,"v":{"b":3}}\n""")
        do_streamload_2pc_commit(seqTableName, txnHighSeq)
        wait_for_publish(txnHighSeq, 60)
        do_streamload_2pc_commit(seqTableName, txnLowSeq)
        wait_for_publish(txnLowSeq, 60)

        def lowSeqDiscardRows = sql """
            SELECT k, seq, cast(v['a'] as int), cast(v['b'] as int), c
            FROM ${seqTableName} ORDER BY k
        """
        assertEquals("[[1, 20, 1, 3, 1]]", lowSeqDiscardRows.toString())

        def txnMissingSeq = prepare_streamload(seqTableName, """{"k":1,"v":{"a":4}}\n""")
        do_streamload_2pc_commit(seqTableName, txnMissingSeq)
        wait_for_publish(txnMissingSeq, 60)

        def missingSeqRows = sql """
            SELECT k, seq, cast(v['a'] as int), cast(v['b'] as int), c
            FROM ${seqTableName} ORDER BY k
        """
        assertEquals("[[1, 20, 4, 3, 1]]", missingSeqRows.toString())

        def txnHigherSeq = prepare_streamload(seqTableName, """{"k":1,"seq":30,"v":{"a":5}}\n""")
        do_streamload_2pc_commit(seqTableName, txnHigherSeq)
        wait_for_publish(txnHigherSeq, 60)

        def highSeqRows = sql """
            SELECT k, seq, cast(v['a'] as int), cast(v['b'] as int), c
            FROM ${seqTableName} ORDER BY k
        """
        assertEquals("[[1, 30, 5, 3, 1]]", highSeqRows.toString())
    }
}
