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

suite("test_row_binlog_multi_segment", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_mow_multi_segment_with_binlog FORCE"

    sql """
        CREATE TABLE test_mow_multi_segment_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """ALTER TABLE test_mow_multi_segment_with_binlog
             ENABLE FEATURE "SEQUENCE_LOAD"
             WITH PROPERTIES ("function_column.sequence_type" = "int")"""

    def customBeConfig = [
        doris_scanner_row_bytes: 1
    ]

    setBeConfigTemporary(customBeConfig) {
        try {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
            // csv reader uses max(state->batch_size(), 4064) as the batch row limit.
            // shrink doris_scanner_row_bytes and enable MemTable.need_flush to make multi segments stable.
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            String overlappingLoadContent = ""
            String sequenceLoadContent = ""
            String partialUpdateLoadContent = ""

            // first load: write base keys [1, 4500], then append keys [2048, 6081] in the same load.
            // Keep the first range larger than one csv batch so sampled overlap keys are not
            // sensitive to the exact segment boundary. Do not include k1=5000 in the first range,
            // otherwise skip_delete_bitmap=true would expose one more sampled physical row.
            (1..4500).each {
                overlappingLoadContent += "${it},${it},${it},${it},${it},1\n"
            }
            (2048..6081).each {
                overlappingLoadContent += "${it},${it},${it},${it + 100000},${it + 100000},2\n"
            }

            streamLoad {
                table "test_mow_multi_segment_with_binlog"
                set 'column_separator', ','
                set 'columns', 'k1,k2,k3,v1,v2,seq'
                set 'function_column.sequence_col', 'seq'
                inputStream new ByteArrayInputStream(overlappingLoadContent.getBytes())
                time 60000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(8534, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            // inspect sampled-key row binlog after the first overlapping load.
            qt_mow_multi_segment_stage1_binlog_sample """
                SELECT __DORIS_BINLOG_OP__ AS op,
                       k1,
                       k2,
                       k3,
                       v1,
                       v2,
                       __BEFORE__v1__,
                       __BEFORE__v2__
                FROM binlog("table" = "test_mow_multi_segment_with_binlog")
                WHERE k1 IN (1, 3000, 5000)
                ORDER BY __DORIS_BINLOG_LSN__
            """

            // second load: first write keys [2048, 7000] with seq=3.
            // then write overlap keys [2800, 3400] again with lower seq=2, which should be ignored.
            // finally write keys [5000, 5600] again with higher seq=4, which should overwrite seq=3.
            (2048..7000).each {
                sequenceLoadContent += "${it},${it},${it},${it + 200000},${it + 200000},3\n"
            }
            (2800..3400).each {
                sequenceLoadContent += "${it},${it},${it},${it + 300000},${it + 300000},2\n"
            }
            (5000..5600).each {
                sequenceLoadContent += "${it},${it},${it},${it + 400000},${it + 400000},4\n"
            }

            streamLoad {
                table "test_mow_multi_segment_with_binlog"
                set 'column_separator', ','
                set 'columns', 'k1,k2,k3,v1,v2,seq'
                set 'function_column.sequence_col', 'seq'
                inputStream new ByteArrayInputStream(sequenceLoadContent.getBytes())
                time 60000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(6155, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            // inspect sampled-key row binlog after sequence comparison.
            qt_mow_multi_segment_stage2_binlog_sample """
                SELECT __DORIS_BINLOG_OP__ AS op,
                       k1,
                       k2,
                       k3,
                       v1,
                       v2,
                       __BEFORE__v1__,
                       __BEFORE__v2__
                FROM binlog("table" = "test_mow_multi_segment_with_binlog")
                WHERE k1 IN (1, 3000, 5000)
                ORDER BY __DORIS_BINLOG_LSN__
            """

            // third load: partial update spans multiple segments and has overlap inside the same load.
            def partialUpdateSeq = { int key ->
                if (key <= 2047) {
                    return 1
                }
                if (key >= 5000 && key <= 5600) {
                    return 4
                }
                return 3
            }
            (2000..7000).each {
                partialUpdateLoadContent += "${it},${it},${it},${it + 500000},${partialUpdateSeq(it)}\n"
            }
            (3000..3600).each {
                partialUpdateLoadContent += "${it},${it},${it},${it + 600000},3\n"
            }
            (5000..5600).each {
                partialUpdateLoadContent += "${it},${it},${it},${it + 700000},4\n"
            }

            sql "SET enable_unique_key_partial_update = true"
            sql "SET require_sequence_in_insert = false"
            streamLoad {
                table "test_mow_multi_segment_with_binlog"
                set 'column_separator', ','
                set 'partial_columns', 'true'
                set 'columns', 'k1,k2,k3,v2,seq'
                set 'function_column.sequence_col', 'seq'
                inputStream new ByteArrayInputStream(partialUpdateLoadContent.getBytes())
                time 60000

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(6203, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            // inspect sampled-key final event chain ordered by raw lsn.
            qt_mow_multi_segment_stage3_binlog_sample """
                SELECT __DORIS_BINLOG_OP__ AS op,
                       k1,
                       k2,
                       k3,
                       v1,
                       v2,
                       __BEFORE__v1__,
                       __BEFORE__v2__
                FROM binlog("table" = "test_mow_multi_segment_with_binlog")
                WHERE k1 IN (1, 3000, 5000)
                ORDER BY __DORIS_BINLOG_LSN__
            """

            sql "SET skip_delete_bitmap = true"

            // inspect sampled-key final event chain again with skip_delete_bitmap=true.
            qt_mow_multi_segment_stage3_binlog_sample_skip_bitmap """
                SELECT __DORIS_BINLOG_OP__ AS op,
                       k1,
                       k2,
                       k3,
                       v1,
                       v2,
                       __BEFORE__v1__,
                       __BEFORE__v2__
                FROM binlog("table" = "test_mow_multi_segment_with_binlog")
                WHERE k1 IN (1, 3000, 5000)
                ORDER BY __DORIS_BINLOG_LSN__
            """
        } finally {
            sql "SET enable_unique_key_partial_update = false"
            sql "SET require_sequence_in_insert = true"
            sql "SET skip_delete_bitmap = false"
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
