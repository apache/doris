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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

suite("test_file_type_statistics") {
    sql "DROP TABLE IF EXISTS test_file_type_statistics"
    sql """
        CREATE TABLE test_file_type_statistics (
            id INT NOT NULL, f FILE,
            file_size BIGINT GENERATED ALWAYS AS (ELEMENT_AT(f, 'size')))
        DUPLICATE KEY(id)
        PARTITION BY RANGE(id) (
            PARTITION p1 VALUES LESS THAN (4), PARTITION p2 VALUES LESS THAN (10))
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")
    """
    sql """ALTER TABLE test_file_type_statistics SET ("auto_analyze_policy"="disable")"""
    long tableId = getTableId("test_file_type_statistics")
    def checkStatistics = { String tag ->
        "qt_${tag}" """
            SELECT col_id, count, ndv, null_count, min, max, data_size_in_bytes, hot_value IS NULL
            FROM __internal_schema.column_statistics
            WHERE tbl_id = '${tableId}' AND col_id IN ('f', 'file_size')
            ORDER BY col_id
        """
        def persisted = "SHOW COLUMN STATS test_file_type_statistics(f)"
        def cached = "SHOW COLUMN CACHED STATS test_file_type_statistics(f)"
        // Keep only column name, row/NULL counts, bytes, average bytes and NDV/MIN/MAX.
        // Timestamps, query counters and analyze-job metadata are not deterministic.
        def stableColumns = { row -> [row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8]] }
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until {
            def storedRows = sql(persisted)
            def cachedRows = sql(cached)
            storedRows.size() == 1 && cachedRows.size() == 1 &&
                    stableColumns(storedRows[0]) == stableColumns(cachedRows[0])
        }
        quickRunTest("${tag}_show", persisted, true, stableColumns)
        quickRunTest("${tag}_cached", cached, true, stableColumns)
    }
    def originalPartitionAnalyze = sql("SHOW GLOBAL VARIABLES LIKE 'enable_partition_analyze'")[0][1]
    try {
        sql "SET GLOBAL enable_partition_analyze = false"
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SYNC"
        checkStatistics("empty")
        streamLoad {
            table "test_file_type_statistics"
            set "format", "json"
            set "read_json_by_line", "true"
            set "columns", "id,f"
            set "strict_mode", "true"
            set "max_filter_ratio", "0"
            file "file_type_statistics.json"
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def status = parseJson(result)
                assertEquals("success", status.Status.toLowerCase(), result)
                assertEquals(5L, status.NumberLoadedRows as long, result)
                assertEquals(0L, status.NumberFilteredRows as long, result)
            }
        }
        sql "SYNC"
        qt_values """
            SELECT id, f IS NULL, ELEMENT_AT(f, 'uri'), ELEMENT_AT(f, 'offset'), file_size
            FROM test_file_type_statistics ORDER BY id
        """
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SYNC"
        checkStatistics("full_default")
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SYNC WITH HOT VALUE"
        checkStatistics("full")
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SAMPLE PERCENT 100 WITH SYNC"
        checkStatistics("sample_full_fallback")

        sql "DROP TABLE IF EXISTS test_file_type_statistics_sample"
        sql """
            CREATE TABLE test_file_type_statistics_sample (id INT NOT NULL, f FILE, null_file FILE)
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1")
        """
        sql """ALTER TABLE test_file_type_statistics_sample SET ("auto_analyze_policy"="disable")"""
        sql """
            INSERT INTO test_file_type_statistics_sample
            SELECT n.number, t.f, CAST(NULL AS FILE)
            FROM numbers("number"="4") n CROSS JOIN test_file_type_statistics t
            WHERE t.id = 1
        """
        sql "SYNC"
        waitRowCountReady(context.dbName, "test_file_type_statistics_sample", 4L)
        long sampleTableId = getTableId("test_file_type_statistics_sample")
        // One tablet has four reported rows, so SAMPLE ROWS 1 cannot take either FULL
        // fallback. Both FILE columns are non-key/non-partition columns: TABLET(...)
        // reads with LIMIT 1 and scaleFactor=4. Every possible sampled row is identical:
        // f has 49 payload bytes (including three inline bytes), null_file is NULL.
        sql "ANALYZE TABLE test_file_type_statistics_sample(f, null_file) WITH SAMPLE ROWS 1 WITH SYNC"
        qt_sample_scaled """
            WITH sampled AS (
                SELECT __file_data_size(f) AS f_bytes, __file_data_size(null_file) AS null_bytes
                FROM test_file_type_statistics_sample LIMIT 1
            ), payload AS (
                SELECT COUNT(*) AS rows_read,
                       COUNT(*) - COUNT(f_bytes) AS f_nulls,
                       COUNT(*) - COUNT(null_bytes) AS null_nulls,
                       COALESCE(SUM(f_bytes), 0) AS f_bytes,
                       COALESCE(SUM(null_bytes), 0) AS null_bytes
                FROM sampled
            )
            SELECT s.col_id, s.count, s.ndv, s.null_count, s.min, s.max,
                   s.data_size_in_bytes, s.hot_value IS NULL,
                   s.count = 4 * p.rows_read AS row_count_scaled,
                   s.null_count = 4 * IF(s.col_id = 'f', p.f_nulls, p.null_nulls) AS null_count_scaled,
                   s.data_size_in_bytes = 4 * IF(s.col_id = 'f', p.f_bytes, p.null_bytes) AS payload_scaled
            FROM __internal_schema.column_statistics s CROSS JOIN payload p
            WHERE s.tbl_id = '${sampleTableId}' AND s.col_id IN ('f', 'null_file')
            ORDER BY s.col_id
        """
        sql "SET GLOBAL enable_partition_analyze = true"
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SYNC"
        checkStatistics("partition_merged")
        qt_partitions """
            SELECT part_name, col_id, count, null_count, min, max, data_size_in_bytes
            FROM __internal_schema.partition_statistics
            WHERE tbl_id = '${tableId}' AND col_id = 'f' ORDER BY part_name
        """
        def partitionCached = "SHOW COLUMN CACHED STATS test_file_type_statistics(f) PARTITION(*)"
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until {
            sql(partitionCached).size() == 2
        }
        def partitionColumns = { row -> [row[0], row[1], row[3], row[4], row[5], row[6], row[7], row[8]] }
        quickRunTest("partitions_show", "SHOW COLUMN STATS test_file_type_statistics(f) PARTITION(*)",
                true, partitionColumns)
        quickRunTest("partitions_cached", partitionCached, true, partitionColumns)
        sql "SET GLOBAL enable_partition_analyze = false"
        sql "INSERT OVERWRITE TABLE test_file_type_statistics(id, f) SELECT id, CAST(NULL AS FILE) FROM test_file_type_statistics"
        sql "ANALYZE TABLE test_file_type_statistics(f, file_size) WITH SYNC"
        checkStatistics("all_null")
        sql """
            ALTER TABLE test_file_type_statistics MODIFY COLUMN f
            SET STATS ('row_count'='5', 'num_nulls'='5', 'data_size'='0')
        """
        qt_manual """
            SELECT count, ndv, null_count, min, max, data_size_in_bytes, hot_value IS NULL
            FROM __internal_schema.column_statistics
            WHERE tbl_id = '${tableId}' AND col_id = 'f'
        """
        test {
            sql """
                ALTER TABLE test_file_type_statistics MODIFY COLUMN f
                SET STATS ('row_count'='5', 'ndv'='1')
            """
            exception "FILE NDV"
        }
    } finally {
        sql "SET GLOBAL enable_partition_analyze = ${originalPartitionAnalyze}"
    }
}
