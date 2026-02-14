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

suite("test_insert_into_local_tvf", "tvf,external,external_docker") {

    List<List<Object>> backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    def be_id = backends[0][0]
    def be_host = backends[0][1]
    def basePath = "/tmp/test_insert_into_local_tvf"

    // Clean and create basePath on the BE node
    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sshExec("root", be_host, "mkdir -p ${basePath}")
    sshExec("root", be_host, "chmod 777 ${basePath}")

    // ============ Source tables ============

    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_local_tvf_src (
            c_bool      BOOLEAN,
            c_tinyint   TINYINT,
            c_smallint  SMALLINT,
            c_int       INT,
            c_bigint    BIGINT,
            c_float     FLOAT,
            c_double    DOUBLE,
            c_decimal   DECIMAL(10,2),
            c_date      DATE,
            c_datetime  DATETIME,
            c_varchar   VARCHAR(100),
            c_string    STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_insert_into_local_tvf_src VALUES
            (true,  1,  100,  1000,  100000,  1.1,  2.2,  123.45, '2024-01-01', '2024-01-01 10:00:00', 'hello', 'world'),
            (false, 2,  200,  2000,  200000,  3.3,  4.4,  678.90, '2024-06-15', '2024-06-15 12:30:00', 'foo',   'bar'),
            (true,  3,  300,  3000,  300000,  5.5,  6.6,  999.99, '2024-12-31', '2024-12-31 23:59:59', 'test',  'data'),
            (NULL,  NULL, NULL, NULL, NULL,   NULL, NULL,  NULL,   NULL,         NULL,                  NULL,    NULL),
            (false, -1, -100, -1000, -100000, -1.1, -2.2, -123.45,'2020-02-29', '2020-02-29 00:00:00', '',      'special_chars');
    """

    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_complex_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_local_tvf_complex_src (
            c_int    INT,
            c_array  ARRAY<INT>,
            c_map    MAP<STRING, INT>,
            c_struct STRUCT<f1:INT, f2:STRING>
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_insert_into_local_tvf_complex_src VALUES
            (1, [1, 2, 3],  {'a': 1, 'b': 2}, {1, 'hello'}),
            (2, [4, 5],     {'x': 10},         {2, 'world'}),
            (3, [],         {},                 {3, ''}),
            (4, NULL,       NULL,               NULL);
    """

    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_join_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_local_tvf_join_src (
            c_int    INT,
            c_label  STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO test_insert_into_local_tvf_join_src VALUES (1000, 'label_a'), (2000, 'label_b'), (3000, 'label_c'); """

    // ============ 1. CSV basic types ============
    // file_path is a prefix; BE generates: {prefix}{query_id}_{idx}.{ext}
    // Read back using wildcard on the prefix

    sshExec("root", be_host, "rm -f ${basePath}/basic_csv_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_csv_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT * FROM test_insert_into_local_tvf_src ORDER BY c_int;
    """

    order_qt_csv_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_csv_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 2. Parquet basic types ============

    sshExec("root", be_host, "rm -f ${basePath}/basic_parquet_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_parquet_",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT * FROM test_insert_into_local_tvf_src ORDER BY c_int;
    """

    order_qt_parquet_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_parquet_*",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 3. ORC basic types ============

    sshExec("root", be_host, "rm -f ${basePath}/basic_orc_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/basic_orc_",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) SELECT * FROM test_insert_into_local_tvf_src ORDER BY c_int;
    """

    order_qt_orc_basic_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/basic_orc_*",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) ORDER BY c_int;
    """

    // ============ 4. Parquet complex types ============

    sshExec("root", be_host, "rm -f ${basePath}/complex_parquet_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/complex_parquet_",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT * FROM test_insert_into_local_tvf_complex_src ORDER BY c_int;
    """

    order_qt_parquet_complex_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/complex_parquet_*",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 5. ORC complex types ============

    sshExec("root", be_host, "rm -f ${basePath}/complex_orc_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/complex_orc_",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) SELECT * FROM test_insert_into_local_tvf_complex_src ORDER BY c_int;
    """

    order_qt_orc_complex_types """
        SELECT * FROM local(
            "file_path" = "${basePath}/complex_orc_*",
            "backend_id" = "${be_id}",
            "format" = "orc"
        ) ORDER BY c_int;
    """

    // ============ 6. CSV separator: comma ============

    sshExec("root", be_host, "rm -f ${basePath}/sep_comma_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_comma_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ","
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_comma """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_comma_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // ============ 7. CSV separator: tab ============

    sshExec("root", be_host, "rm -f ${basePath}/sep_tab_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_tab_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "\t"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_tab """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_tab_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "\t"
        ) ORDER BY c1;
    """

    // ============ 8. CSV separator: pipe ============

    sshExec("root", be_host, "rm -f ${basePath}/sep_pipe_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_pipe_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "|"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_pipe """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_pipe_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 9. CSV separator: multi-char ============

    sshExec("root", be_host, "rm -f ${basePath}/sep_multi_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/sep_multi_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ";;"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_sep_multi """
        SELECT * FROM local(
            "file_path" = "${basePath}/sep_multi_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "column_separator" = ";;"
        ) ORDER BY c1;
    """

    // ============ 10. CSV line delimiter: CRLF ============

    sshExec("root", be_host, "rm -f ${basePath}/line_crlf_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/line_crlf_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "line_delimiter" = "\r\n"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_line_crlf """
        SELECT * FROM local(
            "file_path" = "${basePath}/line_crlf_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "line_delimiter" = "\r\n"
        ) ORDER BY c1;
    """

    // ============ 11. CSV compress: gz ============

    sshExec("root", be_host, "rm -f ${basePath}/compress_gz_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_gz_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "gz"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_gz """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_gz_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "gz"
        ) ORDER BY c1;
    """

    // ============ 12. CSV compress: zstd ============

    sshExec("root", be_host, "rm -f ${basePath}/compress_zstd_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_zstd_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "zstd"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_zstd """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_zstd_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "zstd"
        ) ORDER BY c1;
    """

    // ============ 13. CSV compress: lz4 ============

    // TODO: lz4 read meet error: LZ4F_getFrameInfo error: ERROR_frameType_unknown
    sshExec("root", be_host, "rm -f ${basePath}/compress_lz4_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_lz4_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "lz4block"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_lz4 """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_lz4_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "lz4block"
        ) ORDER BY c1;
    """

    // ============ 14. CSV compress: snappy ============

    sshExec("root", be_host, "rm -f ${basePath}/compress_snappy_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/compress_snappy_",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compression_type" = "snappyblock"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_csv_compress_snappy """
        SELECT * FROM local(
            "file_path" = "${basePath}/compress_snappy_*",
            "backend_id" = "${be_id}",
            "format" = "csv",
            "compress_type" = "snappyblock"
        ) ORDER BY c1;
    """

    // ============ 15. Overwrite mode ============
    // local TVF does not support delete_existing_files=true, so use shell cleanup to simulate overwrite

    // First write: 5 rows
    sshExec("root", be_host, "rm -f ${basePath}/overwrite_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/overwrite_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src ORDER BY c_int;
    """

    order_qt_overwrite_first """
        SELECT * FROM local(
            "file_path" = "${basePath}/overwrite_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // Clean files via shell, then write 2 rows
    sshExec("root", be_host, "rm -f ${basePath}/overwrite_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/overwrite_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src WHERE c_int > 0 ORDER BY c_int LIMIT 2;
    """

    order_qt_overwrite_second """
        SELECT * FROM local(
            "file_path" = "${basePath}/overwrite_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 16. Append mode (default, delete_existing_files=false) ============

    sshExec("root", be_host, "rm -f ${basePath}/append_*")

    // First write
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/append_",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src WHERE c_int = 1000;
    """

    order_qt_append_first """
        SELECT * FROM local(
            "file_path" = "${basePath}/append_*",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // Second write (append — different query_id produces different file name)
    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/append_",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src WHERE c_int = 2000;
    """

    order_qt_append_second """
        SELECT * FROM local(
            "file_path" = "${basePath}/append_*",
            "backend_id" = "${be_id}",
            "format" = "parquet"
        ) ORDER BY c_int;
    """

    // ============ 17. Complex SELECT: constant expressions ============

    sshExec("root", be_host, "rm -f ${basePath}/const_expr_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/const_expr_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT 1, 'hello', 3.14, CAST('2024-01-01' AS DATE);
    """

    order_qt_const_expr """
        SELECT * FROM local(
            "file_path" = "${basePath}/const_expr_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 18. Complex SELECT: WHERE + GROUP BY ============

    sshExec("root", be_host, "rm -f ${basePath}/where_groupby_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/where_groupby_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_bool, COUNT(*), SUM(c_int) FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL GROUP BY c_bool ORDER BY c_bool;
    """

    order_qt_where_groupby """
        SELECT * FROM local(
            "file_path" = "${basePath}/where_groupby_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 19. Complex SELECT: JOIN ============

    sshExec("root", be_host, "rm -f ${basePath}/join_query_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/join_query_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT a.c_int, a.c_varchar, b.c_label
          FROM test_insert_into_local_tvf_src a INNER JOIN test_insert_into_local_tvf_join_src b ON a.c_int = b.c_int
          ORDER BY a.c_int;
    """

    order_qt_join_query """
        SELECT * FROM local(
            "file_path" = "${basePath}/join_query_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 20. Complex SELECT: subquery ============

    sshExec("root", be_host, "rm -f ${basePath}/subquery_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/subquery_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT * FROM (SELECT c_int, c_varchar, c_string FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int) sub;
    """

    order_qt_subquery """
        SELECT * FROM local(
            "file_path" = "${basePath}/subquery_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 21. Complex SELECT: type cast ============

    sshExec("root", be_host, "rm -f ${basePath}/type_cast_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/type_cast_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT CAST(c_int AS BIGINT), CAST(c_float AS DOUBLE), CAST(c_date AS STRING)
          FROM test_insert_into_local_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_type_cast """
        SELECT * FROM local(
            "file_path" = "${basePath}/type_cast_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 22. Complex SELECT: UNION ALL ============

    sshExec("root", be_host, "rm -f ${basePath}/union_query_*")

    sql """
        INSERT INTO local(
            "file_path" = "${basePath}/union_query_",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src WHERE c_int = 1000
          UNION ALL
          SELECT c_int, c_varchar FROM test_insert_into_local_tvf_src WHERE c_int = 2000;
    """

    order_qt_union_query """
        SELECT * FROM local(
            "file_path" = "${basePath}/union_query_*",
            "backend_id" = "${be_id}",
            "format" = "csv"
        ) ORDER BY c1;
    """

    // ============ 23. Error: missing file_path ============

    test {
        sql """
            INSERT INTO local(
                "backend_id" = "${be_id}",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "file_path"
    }

    // ============ 24. Error: missing format ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err_",
                "backend_id" = "${be_id}"
            ) SELECT 1;
        """
        exception "format"
    }

    // ============ 25. Error: missing backend_id for local ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err_",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "backend_id"
    }

    // ============ 26. Error: unsupported TVF name ============

    test {
        sql """
            INSERT INTO unknown_tvf(
                "file_path" = "/tmp/err_",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "INSERT INTO TVF only supports"
    }

    // ============ 27. Error: unsupported format ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err_",
                "backend_id" = "${be_id}",
                "format" = "json"
            ) SELECT 1;
        """
        exception "Unsupported"
    }

    // ============ 28. Error: wildcard in file_path ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/wildcard_*.csv",
                "backend_id" = "${be_id}",
                "format" = "csv"
            ) SELECT 1;
        """
        exception "wildcards"
    }

    // ============ 29. Error: delete_existing_files=true on local TVF ============

    test {
        sql """
            INSERT INTO local(
                "file_path" = "${basePath}/err_",
                "backend_id" = "${be_id}",
                "format" = "csv",
                "delete_existing_files" = "true"
            ) SELECT 1;
        """
        exception "delete_existing_files"
    }

    // ============ Cleanup ============

    sshExec("root", be_host, "rm -rf ${basePath}", false)
    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_src """
    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_complex_src """
    sql """ DROP TABLE IF EXISTS test_insert_into_local_tvf_join_src """
}
