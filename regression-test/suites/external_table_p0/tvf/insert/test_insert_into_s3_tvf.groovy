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

suite("test_insert_into_s3_tvf", "external,external_docker") {

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")

    if (ak == null || ak == "" || sk == null || sk == "") {
        logger.info("S3 not configured, skip")
        return
    }

    def s3BasePath = "${bucket}/regression/insert_tvf_test"

    // file_path is now a prefix; BE generates: {prefix}{query_id}_{idx}.{ext}
    def s3WriteProps = { String path, String format ->
        return """
            "file_path" = "s3://${s3BasePath}/${path}",
            "format" = "${format}",
            "s3.endpoint" = "${s3_endpoint}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.region" = "${region}"
        """
    }

    // Read uses wildcard to match generated file names
    def s3ReadProps = { String path, String format ->
        return """
            "uri" = "https://${bucket}.${s3_endpoint}/regression/insert_tvf_test/${path}",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "${format}",
            "region" = "${region}"
        """
    }

    // ============ Source tables ============

    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_s3_tvf_src (
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
        INSERT INTO test_insert_into_s3_tvf_src VALUES
            (true,  1,  100,  1000,  100000,  1.1,  2.2,  123.45, '2024-01-01', '2024-01-01 10:00:00', 'hello', 'world'),
            (false, 2,  200,  2000,  200000,  3.3,  4.4,  678.90, '2024-06-15', '2024-06-15 12:30:00', 'foo',   'bar'),
            (true,  3,  300,  3000,  300000,  5.5,  6.6,  999.99, '2024-12-31', '2024-12-31 23:59:59', 'test',  'data'),
            (NULL,  NULL, NULL, NULL, NULL,   NULL, NULL,  NULL,   NULL,         NULL,                  NULL,    NULL),
            (false, -1, -100, -1000, -100000, -1.1, -2.2, -123.45,'2020-02-29', '2020-02-29 00:00:00', '',      'special_chars');
    """

    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_complex_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_s3_tvf_complex_src (
            c_int    INT,
            c_array  ARRAY<INT>,
            c_map    MAP<STRING, INT>,
            c_struct STRUCT<f1:INT, f2:STRING>
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        INSERT INTO test_insert_into_s3_tvf_complex_src VALUES
            (1, [1, 2, 3],  {'a': 1, 'b': 2}, {1, 'hello'}),
            (2, [4, 5],     {'x': 10},         {2, 'world'}),
            (3, [],         {},                 {3, ''}),
            (4, NULL,       NULL,               NULL);
    """

    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_join_src """
    sql """
        CREATE TABLE IF NOT EXISTS test_insert_into_s3_tvf_join_src (
            c_int    INT,
            c_label  STRING
        ) DISTRIBUTED BY HASH(c_int) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO test_insert_into_s3_tvf_join_src VALUES (1000, 'label_a'), (2000, 'label_b'), (3000, 'label_c'); """

    // ============ 1. S3 CSV basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_csv/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT * FROM test_insert_into_s3_tvf_src ORDER BY c_int;
    """

    order_qt_s3_csv_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_csv/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 2. S3 Parquet basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_parquet/data_", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT * FROM test_insert_into_s3_tvf_src ORDER BY c_int;
    """

    order_qt_s3_parquet_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_parquet/*", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 3. S3 ORC basic types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("basic_orc/data_", "orc")},
            "delete_existing_files" = "true"
        ) SELECT * FROM test_insert_into_s3_tvf_src ORDER BY c_int;
    """

    order_qt_s3_orc_basic_types """
        SELECT * FROM s3(
            ${s3ReadProps("basic_orc/*", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 4. S3 Parquet complex types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("complex_parquet/data_", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT * FROM test_insert_into_s3_tvf_complex_src ORDER BY c_int;
    """

    order_qt_s3_parquet_complex_types """
        SELECT * FROM s3(
            ${s3ReadProps("complex_parquet/*", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 5. S3 ORC complex types ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("complex_orc/data_", "orc")},
            "delete_existing_files" = "true"
        ) SELECT * FROM test_insert_into_s3_tvf_complex_src ORDER BY c_int;
    """

    order_qt_s3_orc_complex_types """
        SELECT * FROM s3(
            ${s3ReadProps("complex_orc/*", "orc")}
        ) ORDER BY c_int;
    """

    // ============ 6. S3 CSV separator: comma ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_comma/data_", "csv")},
            "column_separator" = ",",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_comma """
        SELECT * FROM s3(
            ${s3ReadProps("sep_comma/*", "csv")},
            "column_separator" = ","
        ) ORDER BY c1;
    """

    // ============ 7. S3 CSV separator: tab ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_tab/data_", "csv")},
            "column_separator" = "\t",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_tab """
        SELECT * FROM s3(
            ${s3ReadProps("sep_tab/*", "csv")},
            "column_separator" = "\t"
        ) ORDER BY c1;
    """

    // ============ 8. S3 CSV separator: pipe ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_pipe/data_", "csv")},
            "column_separator" = "|",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_pipe """
        SELECT * FROM s3(
            ${s3ReadProps("sep_pipe/*", "csv")},
            "column_separator" = "|"
        ) ORDER BY c1;
    """

    // ============ 9. S3 CSV separator: multi-char ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("sep_multi/data_", "csv")},
            "column_separator" = ";;",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_sep_multi """
        SELECT * FROM s3(
            ${s3ReadProps("sep_multi/*", "csv")},
            "column_separator" = ";;"
        ) ORDER BY c1;
    """

    // ============ 10. S3 CSV line delimiter: CRLF ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("line_crlf/data_", "csv")},
            "line_delimiter" = "\r\n",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_line_crlf """
        SELECT * FROM s3(
            ${s3ReadProps("line_crlf/*", "csv")},
            "line_delimiter" = "\r\n"
        ) ORDER BY c1;
    """

    // ============ 11. S3 CSV compress: gz ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_gz/data_", "csv")},
            "compression_type" = "gz",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_gz """
        SELECT * FROM s3(
            ${s3ReadProps("compress_gz/*", "csv")},
            "compress_type" = "gz"
        ) ORDER BY c1;
    """

    // ============ 12. S3 CSV compress: zstd ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_zstd/data_", "csv")},
            "compression_type" = "zstd",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_zstd """
        SELECT * FROM s3(
            ${s3ReadProps("compress_zstd/*", "csv")},
            "compress_type" = "zstd"
        ) ORDER BY c1;
    """

    // ============ 13. S3 CSV compress: lz4 ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_lz4/data_", "csv")},
            "compression_type" = "lz4block",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_lz4 """
        SELECT * FROM s3(
            ${s3ReadProps("compress_lz4/*", "csv")},
            "compress_type" = "lz4block"
        ) ORDER BY c1;
    """

    // ============ 14. S3 CSV compress: snappy ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("compress_snappy/data_", "csv")},
            "compression_type" = "snappyblock",
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_csv_compress_snappy """
        SELECT * FROM s3(
            ${s3ReadProps("compress_snappy/*", "csv")},
            "compress_type" = "snappyblock"
        ) ORDER BY c1;
    """

    // ============ 15. S3 Overwrite mode ============
    // delete_existing_files=true is handled by FE for S3

    // First write: 5 rows
    sql """
        INSERT INTO s3(
            ${s3WriteProps("overwrite/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src ORDER BY c_int;
    """

    order_qt_s3_overwrite_first """
        SELECT * FROM s3(
            ${s3ReadProps("overwrite/*", "csv")}
        ) ORDER BY c1;
    """

    // Second write: 2 rows with overwrite (FE deletes the directory first)
    sql """
        INSERT INTO s3(
            ${s3WriteProps("overwrite/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src WHERE c_int > 0 ORDER BY c_int LIMIT 2;
    """

    order_qt_s3_overwrite_second """
        SELECT * FROM s3(
            ${s3ReadProps("overwrite/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 16. S3 Append mode ============

    // First write
    sql """
        INSERT INTO s3(
            ${s3WriteProps("append/data_", "parquet")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src WHERE c_int = 1000;
    """

    order_qt_s3_append_first """
        SELECT * FROM s3(
            ${s3ReadProps("append/*", "parquet")}
        ) ORDER BY c_int;
    """

    // Second write (append — different query_id produces different file name)
    sql """
        INSERT INTO s3(
            ${s3WriteProps("append/data_", "parquet")}
        ) SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src WHERE c_int = 2000;
    """

    order_qt_s3_append_second """
        SELECT * FROM s3(
            ${s3ReadProps("append/*", "parquet")}
        ) ORDER BY c_int;
    """

    // ============ 17. S3 Complex SELECT: constant expressions ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("const_expr/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT 1, 'hello', 3.14, CAST('2024-01-01' AS DATE);
    """

    order_qt_s3_const_expr """
        SELECT * FROM s3(
            ${s3ReadProps("const_expr/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 18. S3 Complex SELECT: WHERE + GROUP BY ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("where_groupby/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_bool, COUNT(*), SUM(c_int) FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL GROUP BY c_bool ORDER BY c_bool;
    """

    order_qt_s3_where_groupby """
        SELECT * FROM s3(
            ${s3ReadProps("where_groupby/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 19. S3 Complex SELECT: JOIN ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("join_query/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT a.c_int, a.c_varchar, b.c_label
          FROM test_insert_into_s3_tvf_src a INNER JOIN test_insert_into_s3_tvf_join_src b ON a.c_int = b.c_int
          ORDER BY a.c_int;
    """

    order_qt_s3_join_query """
        SELECT * FROM s3(
            ${s3ReadProps("join_query/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 20. S3 Complex SELECT: subquery ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("subquery/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT * FROM (SELECT c_int, c_varchar, c_string FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int) sub;
    """

    order_qt_s3_subquery """
        SELECT * FROM s3(
            ${s3ReadProps("subquery/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 21. S3 Complex SELECT: type cast ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("type_cast/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT CAST(c_int AS BIGINT), CAST(c_float AS DOUBLE), CAST(c_date AS STRING)
          FROM test_insert_into_s3_tvf_src WHERE c_int IS NOT NULL ORDER BY c_int;
    """

    order_qt_s3_type_cast """
        SELECT * FROM s3(
            ${s3ReadProps("type_cast/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 22. S3 Complex SELECT: UNION ALL ============

    sql """
        INSERT INTO s3(
            ${s3WriteProps("union_query/data_", "csv")},
            "delete_existing_files" = "true"
        ) SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src WHERE c_int = 1000
          UNION ALL
          SELECT c_int, c_varchar FROM test_insert_into_s3_tvf_src WHERE c_int = 2000;
    """

    order_qt_s3_union_query """
        SELECT * FROM s3(
            ${s3ReadProps("union_query/*", "csv")}
        ) ORDER BY c1;
    """

    // ============ 23. Error: missing file_path ============

    test {
        sql """
            INSERT INTO s3(
                "format" = "csv",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "file_path"
    }

    // ============ 24. Error: missing format ============

    test {
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BasePath}/err/data_",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "format"
    }

    // ============ 25. Error: unsupported format ============

    test {
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BasePath}/err/data_",
                "format" = "json",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "Unsupported"
    }

    // ============ 26. Error: wildcard in file_path ============

    test {
        sql """
            INSERT INTO s3(
                "file_path" = "s3://${s3BasePath}/wildcard/*.csv",
                "format" = "csv",
                "s3.endpoint" = "${s3_endpoint}",
                "s3.access_key" = "${ak}",
                "s3.secret_key" = "${sk}",
                "s3.region" = "${region}"
            ) SELECT 1;
        """
        exception "wildcards"
    }

    // ============ Cleanup ============

    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_src """
    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_complex_src """
    sql """ DROP TABLE IF EXISTS test_insert_into_s3_tvf_join_src """
}
