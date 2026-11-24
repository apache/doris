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

suite("test_lance_s3_tvf", "p0,external") {
    // The Lance fixture is preinstalled in the MinIO container of the Iceberg
    // external environment, so this suite deliberately shares its switch.
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Lance S3 TVF test because the Iceberg MinIO environment is disabled.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String storage = """
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.region" = "us-east-1",
            "use_path_style" = "true"
    """
    def lanceTvf = { String dataset ->
        """s3("uri" = "s3://warehouse/lance/${dataset}", ${storage}, "format" = "lance")"""
    }
    String allTypes = lanceTvf("all_types.lance")

    sql """SET time_zone = '+08:00'"""

    // all_types deliberately contains both supported and unsupported Arrow
    // types. S3 TVF should expose the same schema as the Lance Catalog and
    // allow a query to project only supported columns.
    order_qt_desc """DESC FUNCTION ${allTypes}"""

    // FE pins one Lance version and creates one split per fragment. These
    // aggregates catch missing fragments and duplicate whole-dataset scans.
    qt_scan_cardinality """
        SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
        FROM ${allTypes}
    """

    qt_integer_boundaries """
        SELECT row_id, int8_col, uint8_col, int64_col, uint64_col
        FROM ${allTypes}
        WHERE row_id IN (2, 3, 10, 11, 12)
        ORDER BY row_id
    """

    qt_representative_types """
        SELECT
            bool_col,
            float16_col,
            decimal256_col,
            hex(binary_col),
            cast(time32_ms_col AS string),
            cast(time64_us_col AS string),
            timestamp_us_col,
            timestamp_us_utc_col,
            fixed_size_list_uint8_col,
            map_col
        FROM ${allTypes}
        WHERE row_id = 1
    """

    qt_additional_types """
        SELECT
            null_col IS NULL AS null_is_null,
            duration_s_col,
            duration_ms_col,
            duration_us_col,
            duration_ns_col,
            CAST(json_col AS STRING) AS json_col,
            bfloat16_vector_col
        FROM ${allTypes}
        WHERE row_id = 1
    """

    // The Lance connector plans the scan, at the version pinned during analysis, and keeps the
    // credentials out of EXPLAIN.
    explain {
        sql """SELECT row_id FROM ${allTypes}"""
        contains "CONNECTOR: lance"
        contains "lanceCatalogType=tvf"
        contains "lanceVersion=1"
        contains "lanceFragments=1"
        notContains "password"
    }

    // Predicates and COUNT(*) reach Lance as they do for a Lance catalog table.
    explain {
        sql """SELECT row_id FROM ${allTypes} WHERE int64_col > 0"""
        contains "lancePushdownPredicate="
        notContains "PREDICATES:"
    }
    order_qt_pushed_predicate """SELECT row_id, int64_col FROM ${allTypes} WHERE int64_col > 0"""
    sql """SET enable_count_push_down_for_external_table = true"""
    explain {
        sql """SELECT count(*) FROM ${allTypes}"""
        contains "pushdown agg=COUNT (12)"
    }

    // Three fragments with one deleted row each: one split per fragment, and the logical rows.
    String multiFragment = lanceTvf("multi_frag.lance")
    explain {
        sql """SELECT row_id FROM ${multiFragment}"""
        contains "lanceVersion=4"
        contains "lanceFragments=3"
        contains "inputSplitNum=3,"
    }
    qt_multi_fragment """
        SELECT count(*), count(DISTINCT row_id), min(row_id), max(row_id), sum(row_id)
        FROM ${multiFragment}
    """
    explain {
        sql """SELECT count(*) FROM ${multiFragment}"""
        contains "pushdown agg=COUNT (27)"
    }
    qt_multi_fragment_count """SELECT count(*) FROM ${multiFragment}"""

    // file() reads S3-compatible storage through s3().
    qt_file_tvf """
        SELECT count(*), sum(row_id)
        FROM file("uri" = "s3://warehouse/lance/all_types.lance", ${storage}, "format" = "lance")
    """

    // A column keeps the dataset's spelling and resolves in any case. Two columns whose names differ only in
    // case are refused, as a file's are: Doris could not tell them apart.
    String columnCase = lanceTvf("column_case.lance")
    order_qt_column_case_desc """DESC FUNCTION ${columnCase}"""
    order_qt_column_case """SELECT row_id, username FROM ${columnCase} WHERE USERNAME <> 'b'"""
    test {
        sql """SELECT * FROM ${lanceTvf("column_case_conflict.lance")}"""
        exception "Repeated lowercase column names: id"
    }

    // An internal table in the statement, as the target of a load or in a join, keeps the
    // predicate with Lance.
    sql """DROP TABLE IF EXISTS test_lance_s3_tvf_load"""
    sql """
        CREATE TABLE test_lance_s3_tvf_load (row_id BIGINT, int64_col BIGINT)
        DUPLICATE KEY(row_id) DISTRIBUTED BY HASH(row_id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_lance_s3_tvf_load SELECT row_id, int64_col FROM ${allTypes} WHERE row_id >= 10"""
    order_qt_load """SELECT row_id, int64_col FROM test_lance_s3_tvf_load"""
    order_qt_join """
        SELECT t.row_id, t.uint8_col
        FROM ${allTypes} t JOIN test_lance_s3_tvf_load l ON t.row_id = l.row_id
        WHERE t.row_id > 10
    """

    // Only s3(), and file() over S3-compatible storage, read Lance, and nothing writes it.
    test {
        sql """
            SELECT * FROM hdfs("uri" = "hdfs://127.0.0.1:8020/lance/all_types.lance",
                "fs.defaultFS" = "hdfs://127.0.0.1:8020", "format" = "lance")
        """
        exception "Format lance is read only by the s3() table function, or by file() over S3-compatible storage"
    }
    // file() hands Azure storage to s3() as well, which must not give it to Lance as an s3:// location.
    test {
        sql """
            SELECT * FROM file("uri" = "wasbs://warehouse@demo.blob.core.windows.net/lance/all_types.lance",
                "azure.endpoint" = "https://demo.blob.core.windows.net", "azure.account_name" = "demo",
                "azure.account_key" = "c2s=", "format" = "lance")
        """
        exception "Format lance is read only by the s3() table function, or by file() over S3-compatible storage"
    }
    test {
        sql """
            SELECT * FROM s3("uri" = "s3://warehouse/lance/all_types.lance", ${storage}, "format" = "lance",
                "path_partition_keys" = "row_id")
        """
        exception "'path_partition_keys' is not supported for format lance"
    }
    test {
        sql """
            INSERT INTO s3("file_path" = "s3://warehouse/lance_out/all_types_", ${storage}, "format" = "lance")
            SELECT row_id FROM ${allTypes}
        """
        exception "format:lance is not supported"
    }
    test {
        sql """
            SELECT row_id FROM ${allTypes}
            INTO OUTFILE "s3://warehouse/lance_out/all_types_" FORMAT AS lance PROPERTIES (${storage})
        """
        exception "format:lance is not supported"
    }
    test {
        sql """SELECT * FROM ${lanceTvf("missing.lance")}"""
        exception "Failed to load Lance metadata from the dataset at the uri"
    }
}
