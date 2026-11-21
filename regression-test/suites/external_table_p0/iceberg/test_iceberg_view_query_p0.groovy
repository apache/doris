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

suite("test_iceberg_view_query_p0", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        logger.info("disable iceberg test.")
        return
    }

    def config = context.config.otherConfigs
    String externalEnvIp = config.get("externalEnvIp")
    String restPort = config.get("iceberg_rest_uri_port")
    String minioPort = config.get("iceberg_minio_port")
    // These defaults match the public Iceberg Docker fixture; local credentials stay in configuration.
    String accessKey = config.get("iceberg_view_access_key") ?: "admin"
    String secretKey = config.get("iceberg_view_secret_key") ?: "password"

    sql """DROP CATALOG IF EXISTS test_iceberg_view_query_p0"""
    sql """CREATE CATALOG test_iceberg_view_query_p0 PROPERTIES (
        'type' = 'iceberg',
        'iceberg.catalog.type' = 'rest',
        'uri' = 'http://${externalEnvIp}:${restPort}',
        's3.access_key' = '${accessKey}',
        's3.secret_key' = '${secretKey}',
        's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
        's3.region' = 'us-east-1'
    )"""

    // run14.sql in the Iceberg Docker fixture creates these persistent Spark views.
    sql """USE test_iceberg_view_query_p0.test_db"""

    qt_partitioned """SELECT * FROM v_with_partitioned_table ORDER BY col1"""
    qt_unpartitioned """SELECT * FROM v_with_unpartitioned_table ORDER BY col1"""
    qt_partition_column """SELECT * FROM v_with_partitioned_column ORDER BY col5"""
    qt_join """SELECT * FROM v_with_joint_table ORDER BY sale_date, product_name"""

    qt_count_partitioned """SELECT COUNT(*) FROM v_with_partitioned_table"""
    qt_count_unpartitioned """SELECT COUNT(*) FROM v_with_unpartitioned_table"""
    qt_count_partition_column """SELECT COUNT(*) FROM v_with_partitioned_column"""
    qt_count_join """SELECT COUNT(*) FROM v_with_joint_table"""

    qt_project_columns """SELECT col1, col2, col3, col4 FROM v_with_partitioned_table ORDER BY col1"""
    qt_project_partition """SELECT col5 FROM v_with_partitioned_table ORDER BY col5"""
    qt_filter_partitioned """SELECT col1, col5 FROM v_with_partitioned_table
        WHERE col5 IN ('Chicago', 'New York') AND col3 >= 25 ORDER BY col1"""
    qt_filter_unpartitioned """SELECT col1, col3 FROM v_with_unpartitioned_table
        WHERE col3 BETWEEN 30 AND 50 ORDER BY col1"""

    qt_describe_partitioned """DESC v_with_partitioned_table"""
    qt_describe_unpartitioned """DESC v_with_unpartitioned_table"""
    qt_describe_partition_column """DESC v_with_partitioned_column"""
    qt_describe_join """DESC v_with_joint_table"""

    explain {
        sql """SELECT * FROM v_with_partitioned_table"""
        contains "t_partitioned_table"
    }
    explain {
        sql """SELECT * FROM v_with_unpartitioned_table"""
        contains "t_unpartitioned_table"
    }
    explain {
        sql """SELECT * FROM v_with_partitioned_column"""
        contains "t_partitioned_table"
    }
    explain {
        sql """SELECT COUNT(*) FROM v_with_partitioned_table"""
        contains "t_partitioned_table"
    }
    explain {
        sql """SELECT COUNT(*) FROM v_with_unpartitioned_table"""
        contains "t_unpartitioned_table"
    }
    explain {
        sql """SELECT COUNT(*) FROM v_with_partitioned_column"""
        contains "t_partitioned_table"
    }
    explain {
        sql """SELECT col1, col2, col3, col4 FROM v_with_partitioned_table"""
        contains "t_partitioned_table"
    }
    explain {
        sql """SELECT col5 FROM v_with_partitioned_table"""
        contains "t_partitioned_table"
    }

    // Spark SQL clients may preserve different whitespace in the saved view text.
    def normalizeViewDdl = { row -> [row[0], row[1].replaceAll("\\s+", " ").trim()] }
    quickRunTest("create_table_partitioned", "SHOW CREATE TABLE v_with_partitioned_table", false, normalizeViewDdl)
    quickRunTest("create_table_unpartitioned", "SHOW CREATE TABLE v_with_unpartitioned_table", false, normalizeViewDdl)
    quickRunTest("create_table_partition_column", "SHOW CREATE TABLE v_with_partitioned_column", false, normalizeViewDdl)
    // SHOW CREATE VIEW currently accepts internal views; external views use SHOW CREATE TABLE.
    for (String view : ["v_with_partitioned_table", "v_with_unpartitioned_table", "v_with_partitioned_column"]) {
        test {
            sql """SHOW CREATE VIEW ${view}"""
            exception "is not VIEW. Use 'SHOW CREATE TABLE"
        }
    }

    for (String view : ["v_with_partitioned_table", "v_with_unpartitioned_table",
                        "v_with_partitioned_column", "v_with_joint_table"]) {
        for (String selector : ["FOR TIME AS OF '2025-06-11 20:17:01'",
                                "FOR VERSION AS OF 5497706844625725452"]) {
            test {
                sql """SELECT * FROM ${view} ${selector} ORDER BY 1 LIMIT 10"""
                exception "view not supported with snapshot time/version travel"
            }
            test {
                sql """SELECT COUNT(*) FROM ${view} ${selector}"""
                exception "view not supported with snapshot time/version travel"
            }
        }
    }
}
