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

suite("test_iceberg_sys_table", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_systable_ctl"
    String db_name = "test_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""

    def assertQueryRowsMatchCount = { String countSql, String querySql, String label ->
        List<List<Object>> countResult = sql countSql
        assertEquals(1, countResult.size())
        long expectedRows = ((Number) countResult[0][0]).longValue()
        List<List<Object>> queryResult = sql querySql
        assertNotNull(queryResult, "${label} result should not be null")
        assertEquals(expectedRows, (long) queryResult.size(), "${label} row count should match count query")
    }

    def test_systable_entries = { table, systableType ->
        def systableName = "${table}\$${systableType}"
        order_qt_desc_entries """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_entries_count """select count(*) from ${systableName}"""
        order_qt_select_entries_where_count """select count(status) from ${systableName} where status="0";"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select status, sequence_number, file_sequence_number from ${systableName}""",
                systableName)
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName} where status="0";""",
                """select status, sequence_number, file_sequence_number from ${systableName} where status="0";""",
                "${systableName} filtered by status")
    }

    def test_systable_files = { table, systableType ->
        def systableName = "${table}\$${systableType}"
        order_qt_desc_files """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_files_count """select count(*) from ${systableName}"""
        order_qt_select_files_where_count """select count(content) from ${systableName} where content="0";"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select content, file_format, record_count, lower_bounds, upper_bounds from ${systableName}""",
                systableName)
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName} where content="0";""",
                """select content, file_format, record_count, lower_bounds, upper_bounds from ${systableName} where content="0";""",
                "${systableName} filtered by content")
    }

    def test_systable_history = { table ->
        def systableName = "${table}\$history"
        order_qt_desc_history """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_history_count """select count(*) from ${systableName}"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select * from ${systableName} order by snapshot_id""",
                systableName)
    }

    def test_systable_metadata_log_entries = { table ->
        def systableName = "${table}\$metadata_log_entries"
        order_qt_desc_metadata_log_entries """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_metadata_log_entries_count """select count(*) from ${systableName}"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select * from ${systableName} order by timestamp""",
                systableName)
    }

    def test_systable_snapshots = { table ->
        def systableName = "${table}\$snapshots"
        order_qt_desc_snapshots """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_snapshots_count """select count(*) from ${systableName}"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select operation from ${systableName}""",
                "${systableName} projected query")
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select * from ${systableName} order by committed_at""",
                systableName)
    }

    def test_systable_refs = { table ->
        def systableName = "${table}\$refs"
        order_qt_desc_refs """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_refs_count """select count(*) from ${systableName}"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select name, type from ${systableName}""",
                "${systableName} projected query")
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select * from ${systableName} order by snapshot_id""",
                systableName)
    }

    def test_systable_manifests = { table, systableType ->
        def systableName = "${table}\$${systableType}"
        order_qt_desc_manifests """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_manifests_count """select count(*) from ${systableName}"""

        if (systableType.equals("manifests")) {
            assertQueryRowsMatchCount(
                    """select count(*) from ${systableName}""",
                    """select * from ${systableName} order by path""",
                    systableName)
        } else {
            assertQueryRowsMatchCount(
                    """select count(*) from ${systableName}""",
                    """select * from ${systableName} order by path, reference_snapshot_id""",
                    systableName)
        }
    }

    def test_systable_partitions = { table ->
        def systableName = "${table}\$partitions"
        order_qt_desc_partitions """desc ${systableName}"""

        List<List<Object>> desc1 = sql """desc ${systableName}"""
        List<List<Object>> desc2 = sql """desc ${db_name}.${systableName}"""
        List<List<Object>> desc3 = sql """desc ${catalog_name}.${db_name}.${systableName}"""
        assertEquals(desc1.size(), desc2.size());
        assertEquals(desc1.size(), desc3.size());
        for (int i = 0; i < desc1.size(); i++) {
            for (int j = 0; j < desc1[i].size(); j++) {
                assertEquals(desc1[i][j], desc2[i][j]);
                assertEquals(desc1[i][j], desc3[i][j]);
            }
        }

        order_qt_select_partitions_count """select count(*) from ${systableName}"""
        assertQueryRowsMatchCount(
                """select count(*) from ${systableName}""",
                """select * from ${systableName};""",
                systableName)
    }

    def test_table_systables = { table ->
        test_systable_entries(table, "entries")
        test_systable_entries(table, "all_entries")
        test_systable_files(table, "files")
        test_systable_files(table, "data_files")
        test_systable_files(table, "delete_files")
        test_systable_files(table, "all_files")
        test_systable_files(table, "all_data_files")
        test_systable_files(table, "all_delete_files")
        test_systable_history(table)
        test_systable_metadata_log_entries(table)
        test_systable_snapshots(table)
        test_systable_refs(table)
        test_systable_manifests(table, "manifests")
        test_systable_manifests(table, "all_manifests")
        test_systable_partitions(table)
        // TODO: these table will be supportted in future
        // test_systable_position_deletes(table)

        test {
            sql """select * from ${table}\$position_deletes"""
            exception "SysTable position_deletes is not supported yet"
        }
    }

    test_table_systables("test_iceberg_systable_unpartitioned")
    test_table_systables("test_iceberg_systable_partitioned")

    sql """drop table if exists test_iceberg_systable_tbl1;"""
    sql """create table test_iceberg_systable_tbl1 (id int);"""
    sql """insert into test_iceberg_systable_tbl1 values(1);"""
    sql """insert into test_iceberg_systable_tbl1 values(2);"""
    sql """insert into test_iceberg_systable_tbl1 values(3);"""
    sql """insert into test_iceberg_systable_tbl1 values(4);"""
    sql """insert into test_iceberg_systable_tbl1 values(5);"""

    String user = "test_iceberg_systable_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """create database if not exists internal.regression_test"""
    sql """grant select_priv on internal.regression_test.* to ${user}""" 
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots
              """
              exception "denied"
        }
    }
    sql """grant select_priv on ${catalog_name}.${db_name}.test_iceberg_systable_tbl1 to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots"""
    }
    try_sql("DROP USER ${user}")

    sql """drop catalog if exists test_iceberg_varbinary_sys"""
    sql """
    CREATE CATALOG test_iceberg_varbinary_sys PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1",
        'enable.mapping.varbinary' = 'true'
    );"""

    sql """switch test_iceberg_varbinary_sys """
    sql """use ${db_name}"""

    order_qt_varbinary_sys_table_desc """desc test_iceberg_systable_unpartitioned\$files"""
    List<List<Object>> varbinaryRows = sql """
        select content, file_format, record_count, lower_bounds, upper_bounds
        from test_iceberg_systable_unpartitioned\$files;
    """
    assertTrue(varbinaryRows.size() > 0, "Varbinary system table query should return data")
    assertTrue(String.valueOf(varbinaryRows[0][3]).contains("0x"),
            "Expected lower_bounds to use varbinary hex output")
    assertTrue(String.valueOf(varbinaryRows[0][4]).contains("0x"),
            "Expected upper_bounds to use varbinary hex output")
}
