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

suite("test_iceberg_sys_table", "p0,external,doris,external_docker,external_docker_doris") {

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

    def test_iceberg_systable = { tblName, systableType ->
        def systableName = "${tblName}\$${systableType}"

        order_qt_desc_systable1 """desc ${systableName}"""
        order_qt_desc_systable2 """desc ${db_name}.${systableName}"""
        order_qt_desc_systable3 """desc ${catalog_name}.${db_name}.${systableName}"""

        List<List<Object>> schema = sql """desc ${systableName}"""
        String key = String.valueOf(schema[1][0])

        order_qt_tbl1_systable """select * from ${systableName}"""
        order_qt_tbl1_systable_select """select ${key} from ${systableName}"""
        order_qt_tbl1_systable_count """select count(*) from ${systableName}"""
        order_qt_tbl1_systable_count_select """select count(${key}) from ${systableName}"""

        List<List<Object>> res1 = sql """select ${key} from ${systableName} order by ${key}"""
        List<List<Object>> res2 = sql """select ${key} from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${tblName}",
            "query_type" = "${systableType}") order by ${key};
        """
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
        }

        if (res1.isEmpty()) {
            return
        }

        String value = String.valueOf(res1[0][0])
        order_qt_tbl1_systable_where """
            select * from ${systableName} where ${key}="${value}";
        """
        order_qt_tbl1_systable_where_count """
            select count(*) from ${systableName} where ${key}="${value}";
        """
        order_qt_systable_where2 """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${tblName}",
            "query_type" = "${systableType}") where ${key}="${value}";
        """
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

        order_qt_select_entries """select status, sequence_number, file_sequence_number from ${systableName}"""
        order_qt_select_entries_count """select count(*) from ${systableName}"""
        order_qt_select_entries_where """select status, sequence_number, file_sequence_number from ${systableName} where status="0";"""
        order_qt_select_entries_where_count """select count(status) from ${systableName} where status="0";"""
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

        order_qt_select_files """select content, file_format, record_count, lower_bounds, upper_bounds from ${systableName}"""
        order_qt_select_files_count """select count(*) from ${systableName}"""
        order_qt_select_files_where """select content, file_format, record_count, lower_bounds, upper_bounds from ${systableName} where content="0";"""
        order_qt_select_files_where_count """select count(content) from ${systableName} where content="0";"""
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

        List<List<Object>> res1 = sql """select * from ${systableName} order by snapshot_id"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "history") order by snapshot_id"""
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
        }
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

        List<List<Object>> res1 = sql """select * from ${systableName} order by timestamp"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "metadata_log_entries") order by timestamp"""
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
        }
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

        order_qt_select_snapshots """select operation from ${systableName}"""
        order_qt_select_snapshots_count """select count(*) from ${systableName}"""

        List<List<Object>> res1 = sql """select * from ${systableName} order by committed_at"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "snapshots") order by committed_at"""
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
        }
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

        order_qt_select_refs """select name, type from ${systableName}"""
        order_qt_select_refs_count """select count(*) from ${systableName}"""

        List<List<Object>> res1 = sql """select * from ${systableName} order by snapshot_id"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "refs") order by snapshot_id"""
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
        }
    }

    def test_systable_manifests = { table ->
        def systableName = "${table}\$manifests"
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

        List<List<Object>> res1 = sql """select * from ${systableName} order by path"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "manifests") order by path"""
        assertEquals(res1.size(), res2.size());
        for (int i = 0; i < res1.size(); i++) {
            for (int j = 0; j < res1[i].size(); j++) {
                assertEquals(res1[i][j], res2[i][j]);
            }
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

        
        List<List<Object>> res1 = sql """select * from ${systableName};"""
        List<List<Object>> res2 = sql """select * from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${table}",
            "query_type" = "partitions");"""
        assertEquals(res1.size(), res2.size());
        // just test can be selected successully
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
        test_systable_manifests(table)
        test_systable_partitions(table)
        // TODO: these table will be supportted in future
        // test_systable_all_manifests(table)
        // test_systable_position_deletes(table)

        test {
            sql """select * from ${table}\$all_manifests"""
            exception "SysTable all_manifests is not supported yet"
        }
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
                 select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                             "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
                                             "query_type" = "snapshots");
              """
              exception "denied"
        }
        test {
              sql """
                 select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots
              """
              exception "denied"
        }
    }
    sql """grant select_priv on ${catalog_name}.${db_name}.test_iceberg_systable_tbl1 to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """
           select committed_at, snapshot_id, parent_id, operation from iceberg_meta(
                                       "table" = "${catalog_name}.${db_name}.test_iceberg_systable_tbl1",
                                       "query_type" = "snapshots");
        """
        sql """select committed_at, snapshot_id, parent_id, operation from ${catalog_name}.${db_name}.test_iceberg_systable_tbl1\$snapshots"""
    }
    try_sql("DROP USER ${user}")
}
