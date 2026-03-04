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

suite("test_iceberg_sys_table", "p0,external,doris,external_docker,external_docker_doris,system_table") {

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
        sql """explain select * from ${systableName}"""
        sql """select * from `${systableName}`"""
        sql """select * from ${systableName}"""
        // Case sensitivity
        test {
            sql """select * from ${tblName}\$${systableType.toUpperCase()}"""
            exception "Unknown sys table"
        }

        // Select only part of the columns (first two columns)
        List<List<Object>> schema = sql """desc ${systableName}"""
        if (schema.size() >= 2) {
            String col1 = String.valueOf(schema[0][0])
            String col2 = String.valueOf(schema[1][0])
            sql """select `${col1}`, `${col2}` from ${systableName}"""
        }
        // WHERE filter (use file_size if exists, otherwise use the first column)
        boolean hasFileSize = schema.any { it[0] == "file_size" }
        if (hasFileSize) {
            sql """select * from ${systableName} where file_size > 100000"""
        } else {
            String col1 = String.valueOf(schema[0][0])
            List<List<Object>> rows = sql """select `${col1}` from ${systableName} limit 1"""
            if (!rows.isEmpty()) {
                String val = rows[0][0]
                sql """select * from ${systableName} where `${col1}` = '${val}' """
            }
        }

        // LIMIT query
        sql """select * from ${systableName} limit 10"""

        // OFFSET pagination
        sql """select * from ${systableName} limit 5 offset 5"""

        // AGG
        order_qt_systable_count """select count(*) from ${systableName}"""
        if (schema.size() >= 2) {
            String col2 = String.valueOf(schema[1][0])
            sql """select count(distinct ${col2}) from ${systableName}"""
            sql """select `${col2}`, count(*) from ${systableName} group by ${col2}"""
        }

        // Subquery/CTE
        if (schema.size() >= 2) {
            String col1 = String.valueOf(schema[0][0])
            String col2 = String.valueOf(schema[1][0])
            sql """with t as (select `${col1}`, `${col2}` from ${systableName}) select count(*) from t"""
        }

        // JOIN (with temp table)
        if (schema.size() >= 1) {
            String col1 = String.valueOf(schema[0][0])
            sql """select a.`${col1}` from ${systableName} a join (select 1) b on 1=1 limit 1"""
            sql """select a.`${col1}`, b.`${col1}` from ${systableName} a join ${systableName} b on a.`${col1}`=b.`${col1}` limit 1"""

            String otherSystableType = (systableType == 'files') ? 'entries' : 'files'
            String otherSystableName = "${tblName}\$${otherSystableType}"
            List<List<Object>> otherSchema = sql """desc ${otherSystableName}"""
            if (!otherSchema.isEmpty()) {
                String otherCol1 = String.valueOf(otherSchema[0][0])
                sql """select a.`${col1}`, b.`${otherCol1}` from ${systableName} a join ${otherSystableName} b on a.`${col1}`=b.`${otherCol1}` limit 1"""
            }
            sql """drop database if exists internal.join_inner_db"""
            sql """create database internal.join_inner_db"""
            sql """create table internal.join_inner_db.join_inner_tbl (`${col1}` varchar(100)) PROPERTIES ("replication_num" = "1");"""
            sql """insert into internal.join_inner_db.join_inner_tbl values('test_val')"""
            sql """select a.`${col1}`, t.`${col1}` from ${systableName} a join internal.join_inner_db.join_inner_tbl t on a.`${col1}`=t.`${col1}` limit 1"""
            sql """drop table if exists internal.join_inner_db.join_inner_tbl"""
        }

        // ORDER BY LIMIT
        if (schema.size() >= 1) {
            String col1 = String.valueOf(schema[0][0])
            sql """select * from ${systableName} order by `${col1}` desc limit 3"""
        }

        // select * correctness check (row count, column count)
        sql """select count(*) from ${systableName}"""
        sql """select * from ${systableName} limit 1"""

        sql """drop database if exists internal.view_db_iceberg_db"""
        sql """create database internal.view_db_iceberg_db"""
        // VIEW
        sql """drop view if exists internal.view_db_iceberg_db.v_sys_table_${tblName}"""
        sql """create view internal.view_db_iceberg_db.v_sys_table_${tblName} as select * from ${systableName}"""
        sql """select * from internal.view_db_iceberg_db.v_sys_table_${tblName} limit 1"""
        sql """drop view if exists internal.view_db_iceberg_db.v_sys_table_${tblName}"""


        // MTMV
        sql """drop materialized view if exists internal.view_db_iceberg_db.mtmv_sys_table_${tblName}"""
        sql """create materialized view internal.view_db_iceberg_db.mtmv_sys_table_${tblName} BUILD IMMEDIATE REFRESH AUTO ON MANUAL 
    DISTRIBUTED BY RANDOM BUCKETS 2 
    PROPERTIES ('replication_num' = '1') 
    AS select count(*) as cnt from ${systableName}"""
        sql """select * from internal.view_db_iceberg_db.mtmv_sys_table_${tblName} limit 1"""
        sql """drop materialized view if exists internal.view_db_iceberg_db.mtmv_sys_table_${tblName}"""
        // OUTFILE
        // SELECT INTO OUTFILE
        sql """select * from ${systableName} into outfile 'file:///iceberg_${tblName}_out.txt'"""
        // EXPORT not supported yet
        // sql """export table ${sysTable} to 'file:///paimon_export_${systemTable}'"""


        order_qt_desc_systable1 """desc ${systableName}"""
        order_qt_desc_systable2 """desc ${db_name}.${systableName}"""
        order_qt_desc_systable3 """desc ${catalog_name}.${db_name}.${systableName}"""

        // Re-fetch schema to avoid impact from previous definition
        schema = sql """desc ${systableName}"""
        String key = String.valueOf(schema[1][0])
        String key2 = String.valueOf(schema[2][0])
        order_qt_tbl1_systable_count """select count(*) from ${systableName}"""
        order_qt_tbl1_systable_count_select """select count(${key}) from ${systableName}"""

        List<List<Object>> res1 = sql """select ${key} from ${systableName} order by ${key},${key2}"""
        List<List<Object>> res2 = sql """select ${key} from iceberg_meta(
            "table" = "${catalog_name}.${db_name}.${tblName}",
            "query_type" = "${systableType}") order by ${key},${key2};
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
        order_qt_tbl1_systable_where_count """
            select count(*) from ${systableName} where `${key}`='${value}';
        """
    }

    def test_systable_entries = { table, systableType ->
        def systableName = "${table}\$${systableType}"
        sql """select * from ${systableName}"""
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
        sql """select * from ${systableName}"""
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
        sql """select * from ${systableName}"""
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
        sql """select * from ${systableName}"""
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
        sql """select * from ${systableName}"""
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
        sql """select * from ${systableName}"""
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
            List<List<Object>> res1 = sql """select * from ${systableName} order by path"""
            List<List<Object>> res2 = sql """select * from iceberg_meta(
                "table" = "${catalog_name}.${db_name}.${table}",
                "query_type" = "${systableType}") order by path"""
            assertEquals(res1.size(), res2.size());
            for (int i = 0; i < res1.size(); i++) {
                for (int j = 0; j < res1[i].size(); j++) {
                    assertEquals(res1[i][j], res2[i][j]);
                }
            }
        } else {
            List<List<Object>> res1 = sql """select * from ${systableName} order by path, reference_snapshot_id"""
            List<List<Object>> res2 = sql """select * from iceberg_meta(
                "table" = "${catalog_name}.${db_name}.${table}",
                "query_type" = "${systableType}") order by path, reference_snapshot_id"""
            assertEquals(res1.size(), res2.size());
            for (int i = 0; i < res1.size(); i++) {
                for (int j = 0; j < res1[i].size(); j++) {
                    assertEquals(res1[i][j], res2[i][j]);
                }
            }
        }
    }

    def test_systable_partitions = { table ->
        def systableName = "${table}\$partitions"
        sql """select * from ${systableName}"""
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
        test_iceberg_systable(table, "entries")
        test_systable_entries(table, "all_entries")
        test_iceberg_systable(table, "all_entries")
        test_systable_files(table, "files")
        test_iceberg_systable(table, "files")
        test_systable_files(table, "data_files")
        test_iceberg_systable(table, "data_files")
        test_systable_files(table, "delete_files")
        test_iceberg_systable(table, "delete_files")
        test_systable_files(table, "all_files")
        test_iceberg_systable(table, "all_files")
        test_systable_files(table, "all_data_files")
        test_iceberg_systable(table, "all_data_files")
        test_systable_files(table, "all_delete_files")
        test_iceberg_systable(table, "all_delete_files")
        test_systable_history(table)
        test_iceberg_systable(table, "history")
        test_systable_metadata_log_entries(table)
        test_iceberg_systable(table, "metadata_log_entries")
        test_systable_snapshots(table)
        test_iceberg_systable(table, "snapshots")
        test_systable_refs(table)
        test_iceberg_systable(table, "refs")
        test_systable_manifests(table, "manifests")
        test_systable_manifests(table, "all_manifests")
        test_iceberg_systable(table, "manifests")
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
    order_qt_varbinary_sys_table_select """select content, file_format, record_count, lower_bounds, upper_bounds from test_iceberg_systable_unpartitioned\$files;"""
}
