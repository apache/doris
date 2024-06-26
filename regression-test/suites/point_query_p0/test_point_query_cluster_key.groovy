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

import java.math.BigDecimal;

suite("test_point_query_cluster_key") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    } 
    try {
        set_be_config.call("disable_storage_row_cache", "false")
        // nereids do not support point query now
        sql """set enable_nereids_planner=true"""

        def user = context.config.jdbcUser
        def password = context.config.jdbcPassword
        def realDb = "regression_test_serving_p0"
        def tableName = realDb + ".tbl_point_query_cluster_key"
        sql "CREATE DATABASE IF NOT EXISTS ${realDb}"

        // Parse url
        String jdbcUrl = context.config.jdbcUrl
        String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
        def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
        def sql_port
        if (urlWithoutSchema.indexOf("/") >= 0) {
            // e.g: jdbc:mysql://locahost:8080/?a=b
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
        } else {
            // e.g: jdbc:mysql://locahost:8080
            sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
        }
        // set server side prepared statement url
        def prepare_url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb + "?&useServerPrepStmts=true"

        def generateString = {len ->
            def str = ""
            for (int i = 0; i < len; i++) {
                str += "a"
            }
            return str
        }

        def nprep_sql = { sql_str ->
            def url_without_prep = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb
            connect(user = user, password = password, url = url_without_prep) {
                // set to false to invalid cache correcly
                sql "set enable_memtable_on_sink_node = false"
                sql sql_str
            }
        }

        sql """DROP TABLE IF EXISTS ${tableName}"""
        // test {
        //     // abnormal case
        //     sql """
        //           CREATE TABLE IF NOT EXISTS ${tableName} (
        //             `k1` int NULL COMMENT ""
        //           ) ENGINE=OLAP
        //           UNIQUE KEY(`k1`)
        //           DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        //           PROPERTIES (
        //           "replication_allocation" = "tag.location.default: 1",
        //           "store_row_column" = "true",
        //           "light_schema_change" = "false"
        //           )
        //       """
        //     exception "errCode = 2, detailMessage = Row store column rely on light schema change, enable light schema change first"
        // }

        def create_table_sql = { property ->
            return String.format("""
                  CREATE TABLE IF NOT EXISTS ${tableName} (
                    `k1` int(11) NULL COMMENT "",
                    `k2` decimalv3(27, 9) NULL COMMENT "",
                    `k3` varchar(300) NULL COMMENT "",
                    `k4` varchar(30) NULL COMMENT "",
                    `k5` date NULL COMMENT "",
                    `k6` datetime NULL COMMENT "",
                    `k7` float NULL COMMENT "",
                    `k8` datev2 NULL COMMENT "",
                    `k9` boolean NULL COMMENT "",
                    `k10` decimalv3(20, 3) NULL COMMENT "",
                    `k11` array<decimalv3(27, 9)> NULL COMMENT "",
                    `k12` array<text> NULL COMMENT ""
                  ) ENGINE=OLAP
                  UNIQUE KEY(`k1`, `k2`, `k3`)
                  CLUSTER BY(`k9`, `k5`, `k4`, `k2`)
                  DISTRIBUTED BY HASH(`k1`, k2, k3) BUCKETS 1
                  PROPERTIES (
                  "replication_allocation" = "tag.location.default: 1",
                  "store_row_column" = "true",
                  "enable_unique_key_merge_on_write" = "true",
                  "light_schema_change" = "true",
                  %s
                  "storage_format" = "V2")
                  """, property)
        }

        for (int i = 0; i < 2; i++) {
            tableName = realDb + ".tbl_point_query_cluster_key" + i
            sql """DROP TABLE IF EXISTS ${tableName}"""
            if (i == 0) {
                def sql0 = create_table_sql("")
                sql """ ${sql0} """
            } else {
                def sql1 = create_table_sql("\"function_column.sequence_col\" = 'k6',")
                sql """ ${sql1} """
            }
            sql """ INSERT INTO ${tableName} VALUES(1231, 119291.11, "ddd", "laooq", null, "2020-01-01 12:36:38", null, "1022-01-01 11:30:38", null, 1.111112, [119181.1111, 819019.1191, null], null) """
            sql """ INSERT INTO ${tableName} VALUES(1232, 12222.99121135, "xxx", "laooq", "2023-01-02", "2020-01-01 12:36:38", 522.762, "2022-01-01 11:30:38", 1, 212.111, null, null) """
            sql """ INSERT INTO ${tableName} VALUES(1233, 1.392932911, "yyy", "laooq", "2024-01-02", "2020-01-01 12:36:38", 52.862, "3022-01-01 11:30:38", 0, 5973903488739435.668, [119181.1111, null, 819019.1191], ["dijiiixxx"]) """
            sql """ INSERT INTO ${tableName} VALUES(1234, 12919291.129191137, "xxddd", "laooq", "2025-01-02", "2020-01-01 12:36:38", 552.872, "4022-01-01 11:30:38", 1, 5973903488739435.668, [1888109181.192111, 192129019.1191], ["1", "2", "3"]) """
            sql """ INSERT INTO ${tableName} VALUES(1235, 991129292901.11138, "dd", null, "2120-01-02", "2020-01-01 12:36:38", 652.692, "5022-01-01 11:30:38", 0, 90696620686827832.374, [119181.1111], ["${generateString(251)}"]) """
            sql """ INSERT INTO ${tableName} VALUES(1236, 100320.11139, "laa    ddd", "laooq", "2220-01-02", "2020-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38", 1, 23698.299, [], ["${generateString(251)}"]) """
            sql """ INSERT INTO ${tableName} VALUES(1237, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", 0, 90696620686827832.374, [1.1, 2.2, 3.3, 4.4, 5.5], []) """
            sql """ INSERT INTO ${tableName} VALUES(251, 120939.11130, "${generateString(251)}", "laooq", "2030-01-02", "2020-01-01 12:36:38", 251, "7022-01-01 11:30:38", 1, 90696620686827832.374, [11111], []) """
            sql """ INSERT INTO ${tableName} VALUES(252, 120939.11130, "${generateString(252)}", "laooq", "2030-01-02", "2020-01-01 12:36:38", 252, "7022-01-01 11:30:38", 0, 90696620686827832.374, [0], null) """
            sql """ INSERT INTO ${tableName} VALUES(298, 120939.11130, "${generateString(298)}", "laooq", "2030-01-02", "2020-01-01 12:36:38", 298, "7022-01-01 11:30:38", 1, 90696620686827832.374, [], []) """

            def result1 = connect(user=user, password=password, url=prepare_url) {
                def stmt = prepareStatement "select /*+ SET_VAR(enable_nereids_planner=true) */ * from ${tableName} where k1 = ? and k2 = ? and k3 = ?"
                assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
                stmt.setInt(1, 1231)
                stmt.setBigDecimal(2, new BigDecimal("119291.11"))
                stmt.setString(3, "ddd")
                qe_point_select stmt
                stmt.setInt(1, 1231)
                stmt.setBigDecimal(2, new BigDecimal("119291.11"))
                stmt.setString(3, "ddd")
                qe_point_select stmt
                stmt.setInt(1, 1237)
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, "a    ddd")
                qe_point_select stmt

                stmt.setInt(1, 1232)
                stmt.setBigDecimal(2, new BigDecimal("12222.99121135"))
                stmt.setString(3, 'xxx')
                qe_point_select stmt

                stmt.setInt(1, 251)
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, generateString(251))
                qe_point_select stmt

                stmt.setInt(1, 252)
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, generateString(252))
                qe_point_select stmt

                stmt.setInt(1, 298)
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, generateString(298))
                qe_point_select stmt
                stmt.close()

                stmt = prepareStatement "select /*+ SET_VAR(enable_nereids_planner=true) */ * from ${tableName} where k1 = ? and k2 = ? and k3 = ?"
                assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
                stmt.setInt(1, 1235)
                stmt.setBigDecimal(2, new BigDecimal("991129292901.11138"))
                stmt.setString(3, "dd")
                qe_point_select stmt

                def stmt_fn = prepareStatement "select /*+ SET_VAR(enable_nereids_planner=true) */ hex(k3), hex(k4) from ${tableName} where k1 = ? and k2 =? and k3 = ?"
                assertEquals(stmt_fn.class, com.mysql.cj.jdbc.ServerPreparedStatement);
                stmt_fn.setInt(1, 1231)
                stmt_fn.setBigDecimal(2, new BigDecimal("119291.11"))
                stmt_fn.setString(3, "ddd")
                qe_point_select stmt_fn
                qe_point_select stmt_fn
                qe_point_select stmt_fn

                nprep_sql """
                ALTER table ${tableName} ADD COLUMN new_column0 INT default "0";
                """
                sleep(1);
                nprep_sql """ INSERT INTO ${tableName} VALUES(1235, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", 1, 1.1111299, [119291.19291], ["111", "222", "333"], 1) """
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, "a    ddd")
                qe_point_select stmt
                qe_point_select stmt
                // invalidate cache
                nprep_sql """ INSERT INTO ${tableName} VALUES(1235, 120939.11130, "a    ddd", "xxxxxx", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", 0, 1929111.1111,[119291.19291], ["111", "222", "333"], 2) """
                qe_point_select stmt
                qe_point_select stmt
                qe_point_select stmt
                nprep_sql """
                ALTER table ${tableName} ADD COLUMN new_column1 INT default "0";
                """
                qe_point_select stmt
                qe_point_select stmt
                nprep_sql """
                ALTER table ${tableName} DROP COLUMN new_column1;
                """
                qe_point_select stmt
                qe_point_select stmt

                sql """
                  ALTER table ${tableName} ADD COLUMN new_column1 INT default "0";
                """
                qe_point_select stmt
            }
            // disable useServerPrepStmts
            def result2 = connect(user=user, password=password, url=context.config.jdbcUrl) {
                qt_sql """select /*+ SET_VAR(enable_nereids_planner=true) */ * from ${tableName} where k1 = 1231 and k2 = 119291.11 and k3 = 'ddd'"""
                qt_sql """select /*+ SET_VAR(enable_nereids_planner=true) */ * from ${tableName} where k1 = 1237 and k2 = 120939.11130 and k3 = 'a    ddd'"""
                qt_sql """select /*+ SET_VAR(enable_nereids_planner=true) */ hex(k3), hex(k4), k7 + 10.1 from ${tableName} where k1 = 1237 and k2 = 120939.11130 and k3 = 'a    ddd'"""
                // prepared text
                // sql """ prepare stmt1 from  select * from ${tableName} where k1 = % and k2 = % and k3 = % """
                // qt_sql """execute stmt1 using (1231, 119291.11, 'ddd')"""
                // qt_sql """execute stmt1 using (1237, 120939.11130, 'a    ddd')"""

                // sql """prepare stmt2 from  select * from ${tableName} where k1 = % and k2 = % and k3 = %"""
                // qt_sql """execute stmt2 using (1231, 119291.11, 'ddd')"""
                // qt_sql """execute stmt2 using (1237, 120939.11130, 'a    ddd')"""
                tableName = "test_query_cluster_key"
                sql """DROP TABLE IF EXISTS ${tableName}"""
                sql """CREATE TABLE ${tableName} (
                        `customer_key` bigint(20) NULL,
                        `customer_btm_value_0` text NULL,
                        `customer_btm_value_1` text NULL,
                        `customer_btm_value_2` text NULL
                    ) ENGINE=OLAP
                    UNIQUE KEY(`customer_key`)
                    CLUSTER BY(`customer_btm_value_1`)
                    COMMENT 'OLAP'
                    DISTRIBUTED BY HASH(`customer_key`) BUCKETS 16
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "storage_format" = "V2",
                    "light_schema_change" = "true",
                    "store_row_column" = "true",
                    "enable_unique_key_merge_on_write" = "true",
                    "disable_auto_compaction" = "false"
                    );"""
                sql """insert into ${tableName} values (0, "1", "2", "3")"""
                qt_sql """select /*+ SET_VAR(enable_nereids_planner=true) */ * from ${tableName} where customer_key = 0"""
            }
        }
    } finally {
        set_be_config.call("disable_storage_row_cache", "true")
    }
}