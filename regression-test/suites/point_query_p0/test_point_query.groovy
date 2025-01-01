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
import com.mysql.cj.ServerPreparedQuery
import com.mysql.cj.jdbc.ConnectionImpl
import com.mysql.cj.jdbc.JdbcStatement
import com.mysql.cj.jdbc.ServerPreparedStatement
import com.mysql.cj.jdbc.StatementImpl
import org.apache.doris.regression.util.JdbcUtils

import java.lang.reflect.Field
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.CopyOnWriteArrayList

suite("test_point_query", "nonConcurrent") {
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
        sql "set global enable_fallback_to_original_planner = false"
        sql """set global enable_nereids_planner=true"""
        def user = context.config.jdbcUser
        def password = context.config.jdbcPassword
        def realDb = "regression_test_serving_p0"
        def tableName = realDb + ".tbl_point_query"
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
            connect(user, password, url_without_prep) {
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
            tableName = realDb + ".tbl_point_query" + i
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

            def result1 = connect(user, password, prepare_url) {
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
                stmt.setBigDecimal(1, 1235)
                stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
                stmt.setString(3, "a    ddd")
                qe_point_select stmt
                qe_point_select stmt
                // invalidate cache
                // "sync"
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

                nprep_sql """
                  ALTER table ${tableName} ADD COLUMN new_column1 INT default "0";
                """
                sql "select 1"
                qe_point_select stmt
            }
            // disable useServerPrepStmts
            def result2 = connect(user, password, context.config.jdbcUrl) {
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
                tableName = "test_query"
                sql """DROP TABLE IF EXISTS ${tableName}"""
                sql """CREATE TABLE ${tableName} (
                        `customer_key` bigint(20) NULL,
                        `customer_btm_value_0` text NULL,
                        `customer_btm_value_1` text NULL,
                        `customer_btm_value_2` text NULL
                    ) ENGINE=OLAP
                    UNIQUE KEY(`customer_key`)
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
        sql "DROP TABLE IF EXISTS test_ODS_EBA_LLREPORT";
        sql """
            CREATE TABLE `test_ODS_EBA_LLREPORT` (
              `RPTNO` VARCHAR(20) NOT NULL ,
              `A_ENTTYP` VARCHAR(6) NULL ,
              `A_INTIME` DATETIME NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`RPTNO`)
            DISTRIBUTED BY HASH(`RPTNO`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "store_row_column" = "true"
            ); 
        """                
        sql "insert into test_ODS_EBA_LLREPORT(RPTNO) values('567890')"
        sql "select  /*+ SET_VAR(enable_nereids_planner=true) */  substr(RPTNO,2,5) from test_ODS_EBA_LLREPORT where  RPTNO = '567890'"

        sql "DROP TABLE IF EXISTS test_cc_aaaid2";
        sql """
        CREATE TABLE `test_cc_aaaid2` (
          `aaaid` VARCHAR(13) NULL COMMENT '3aid'
        ) ENGINE=OLAP
        UNIQUE KEY(`aaaid`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`aaaid`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true",
        "store_row_column" = "true"
        );
        """                
        sql """insert into `test_cc_aaaid2` values('1111111')"""
        qt_sql """SELECT
             `__DORIS_DELETE_SIGN__`,
             aaaid

            FROM
             `test_cc_aaaid2` 
            WHERE
             aaaid = '1111111'"""
    } finally {
        set_be_config.call("disable_storage_row_cache", "true")
        sql """set global enable_nereids_planner=true"""
    }

    // test partial update/delete
    sql "DROP TABLE IF EXISTS table_3821461"
    sql """
        CREATE TABLE `table_3821461` (
            `col1` smallint NOT NULL,
            `col2` int NOT NULL,
            `loc3` char(10) NOT NULL,
            `value` char(10) NOT NULL,
            INDEX col3 (`loc3`) USING INVERTED,
            INDEX col2 (`col2`) USING INVERTED )
        ENGINE=OLAP UNIQUE KEY(`col1`, `col2`, `loc3`)
        DISTRIBUTED BY HASH(`col1`, `col2`, `loc3`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "bloom_filter_columns" = "col1", "store_row_column" = "true", "enable_mow_light_delete" = "false" );
    """
    sql "insert into table_3821461 values (-10, 20, 'aabc', 'value')"
    sql "insert into table_3821461 values (10, 20, 'aabc', 'value');"
    sql "insert into table_3821461 values (20, 30, 'aabc', 'value');"
    explain {
        sql("select * from table_3821461 where col1 = -10 and col2 = 20 and loc3 = 'aabc'")
        contains "SHORT-CIRCUIT"
    } 
    qt_sql "select * from table_3821461 where col1 = 10 and col2 = 20 and loc3 = 'aabc';"
    sql "delete from table_3821461 where col1 = 10 and col2 = 20 and loc3 = 'aabc';"
    qt_sql "select * from table_3821461 where col1 = 10 and col2 = 20 and loc3 = 'aabc';"
    sql "update table_3821461 set value = 'update value' where col1 = -10 or col1 = 20;"
    qt_sql """select * from table_3821461 where col1 = -10 and col2 = 20 and loc3 = 'aabc'"""

    tableName = "in_table_1"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 1: Default partitioning, part of the primary key is a bucket column
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            distributed by hash(a) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 132, "a");
            insert into ${tableName} values(123, 222, "b");
            insert into ${tableName} values(22, 2, "c");
            insert into ${tableName} values(1, 1, "d");
            insert into ${tableName} values(2, 2, "e");
            insert into ${tableName} values(3, 3, "f");
            insert into ${tableName} values(4, 4, "i");
        """
    qt_case_1_sql "select * from ${tableName} where a = 123 and b = 132;"
    explain {
        sql("select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);")
        contains "SHORT-CIRCUIT"
    }
    order_qt_case_1_sql "select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);"
    explain {
        sql("select * from ${tableName} where a in (123, 1, 222) and b in (132, 1, 222, 333);")
        contains "SHORT-CIRCUIT"
    }
    order_qt_case_1_sql "select * from ${tableName} where a in (123, 1, 222) and b in (132, 1, 222, 333);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    // Case 2: Partition columns, bucket columns, and primary keys are the same
    tableName = "in_table_2"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a, b)
            (
                partition p0 values [(100, 100), (200, 140)),
                partition p1 values [(200, 140), (300, 170)),
                partition p2 values [(300, 170), (400, 250)),
                partition p3 values [(400, 250), (420, 300)),
                partition p4 values [(420, 300), (500, 400))
            )
            distributed by hash(a, b) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
            """
    sql """
            insert into ${tableName} values(123, 120, "a");
            insert into ${tableName} values(150, 120, "b");
            insert into ${tableName} values(222, 150, "c");
            insert into ${tableName} values(333, 200, "e");
            insert into ${tableName} values(400, 260, "f");
            insert into ${tableName} values(400, 250, "g");
            insert into ${tableName} values(440, 350, "h");
            insert into ${tableName} values(450, 320, "i");
        """

    qt_case_2_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_2_sql "select * from ${tableName} where a = 222 and b = 150;"
    order_qt_case_2_sql "select * from ${tableName} where a = 123 and b in (132, 120, 222, 333);"
    order_qt_case_2_sql "select * from ${tableName} where a = 400 and b in (260, 250, 300);"
    order_qt_case_2_sql "select * from ${tableName} where a in (400, 222, 100) and b in (260, 250, 100, 150);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_3"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 3: The partition column is the same as the primary key, and the bucket column is part of the primary key.
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a, b)
            (
                partition p0 values [(100, 100), (100, 140)),
                partition p1 values [(100, 140), (200, 140)),
                partition p2 values [(200, 140), (300, 170)),
                partition p3 values [(300, 170), (400, 250)),
                partition p4 values [(400, 250), (420, 300)),
                partition p5 values [(420, 300), (500, 400))
            )
            distributed by hash(a)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(100, 100, "aaa");
            insert into ${tableName} values(100, 120, "aaaaa");
            insert into ${tableName} values(123, 100, "a");
            insert into ${tableName} values(150, 100, "b");
            insert into ${tableName} values(350, 200, "c");
            insert into ${tableName} values(400, 250, "d");
            insert into ${tableName} values(400, 280, "e");
            insert into ${tableName} values(450, 350, "f");
        """
    qt_case_3_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_3_sql "select * from ${tableName} where a = 222 and b = 100;"
    order_qt_case_3_sql "select * from ${tableName} where a = 100 and b in (132, 100, 222, 120);"
    order_qt_case_3_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333);"
    order_qt_case_3_sql "select * from ${tableName} where a = 400 and b in (250, 280, 300);"
    order_qt_case_3_sql "select * from ${tableName} where a in (123, 1, 350, 400, 420, 500, 1000) and b in (132, 100, 222, 200, 350, 250);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_4"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 4: Bucket columns and partition columns are both partial primary keys,
    // and there is no overlap between bucket columns and partition columns
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(c)
            (
                partition p0 values [(100), (200)),
                partition p1 values [(200), (300)),
                partition p2 values [(300), (400)),
                partition p3 values [(400), (500))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_4_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_4_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_4_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_4_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_4_sql "select * from ${tableName} where a in (123, 1, 222, 1231, 420, 500) and b in (132, 100, 222, 1220, 300) and c in (210, 110, 115, 210);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_5"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 5: Bucket columns and partition columns are both partial primary keys,
    // and bucket columns and partition columns overlap
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a)
            (
                partition p0 values [(0), (100)),
                partition p1 values [(100), (200)),
                partition p2 values [(200), (300)),
                partition p3 values [(300), (400)),
                partition p4 values [(400), (500)),
                partition p5 values [(500), (900)),
                partition p6 values [(900), (1200)),
                partition p7 values [(1200), (9000))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_5_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_5_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_5_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_5_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_5_sql "select * from ${tableName} where a in (123, 12, 222, 1231, 420, 500, 6333, 633, 843) and b in (132, 100, 222, 1220, 300, 2642) and c in (210, 110, 115, 210, 480);"

    sql """DROP TABLE IF EXISTS ${tableName}"""
    
    tableName = "in_table_6"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 6: Default partitioning, primary keys are all bucket columns
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            distributed by hash(a, b) buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 132, "a");
            insert into ${tableName} values(123, 222, "b");
            insert into ${tableName} values(22, 2, "c");
            insert into ${tableName} values(1, 1, "d");
            insert into ${tableName} values(2, 2, "e");
            insert into ${tableName} values(3, 3, "f");
            insert into ${tableName} values(4, 4, "i");
        """
    qt_case_6_sql "select * from ${tableName} where a = 123 and b = 132;"
    order_qt_case_6_sql "select * from ${tableName} where a = 123 and b in (132, 1, 222, 333);"
    order_qt_case_6_sql "select * from ${tableName} where a in (123, 1, 222, 2, 3, 4) and b in (132, 1, 222, 333, 2, 3, 4);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_7"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 7: Partition and bucket columns are the same, but only part of the primary key
    sql """
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a)
            (
                partition p0 values [(0), (100)),
                partition p1 values [(100), (200)),
                partition p2 values [(200), (300)),
                partition p3 values [(300), (400)),
                partition p4 values [(400), (500)),
                partition p5 values [(500), (900)),
                partition p6 values [(900), (1200)),
                partition p7 values [(1200), (9000))
            )
            distributed by hash(a)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """

    sql """
            insert into ${tableName} values(123, 100, 110, "a");
            insert into ${tableName} values(222, 100, 115, "b");
            insert into ${tableName} values(12, 12, 120, "c");
            insert into ${tableName} values(1231, 1220, 210, "d");
            insert into ${tableName} values(323, 49, 240, "e");
            insert into ${tableName} values(843, 7342, 370, "f");
            insert into ${tableName} values(633, 2642, 480, "g");
            insert into ${tableName} values(6333, 2642, 480, "h");
        """

    qt_case_7_sql "select * from ${tableName} where a = 123 and b = 100 and c = 110;"
    qt_case_7_sql "select * from ${tableName} where a = 123 and b = 101 and c = 110;"
    qt_case_7_sql "select * from ${tableName} where a = 1231 and b = 1220 and c = 210;"
    order_qt_case_7_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333) and c in (110, 115, 120);"
    order_qt_case_7_sql "select * from ${tableName} where a in (123, 12, 222, 1231, 420, 500, 6333, 633, 843) and b in (132, 100, 222, 1220, 300, 2642) and c in (210, 110, 115, 210, 480);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_8"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 8: The bucket column is the same as the primary key, and the partition column is part of the primary key.
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c string not null
            )
            unique key(a, b)
            partition by RANGE(a)
            (
                partition p0 values [(100), (200)),
                partition p1 values [(200), (300)),
                partition p2 values [(300), (400)),
                partition p3 values [(400), (420)),
                partition p4 values [(420), (500))
            )
            distributed by hash(a, b)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 100, "a");
            insert into ${tableName} values(150, 100, "b");
            insert into ${tableName} values(350, 200, "c");
            insert into ${tableName} values(400, 250, "d");
            insert into ${tableName} values(400, 280, "e");
            insert into ${tableName} values(450, 350, "f");
        """
    qt_case_8_sql "select * from ${tableName} where a = 123 and b = 100;"
    qt_case_8_sql "select * from ${tableName} where a = 222 and b = 100;"
    order_qt_case_8_sql "select * from ${tableName} where a = 123 and b in (132, 100, 222, 333);"
    order_qt_case_8_sql "select * from ${tableName} where a = 400 and b in (250, 280, 300);"
    order_qt_case_8_sql "select * from ${tableName} where a in (123, 1, 350, 400, 420, 500, 1000) and b in (132, 100, 222, 200, 350, 250);"

    sql """DROP TABLE IF EXISTS ${tableName}"""

    tableName = "in_table_9"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    // Case 9: Partition leftmost match
    sql """    
            create table ${tableName} (
                a int not null,
                b int not null,
                c int not null,
                d string not null
            )
            unique key(a, b, c)
            partition by RANGE(a, b, c)
            (
                partition p values [(1, 1, 1), (100, 100, 100)),
                partition p0 values [(100, 100, 100), (200, 210, 220)),
                partition p1 values [(200, 210, 220), (300, 250, 290)),
                partition p2 values [(300, 250, 290), (350, 290, 310)),
                partition p3 values [(350, 290, 310), (400, 350, 390)),
                partition p4 values [(400, 350, 390), (800, 400, 450)),
                partition p5 values [(800, 400, 450), (2000, 500, 500)),
                partition p6 values [(2000, 500, 500), (5000, 600, 600)),
                partition p7 values [(5000, 600, 600), (9999, 9999, 9999))
            )
            distributed by hash(a, c)
            buckets 16
            PROPERTIES(
                "replication_num" = "1",
                "store_row_column" = "true"
            );
        """
    sql """
            insert into ${tableName} values(123, 100, 110, "zxcd");
            insert into ${tableName} values(222, 100, 115, "zxc");
            insert into ${tableName} values(12, 12, 120, "zxc");
            insert into ${tableName} values(1231, 1220, 210, "zxc");
            insert into ${tableName} values(323, 49, 240, "zxc");
            insert into ${tableName} values(843, 7342, 370, "zxcde");
            insert into ${tableName} values(633, 2642, 480, "zxc");
            insert into ${tableName} values(6333, 2642, 480, "zxc");
        """
    
    order_qt_case_9_sql "select * from ${tableName} where a=123 and b=100 and c=110;"
    order_qt_case_9_sql "select * from ${tableName} where a=222 and b=100 and c=115;"
    order_qt_case_9_sql "select * from ${tableName} where a=323 and b=49 and c=240;"
    order_qt_case_9_sql "select * from ${tableName} where b=100 and a=123 and c=110;"
    order_qt_case_9_sql "select * from ${tableName} where a=1231 and b=1220 and c=210;"
    order_qt_case_9_sql "select * from ${tableName} where a=6333 and b=2642 and c=480;"
    order_qt_case_9_sql "select * from ${tableName} where a=633 and b=2642 and c=480;"
    order_qt_case_9_sql "select * from ${tableName} where a=123 and b in (132,100,222,333) and c in (110, 115, 120);"
    order_qt_case_9_sql "select * from ${tableName} where a in (222,1231) and b in (100,1220,2642) and c in (115,210,480);"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,222,12) and b in (100,12) and c in (110,115,120,210);"
    order_qt_case_9_sql "select * from ${tableName} where a=1231 and b in (20490,1220,300) and c = 210;"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,1,222, 400,420, 500) and b in (132,100,222, 200,300) and c in (110,115,210);"
    order_qt_case_9_sql "select * from ${tableName} where a in (123,1,222, 1231,420, 500) and b in (132,100,222, 1220,300) and c in (210,110,115,210);"
    sql """DROP TABLE IF EXISTS ${tableName}"""
} 