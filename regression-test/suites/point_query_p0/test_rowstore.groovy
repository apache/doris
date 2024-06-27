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

suite("test_rowstore", "p0,nonConcurrent") {
    // Parse url
    String jdbcUrl = context.config.jdbcUrl
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def realDb = "regression_test_point_query_p0"
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    def prepare_url = "jdbc:mysql://" + sql_ip + ":" + sql_port + "/" + realDb + "?&useServerPrepStmts=true"

    sql "DROP TABLE IF EXISTS table_with_column_group"
    sql """
        CREATE TABLE IF NOT EXISTS table_with_column_group (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` bigint NULL COMMENT "",
                `v3` double NULL COMMENT "",
                `v4` datetime NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "row_store_columns" = "v1,v2",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
    """
    sql """
        insert into table_with_column_group values (1, "11111111111111111111111111111111111111", 3, 4.0, '2021-02-01 11:11:11'), (2, "222222222222222222222222222222222", 3, 4, '2022-02-01 11:11:11'), (3, "33333333333333333333333333333333", 3, 4, '2023-02-01 11:11:11');
    """
    sql "set show_hidden_columns = true"
    qt_sql """
        select length(__DORIS_ROW_STORE_COL__) from table_with_column_group order by k1;
    """

     sql "DROP TABLE IF EXISTS table_with_column_group1"
    sql """
        CREATE TABLE IF NOT EXISTS table_with_column_group1 (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` bigint NULL COMMENT "",
                `v3` double NULL COMMENT "",
                `v4` datetime NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "row_store_columns" = "v2,v4",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
    """
    sql """
        insert into table_with_column_group1 values (1, "11111111111111111111111111111111111111", 3, 4.0, '2021-02-01 11:11:11'), (2, "222222222222222222222222222222222", 3, 4, '2022-02-01 11:11:11'), (3, "33333333333333333333333333333333", 3, 4, '2023-02-01 11:11:11');
    """
    sql "set show_hidden_columns = true"
    qt_sql """
        select length(__DORIS_ROW_STORE_COL__) from table_with_column_group1 order by k1;
    """

    sql "DROP TABLE IF EXISTS table_with_column_group2"
    sql """
        CREATE TABLE IF NOT EXISTS table_with_column_group2 (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` bigint NULL COMMENT "",
                `v3` double NULL COMMENT "",
                `v4` datetime NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "row_store_columns" = "v2",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
    """
    sql """
        insert into table_with_column_group2 values (1, "11111111111111111111111111111111111111", 3, 4.0, '2021-02-01 11:11:11'), (2, "222222222222222222222222222222222", 3, 4, '2022-02-01 11:11:11'), (3, "33333333333333333333333333333333", 3, 4, '2023-02-01 11:11:11');
    """
    qt_sql """
        select length(__DORIS_ROW_STORE_COL__) from table_with_column_group2 order by k1;
    """

    sql "DROP TABLE IF EXISTS table_with_column_group3"
    sql """
        CREATE TABLE IF NOT EXISTS table_with_column_group3 (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` bigint NULL COMMENT "",
                `v3` double NULL COMMENT "",
                `v4` datetime NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "row_store_columns" = "v2,v4",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
    """
     sql """
        insert into table_with_column_group3 values (1, "11111111111111111111111111111111111111", 3, 4.0, '2021-02-01 11:11:11'), (2, "222222222222222222222222222222222", 3, 4, '2022-02-01 11:11:11'), (3, "33333333333333333333333333333333", 3, 4, '2023-02-01 11:11:11');
    """
    qt_sql """
        select length(__DORIS_ROW_STORE_COL__) from table_with_column_group3 order by k1;
    """
    sql "set show_hidden_columns = false"

    sql """DROP TABLE IF EXISTS table_with_column_group_xxx"""
    sql """
            CREATE TABLE IF NOT EXISTS table_with_column_group_xxx (
                `user_id` int NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_1` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_2` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `last_visit_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `last_update_date` DATETIME DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `datetime_val1` DATETIMEV2(3) DEFAULT "1970-01-01 00:00:00.111" COMMENT "用户最后一次访问时间",
                `datetime_val2` DATETIME(6) DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次更新时间",
                `last_visit_date_not_null` DATETIME NOT NULL DEFAULT "1970-01-01 00:00:00" COMMENT "用户最后一次访问时间",
                `cost` BIGINT DEFAULT "0" COMMENT "用户总消费",
                `max_dwell_time` INT DEFAULT "0" COMMENT "用户最大停留时间",
                `min_dwell_time` INT DEFAULT "99999" COMMENT "用户最小停留时间")
            UNIQUE KEY(`user_id`, `date`, `datev2`, `datetimev2_1`, `datetimev2_2`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1",
                    "enable_unique_key_merge_on_write" = "true",
                    "row_store_columns" = "datetimev2_1,datetime_val1,datetime_val2,max_dwell_time"
            );
        """
    sql """ INSERT INTO table_with_column_group_xxx values
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.021', '2017-10-01 11:11:11.011', 'Beijing', 10, 1, '2020-01-01', '2020-01-01', '2017-10-01 11:11:11.170000', '2017-10-01 11:11:11.110111', '2020-01-01', 1, 30, 20)
            """
    sql """ INSERT INTO table_with_column_group_xxx VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.022', '2017-10-01 11:11:11.012', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.160000', '2017-10-01 11:11:11.100111', '2020-01-02', 1, 31, 19)
            """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.023', '2017-10-01 11:11:11.013', 'Beijing', 10, 1, '2020-01-02', '2020-01-02', '2017-10-01 11:11:11.150000', '2017-10-01 11:11:11.130111', '2020-01-02', 1, 31, 21)
        """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.024', '2017-10-01 11:11:11.014', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.140000', '2017-10-01 11:11:11.120111', '2020-01-03', 1, 32, 20)
        """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.025', '2017-10-01 11:11:11.015', 'Beijing', 10, 1, '2020-01-03', '2020-01-03', '2017-10-01 11:11:11.100000', '2017-10-01 11:11:11.140111', '2020-01-03', 1, 32, 22)
        """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.026', '2017-10-01 11:11:11.016', 'Beijing', 10, 1, '2020-01-04', '2020-01-04', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.150111', '2020-01-04', 1, 33, 21)
        """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.027', '2017-10-01 11:11:11.017', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
        """

    sql """ INSERT INTO table_with_column_group_xxx VALUES
         (4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.028', '2017-10-01 11:11:11.018', 'Beijing', 10, 1, NULL, NULL, NULL, NULL, '2020-01-05', 1, 34, 20)
        """

    // set server side prepared statement url
    connect(user = user, password = password, url = prepare_url) {
        def prep_sql = { sql_str, k ->
            def stmt = prepareStatement sql_str
            stmt.setInt(1, k)
            assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
            qe_point_select stmt
        }
        def sql_str = "select v1, v2 from table_with_column_group where k1 = ?"
        prep_sql sql_str, 1
        prep_sql sql_str, 2 
        prep_sql sql_str, 3
        sql_str = "select v2 from table_with_column_group where k1 = ?"
        prep_sql sql_str, 1
        prep_sql sql_str, 2 
        prep_sql sql_str, 3
        sql_str = "select v1 from table_with_column_group where k1 = ?"
        prep_sql sql_str, 3
        sql_str = "select v2, v1 from table_with_column_group where k1 = ?"
        prep_sql sql_str, 3

    
        sql_str = "select v2 from table_with_column_group where k1 = ?"
        prep_sql sql_str, 1

        sql_str = "select v2 from table_with_column_group2 where k1 = ?"
        prep_sql sql_str, 1
        prep_sql sql_str, 2
        prep_sql sql_str, 3

        sql_str = "select v4 from table_with_column_group3 where k1 = ?"
        prep_sql sql_str, 1
        prep_sql sql_str, 2
        prep_sql sql_str, 3

        def setPrepareStmtArgs = {stmt, user_id, date, datev2, datetimev2_1, datetimev2_2, city, age, sex ->
            java.text.SimpleDateFormat formater = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS");
            stmt.setInt(1, user_id)
            stmt.setDate(2, java.sql.Date.valueOf(date))
            stmt.setDate(3, java.sql.Date.valueOf(datev2))
            stmt.setTimestamp(4, new java.sql.Timestamp(formater.parse(datetimev2_1).getTime()))
            stmt.setTimestamp(5, new java.sql.Timestamp(formater.parse(datetimev2_2).getTime()))
            stmt.setString(6, city)
            stmt.setInt(7, age)
            stmt.setInt(8, sex)
        }

        def stmt = prepareStatement """ SELECT datetimev2_1,datetime_val1,datetime_val2,max_dwell_time FROM table_with_column_group_xxx t where user_id = ? and date = ? and datev2 = ? and datetimev2_1 = ? and datetimev2_2 = ? and city = ? and age = ? and sex = ?; """
        setPrepareStmtArgs stmt, 1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.21', '2017-10-01 11:11:11.11', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.22', '2017-10-01 11:11:11.12', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.23', '2017-10-01 11:11:11.13', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.24', '2017-10-01 11:11:11.14', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.25', '2017-10-01 11:11:11.15', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.26', '2017-10-01 11:11:11.16', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.27', '2017-10-01 11:11:11.17', 'Beijing', 10, 1
        qe_point_select stmt
        setPrepareStmtArgs stmt, 4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.28', '2017-10-01 11:11:11.18', 'Beijing', 10, 1
        qe_point_select stmt
    }

    sql "DROP TABLE IF EXISTS table_with_column_group4"
    sql """
         CREATE TABLE IF NOT EXISTS table_with_column_group4 (
                 `k1` int(11) NULL COMMENT "",
                 `v1` text NULL COMMENT "",
                 `v2` bigint NULL COMMENT "",
                 `v3` double NULL COMMENT "",
                 `v4` datetime NULL COMMENT ""
               ) ENGINE=OLAP
               UNIQUE KEY(`k1`)
               DISTRIBUTED BY HASH(`k1`) BUCKETS 1
               PROPERTIES (
               "replication_allocation" = "tag.location.default: 1",
               "enable_unique_key_merge_on_write" = "true",
               "light_schema_change" = "true",
               "row_store_columns" = "v4",
               "storage_format" = "V2"
               )
    """
    sql "select /*+ SET_VAR(enable_nereids_planner=true)*/ * from table_with_column_group where k1 = 1"

    def tableName = "rs_query"
    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql "set enable_decimal256 = true"
    sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL COMMENT "",
                `v1` text NULL COMMENT "",
                `v2` DECIMAL(50, 18) NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
          """

    sql """insert into ${tableName} values (1, 'abc', 1111919.12345678919)"""
    explain {
        sql("select * from ${tableName} order by k1 limit 1")
        contains "TOPN OPT"
    } 
    qt_sql """select * from ${tableName} order by k1 limit 1"""

    sql """
         ALTER table ${tableName} ADD COLUMN new_column1 INT default "123";
    """
    qt_sql """select * from ${tableName} where k1 = 1"""

    sql """
         ALTER table ${tableName} ADD COLUMN new_column2 DATETIMEV2(3) DEFAULT "1970-01-01 00:00:00.111";
    """
    sleep(1000)
    qt_sql """select * from ${tableName} where k1 = 1"""
   
    sql """insert into ${tableName} values (2, 'def', 1111919.12345678919, 456, NULL)"""
    qt_sql """select * from ${tableName} where k1 = 2"""

    sql "set global enable_short_circuit_query_access_column_store = false"
    test {
        sql "select * from table_with_column_group where k1 = 1"
        exception("Not support column store")
    }
    sql "set global enable_short_circuit_query_access_column_store = true"
}