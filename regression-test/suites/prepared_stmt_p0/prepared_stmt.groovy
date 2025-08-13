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
import com.mysql.cj.MysqlType;

suite("test_prepared_stmt", "nonConcurrent") {
    def config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_decimal_conversion'; """
    String old_value = config_row[0][1]
    sql """
        admin set frontend config("enable_decimal_conversion" = "true");
    """

    def tableName = "tbl_prepared_stmt"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    // def url = context.config.jdbcUrl + "&useServerPrepStmts=true&useCursorFetch=true"
    String url = getServerPrepareJdbcUrl(context.config.jdbcUrl, "regression_test_prepared_stmt_p0")
    logger.info("jdbc prepare statement url: ${url}")
    def result1 = connect(user, password, url) {

        sql """DROP TABLE IF EXISTS ${tableName} """
        sql """
             CREATE TABLE IF NOT EXISTS ${tableName} (
                     `k1` int(11) NULL COMMENT "",
                     `k2` decimalv3(27, 9) NULL COMMENT "",
                     `k3` varchar(30) NULL COMMENT "",
                     `k4` varchar(30) NULL COMMENT "",
                     `k5` date NULL COMMENT "",
                     `k6` datetime NULL COMMENT "",
                     `k7` float NULL COMMENT "",
                     `k8` datev2 NULL COMMENT "",
                     `k9` array<datetime> NULL COMMENT ""
                   ) ENGINE=OLAP
                   UNIQUE KEY(`k1`, `k2`, `k3`)
                   DISTRIBUTED BY HASH(`k1`, k2, k3) BUCKETS 1
                   PROPERTIES (
                   "replication_allocation" = "tag.location.default: 1",
                   "light_schema_change" = "true",
                   "storage_format" = "V2"
                   )
               """

        sql """ INSERT INTO ${tableName} VALUES(1231, 119291.11, "ddd", "laooq", null, "2020-01-01 12:36:38", null, "1022-01-01 11:30:38", "[2022-01-01 11:30:38, 2022-01-01 11:30:38, 2022-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1232, 12222.99121135, "xxx", "laooq", "2023-01-02", "2020-01-01 12:36:38", 522.762, "2022-01-01 11:30:38", "[2023-01-01 11:30:38, 2023-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1233, 1.392932911136, "yyy", "laooq", "2024-01-02", "2020-01-01 12:36:38", 52.862, "3022-01-01 11:30:38", "[2024-01-01 11:30:38, 2024-01-01 11:30:38, 2024-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1234, 12919291.129191137, "xxddd", "laooq", "2025-01-02", "2020-01-01 12:36:38", 552.872, "4022-01-01 11:30:38", "[2025-01-01 11:30:38, 2025-01-01 11:30:38, 2025-01-01 11:30:38]") """
        sql """ INSERT INTO ${tableName} VALUES(1235, 991129292901.11138, "dd", null, "2120-01-02", "2020-01-01 12:36:38", 652.692, "5022-01-01 11:30:38", "[]") """
        sql """ INSERT INTO ${tableName} VALUES(1236, 100320.11139, "laa    ddd", "laooq", "2220-01-02", "2020-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38", "[null]") """
        sql """ INSERT INTO ${tableName} VALUES(1237, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", "[2025-01-01 11:30:38]") """
        sql """sync"""

        qt_sql """select * from  ${tableName} order by 1, 2, 3"""
        qt_sql """select * from  ${tableName} order by 1, 2, 3"""
        sql "set global max_prepared_stmt_count = 10000"
        sql "set enable_fallback_to_original_planner = false"
        sql """set global enable_server_side_prepared_statement = true"""

        int count = 65536;
        StringBuilder sb = new StringBuilder();
        sb.append("?");
        for (int i = 1; i < count; i++) {
            sb.append(", ?");
        }
        String sqlWithTooManyPlaceholder = sb.toString();
        def stmt_read = prepareStatement "select * from ${tableName} where k1 in ${sqlWithTooManyPlaceholder}"
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read.class)

        stmt_read = prepareStatement "select * from ${tableName} where k1 = ? order by k1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 1231)
        qe_select0 stmt_read
        stmt_read.setInt(1, 1232)
        qe_select0 stmt_read
        qe_select0 stmt_read
        def stmt_read1 = prepareStatement "select hex(k3), ? \n from ${tableName} where k1 = ? order by 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read1.setString(1, "xxxx---")
        stmt_read1.setInt(2, 1231)
        qe_select1 stmt_read1
        stmt_read1.setString(1, "yyyy---")
        stmt_read1.setInt(2, 1232)
        qe_select1 stmt_read1
        qe_select1 stmt_read1
        stmt_read1.close()
        def stmt_read2 = prepareStatement "select * from ${tableName} as t1 join ${tableName} as t2 on t1.`k1` = t2.`k1` where t1.`k1` >= ? and t1.`k2` >= ? and size(t1.`k9`) > ? order by 1, 2, 3"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read2.setInt(1, 1237)
        stmt_read2.setBigDecimal(2, new BigDecimal("120939.11130"))
        stmt_read2.setInt(3, 0)
        qe_select2 stmt_read2
        qe_select2 stmt_read2
        qe_select2 stmt_read2
        stmt_read2.close()

        sql "DROP TABLE IF EXISTS mytable1"
        sql "DROP TABLE IF EXISTS mytable2"
        sql """
          CREATE TABLE mytable1
          (
              siteid INT DEFAULT '10',
              citycode SMALLINT,
              username VARCHAR(32) DEFAULT '',
              pv BIGINT SUM DEFAULT '0'
          )
          AGGREGATE KEY(siteid, citycode, username)
          DISTRIBUTED BY HASH(siteid) BUCKETS 10
          PROPERTIES("replication_num" = "1");
          """
    
        sql """insert into mytable1 values(1,1,'user1',10);"""
        sql """insert into mytable1 values(1,1,'user1',10);"""
        sql """insert into mytable1 values(1,1,'user1',10);"""
        sql "sync"

        stmt_read = prepareStatement "SELECT *, ? FROM (select *, ? from mytable1 where citycode = ?) AS `SpotfireCustomQuery1` WHERE 1 = 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 12345)
        stmt_read.setInt(2, 1234)
        stmt_read.setInt(3, 1)
        qe_select3 stmt_read
        stmt_read.close()

        stmt_read = prepareStatement "SELECT 10"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select4 stmt_read
        stmt_read = prepareStatement "SELECT 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select5 stmt_read
        stmt_read = prepareStatement "SELECT 'a' FROM mytable1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select5 stmt_read
        stmt_read.close()

        stmt_read = prepareStatement "SELECT 1-2 + ?" 
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 3);
        qe_select5 stmt_read
        stmt_read.setInt(1, -1);
        qe_select5 stmt_read
        stmt_read.close()

        stmt_read = prepareStatement "SELECT 1 + ? AS c1, 'MySQL' AS c2, ? AS c3"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 5) 
        stmt_read.setString(2, "Connector/J") 
        qe_select5 stmt_read

        stmt_read = prepareStatement "SELECT ?, ?, ?, ?, ?"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        boolean value = false;
        stmt_read.setBoolean(1, value);
        stmt_read.setObject(2, value);
        stmt_read.setObject(3, value, MysqlType.BOOLEAN);
        stmt_read.setObject(4, value, MysqlType.TINYINT);
        stmt_read.setObject(5, value, MysqlType.BIT);
        qe_select5 stmt_read

        sql """insert into mytable1 values(2,1,'user1',null);"""

        stmt_read = prepareStatement "SELECT *, ? FROM (select *, ? from mytable1 where pv is null) AS `SpotfireCustomQuery1` WHERE 1 = 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setString(1, "xxxlalala")
        stmt_read.setDouble(2, 1234.1111)
        qe_select6 stmt_read
        stmt_read.setString(1, "1111111")
        stmt_read.setString(2, "1111111")
        qe_select7 stmt_read
        // stmt_read.close()

        stmt_read.setString(1, "xxxlalala")
        stmt_read.setDouble(2, 1234.1111)
        qe_select6_1 stmt_read
        stmt_read.setString(1, "1111111")
        stmt_read.setString(2, "1111111")
        qe_select7_1 stmt_read

        stmt_read = prepareStatement "SELECT COUNT() from mytable1 WHERE citycode = ? GROUP BY siteid"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setString(1, "1")
        qe_select8 stmt_read
        stmt_read.close()

        stmt_read = prepareStatement "SELECT COUNT() from mytable1 WHERE citycode = ? GROUP BY ?"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setString(1, "1")
        stmt_read.setString(2, "1")
        qe_select9 stmt_read
        stmt_read.close()

        // multi statements
        // stmt_read = prepareStatement "SELECT 1 FROM mytable1;SELECT 1 FROM mytable1" 
        // assertEquals(stmt_read.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        // qe_select10 stmt_read
        // stmt_read.close()
        // stmt_read = prepareStatement "SELECT ? FROM mytable1;SELECT ? FROM mytable1 WHERE citycode = ?" 
        // assertEquals(stmt_read.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        // stmt_read.setString(1, "1")
        // stmt_read.setString(2, "1")
        // stmt_read.setString(3, "1")
        // qe_select11 stmt_read
        // stmt_read.close()

        // prepared stmt outof limit
        sql "set global max_prepared_stmt_count = 1"
        stmt_read = prepareStatement "SELECT 1" 
        qe_select13 stmt_read
        // assertEquals(stmt_read.class, com.mysql.cj.jdbc.ClientPreparedStatement);
        stmt_read = prepareStatement "SELECT 1" 
        // assertEquals(stmt_read.class, com.mysql.cj.jdbc.ClientPreparedStatement);
        // set back
        sql "set global max_prepared_stmt_count = 1000000"

        // limit
        stmt_read = prepareStatement "SELECT 1 LIMIT ?" 
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 1)
        qe_select14 stmt_read
        stmt_read = prepareStatement "SELECT 1 LIMIT 1"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select15 stmt_read

        // insert with label
        stmt_read = prepareStatement "insert into mytable1 with xxx_label 12222 values(?, ?, ?, ?)"
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read.class)
        // alter stmt
        stmt_read = prepareStatement "alter table mytable1 rename mytable2"
        def result = stmt_read.execute()
        logger.info("result: ${result}")
        // assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read.class)
        // update stmt
        stmt_read = prepareStatement "update tbl_prepared_stmt set k5 = ?"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setString(1, "2021-01-01")
        result = stmt_read.execute()
        logger.info("result: ${result}")
        stmt_read = prepareStatement "update tbl_prepared_stmt set k4 = 'Will we ignore LIMIT ?,?'"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        result = stmt_read.execute()
        logger.info("result: ${result}")
        qt_sql "select * from tbl_prepared_stmt where k4 = 'Will we ignore LIMIT ?,?' order by k1"
        // show create table
        stmt_read = prepareStatement "SHOW CREATE TABLE tbl_prepared_stmt"
        result = stmt_read.execute()
        logger.info("result: ${result}")
        // assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read.class)
        // not stable
        // qe_select16 stmt_read
        stmt_read.close()

        stmt_read = prepareStatement "SELECT connection_id()" 
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        result = stmt_read.execute()
        logger.info("connection_id: ${result}")
        // qe_select16 stmt_read

        // test prepared with between, test placeholder equal
        sql """insert into mytable2 values(3,1,'user1',10);"""
        stmt_read = prepareStatement "SELECT COUNT() from mytable2 WHERE siteid between ? and ?"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        stmt_read.setInt(1, 0)
        stmt_read.setInt(2, 3)
        qe_select17 stmt_read


        // test array1
        stmt_read = prepareStatement """SELECT 1, [1, 2, 3], null, ["1"], null, null, [1.111]"""
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select18 stmt_read

        // test array2
        stmt_read = prepareStatement "SELECT 1, [1, 2, 3], null"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select18_1 stmt_read

        // test array3
        stmt_read = prepareStatement """SELECT 1, [1, null, 3], null, [null], null, null, [null, null]"""
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select18_2 stmt_read

        // test map
        stmt_read = prepareStatement """SELECT 1, {"a" : 1}, null"""
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select19 stmt_read

        // test struct
        stmt_read = prepareStatement """SELECT 1, struct('a', 1, 'doris', 'aaaaa', 1.32), null"""
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select20 stmt_read

        // test nested array
        stmt_read = prepareStatement("""SELECT 1, [[1, 2], [3, 4]], null""")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select21 stmt_read

        // test nested map
        stmt_read = prepareStatement("""SELECT 1, {"a": {"b": 2}}, null""")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select22 stmt_read

        // test struct with array
        stmt_read = prepareStatement("""SELECT 1, struct('name', 'doris', 'values', [1, 2, 3]), null""")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select23 stmt_read

        stmt_read = prepareStatement("""SELECT 1, null, [{'id': 1, 'name' : 'doris'}, {'id': 2, 'name': 'apache'}, null], null""")
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read.class)
        qe_select24 stmt_read
    }

    // test stmtId overflow
    def result2 = connect(user, password, url) {
        // def stmt_read1 = prepareStatement "select 1"
        // assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read1.class)
        // qe_overflow_1 stmt_read1
        // stmt_read1.close()
        // int max
        sql """admin set frontend config("prepared_stmt_start_id" = "2147483647");"""
        def stmt_read2 = prepareStatement "select 2"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read2.class)
        qe_overflow_2 stmt_read2
        qe_overflow_2 stmt_read2
        stmt_read2.close()
        // int max + 1
        sql """admin set frontend config("prepared_stmt_start_id" = "2147483648");"""
        def stmt_read3 = prepareStatement "select 3"
        // overflow throw NumberFormatExceptio and fallback to ClientPreparedStatement
        assertEquals(com.mysql.cj.jdbc.ClientPreparedStatement, stmt_read3.class)
        qe_overflow_3 stmt_read3
        qe_overflow_3 stmt_read3
        stmt_read3.close()
        // int min 
        sql """admin set frontend config("prepared_stmt_start_id" = "2147483646");"""
        def stmt_read4 = prepareStatement "select 4"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read4.class)
        qe_overflow_4 stmt_read4
        qe_overflow_4 stmt_read4
        stmt_read4.close()

        sql """admin set frontend config("prepared_stmt_start_id" = "123");"""
        def stmt_read5 = prepareStatement "select 5"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read5.class)
        qe_overflow_5 stmt_read5
        qe_overflow_5 stmt_read5
        stmt_read5.close()

        // set back
        sql """admin set frontend config("prepared_stmt_start_id" = "-1");"""
        def stmt_read6 = prepareStatement "select 6"
        assertEquals(com.mysql.cj.jdbc.ServerPreparedStatement, stmt_read6.class)
        qe_overflow_6 stmt_read6
        qe_overflow_6 stmt_read6
        qe_overflow_6 stmt_read6
        stmt_read6.close()
    }

    // restore enable_decimal_conversion to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("enable_decimal_conversion" = "${old_value}"); """
}
