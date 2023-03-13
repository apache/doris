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

suite("test_prepared_stmt") {
    def tableName = "tbl_prepared_stmt"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
    // def url = context.config.jdbcUrl
    def result1 = connect(user=user, password=password, url=url) {
    def insert_prepared = { stmt, k1 , k2, k3, k4, k5, k6, k7, k8, k9 ->
        java.text.SimpleDateFormat formater = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (k1 == null) {
            stmt.setNull(1, java.sql.Types.INTEGER);
        } else {
            stmt.setInt(1, k1)
        }
        if (k2 == null) {
            stmt.setNull(2, java.sql.Types.DECIMAL);
        } else {
            stmt.setBigDecimal(2, k2)
        }
        if (k3 == null) {
            stmt.setNull(3, java.sql.Types.VARCHAR);    
        } else {
            stmt.setString(3, k3)
        }
        if (k4 == null) {
            stmt.setNull(4, java.sql.Types.VARCHAR);     
        } else {
            stmt.setString(4, k4)
        }
        if (k5 == null) {
            stmt.setNull(5, java.sql.Types.DATE);     
        } else {
            stmt.setDate(5, java.sql.Date.valueOf(k5))
        }
        if (k6 == null) {
            stmt.setNull(6, java.sql.Types.TIMESTAMP);     
        } else {
            stmt.setTimestamp(6, new java.sql.Timestamp(formater.parse(k6).getTime()))
        }
        if (k7 == null) {
            stmt.setNull(7, java.sql.Types.FLOAT);
        } else {
            stmt.setFloat(7, k7)
        }
        if (k8 == null) {
            stmt.setNull(8, java.sql.Types.DATE);
        } else {
            stmt.setTimestamp(8, new java.sql.Timestamp(formater.parse(k8).getTime()))
        }
        if (k9 == null) {
            stmt.setNull(9, java.sql.Types.VARCHAR);
        } else {
            stmt.setString(9, k9)
        }
        exec stmt
    }

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
              DUPLICATE KEY(`k1`, `k2`, `k3`)
              DISTRIBUTED BY HASH(`k1`, k2, k3) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "light_schema_change" = "true",
              "storage_format" = "V2"
              )
          """
      
      def insert_stmt = prepareStatement """ INSERT INTO ${tableName} VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) """
      assertEquals(insert_stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
      insert_prepared insert_stmt, 1231, 119291.11, "ddd", "laooq", null, "2020-01-01 12:36:38", null, "1022-01-01 11:30:38", "[2022-01-01 11:30:38, 2022-01-01 11:30:38, 2022-01-01 11:30:38]"
      insert_prepared insert_stmt, 1232, 12222.99121135, "xxx", "laooq", "2023-01-02", "2020-01-01 12:36:38", 522.762, "2022-01-01 11:30:38", "[2023-01-01 11:30:38, 2023-01-01 11:30:38]"
      insert_prepared insert_stmt, 1233, 1.392932911136, "yyy", "laooq", "2024-01-02", "2020-01-01 12:36:38", 52.862, "3022-01-01 11:30:38", "[2024-01-01 11:30:38, 2024-01-01 11:30:38, 2024-01-01 11:30:38]"
      insert_prepared insert_stmt, 1234, 12919291.129191137, "xxddd", "laooq", "2025-01-02", "2020-01-01 12:36:38", 552.872, "4022-01-01 11:30:38", "[2025-01-01 11:30:38, 2025-01-01 11:30:38, 2025-01-01 11:30:38]"
      insert_prepared insert_stmt, 1235, 991129292901.11138, "dd", null, "2120-01-02", "2020-01-01 12:36:38", 652.692, "5022-01-01 11:30:38", "[]"
      insert_prepared insert_stmt, 1236, 100320.11139, "laa    ddd", "laooq", "2220-01-02", "2020-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38", "[null]"
      insert_prepared insert_stmt, 1237, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38", "[2025-01-01 11:30:38]"

    qt_sql """select * from  ${tableName} order by 1, 2, 3"""
    qt_sql """select * from  ${tableName} order by 1, 2, 3"""

    def stmt_read = prepareStatement "select * from ${tableName} where k1 = ? order by k1"
    assertEquals(stmt_read.class, com.mysql.cj.jdbc.ServerPreparedStatement);
    stmt_read.setInt(1, 1231)
    qe_select0 stmt_read
    stmt_read.setInt(1, 1232)
    qe_select0 stmt_read
    qe_select0 stmt_read
    def stmt_read1 = prepareStatement "select hex(k3), ? from ${tableName} where k1 = ? order by 1"
    assertEquals(stmt_read1.class, com.mysql.cj.jdbc.ServerPreparedStatement);
    stmt_read1.setString(1, "xxxx---")
    stmt_read1.setInt(2, 1231)
    qe_select1 stmt_read1
    stmt_read1.setString(1, "yyyy---")
    stmt_read1.setInt(2, 1232)
    qe_select1 stmt_read1
    qe_select1 stmt_read1
      def stmt_read2 = prepareStatement "select * from ${tableName} as t1 join ${tableName} as t2 on t1.`k1` = t2.`k1` where t1.`k1` >= ? and t1.`k2` >= ? and size(t1.`k9`) > ? order by 1, 2, 3"
      assertEquals(stmt_read2.class, com.mysql.cj.jdbc.ServerPreparedStatement);
      stmt_read2.setInt(1, 1237)
      stmt_read2.setBigDecimal(2, new BigDecimal("120939.11130"))
      stmt_read2.setInt(3, 0)
      qe_select2 stmt_read2
      qe_select2 stmt_read2
      qe_select2 stmt_read2

      sql "DROP TABLE IF EXISTS mytable1"
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
     stmt_read = prepareStatement "SELECT *, ? FROM (select *, ? from mytable1 where citycode = ?) AS `SpotfireCustomQuery1` WHERE 1 = 1"
     stmt_read.setInt(1, 12345)
     stmt_read.setInt(2, 1234)
     stmt_read.setInt(3, 1)
     qe_select3 stmt_read
    }
}
