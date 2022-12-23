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

suite("test_point_query") {
    def tableName = "tbl_point_query"
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def url = context.config.jdbcUrl + "&useServerPrepStmts=true"
    def result1 = connect(user=user, password=password, url=url) {
      sql """DROP TABLE IF EXISTS ${tableName}"""
      sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL COMMENT "",
                `k2` decimalv3(27, 9) NULL COMMENT "",
                `k3` text NULL COMMENT "",
                `k4` varchar(30) NULL COMMENT "",
                `k5` date NULL COMMENT "",
                `k6` datetime NULL COMMENT "",
                `k7` float NULL COMMENT "",
                `k8` datev2 NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`, `k2`)
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "storage_format" = "V2"
              )
          """
      sql """ INSERT INTO ${tableName} VALUES(1231, 119291.11, "ddd", "laooq", "2022-01-02", "2020-01-01 12:36:38", 19222.869, "1022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1232, 12222.99121135, "xxx", "laooq", "2023-01-02", "2020-01-01 12:36:38", 522.762, "2022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1233, 1.392932911136, "yyy", "laooq", "2024-01-02", "2020-01-01 12:36:38", 52.862, "3022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1234, 12919291.129191137, "xxddd", "laooq", "2025-01-02", "2020-01-01 12:36:38", 552.872, "4022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1235, 991129292901.11138, "dd", "laooq", "2120-01-02", "2020-01-01 12:36:38", 652.692, "5022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1236, 100320.11139, "laa    ddd", "laooq", "2220-01-02", "2020-01-01 12:36:38", 2.7692, "6022-01-01 11:30:38") """
      sql """ INSERT INTO ${tableName} VALUES(1237, 120939.11130, "a    ddd", "laooq", "2030-01-02", "2020-01-01 12:36:38", 22.822, "7022-01-01 11:30:38") """

      def stmt = prepareStatement "select * from ${tableName} where k1 = ? and k2 = ?"
      stmt.setInt(1, 1231)
      stmt.setBigDecimal(2, new BigDecimal("119291.11"))
      qe_point_select stmt
      stmt.setInt(1, 1231)
      stmt.setBigDecimal(2, new BigDecimal("119291.11"))
      qe_point_select stmt
      stmt.setInt(1, 1237)
      stmt.setBigDecimal(2, new BigDecimal("120939.11130"))
      qe_point_select stmt

      stmt = prepareStatement "select * from ${tableName} where k1 =  1235 and k2 = ?"
      stmt.setBigDecimal(1, new BigDecimal("991129292901.11138"))
      qe_point_select stmt

      def stmt_fn = prepareStatement "select hex(k3), hex(k4) from ${tableName} where k1 = ? and k2 =?"
      stmt_fn.setInt(1, 1231)
      stmt_fn.setBigDecimal(2, new BigDecimal("119291.11"))
      qe_point_select stmt_fn 
    }
}