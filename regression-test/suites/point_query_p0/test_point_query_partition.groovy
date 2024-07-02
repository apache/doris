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

suite("test_point_query_partition") {
    def user = context.config.jdbcUser
    def password = context.config.jdbcPassword
    def realDb = "regression_test_serving_p0"
    def tableName = realDb + ".tbl_point_query_partition"
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
            sql sql_str
        }
    }

    sql """DROP TABLE IF EXISTS ${tableName}"""
    sql """
              CREATE TABLE IF NOT EXISTS ${tableName} (
                `k1` int(11) NULL COMMENT "",
                `value` text NULL COMMENT ""
              ) ENGINE=OLAP
              UNIQUE KEY(`k1`)
              PARTITION BY RANGE(`k1`)
              (
                  PARTITION `p1` VALUES LESS THAN ("1"),
                  PARTITION `p2` VALUES LESS THAN ("10"),
                  PARTITION `p3` VALUES LESS THAN ("30"),
                  PARTITION `p4` VALUES LESS THAN ("40"),
                  PARTITION `p5` VALUES LESS THAN ("1000")
              )
              DISTRIBUTED BY HASH(`k1`) BUCKETS 1
              PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "store_row_column" = "true",
              "enable_unique_key_merge_on_write" = "true",
              "light_schema_change" = "true",
              "storage_format" = "V2")
              """

    sql """INSERT INTO ${tableName} VALUES (1, 'a')"""
    sql """INSERT INTO ${tableName} VALUES (2, 'b')"""
    sql """INSERT INTO ${tableName} VALUES (-1, 'c')"""
    sql """INSERT INTO ${tableName} VALUES (11, 'd')"""
    sql """INSERT INTO ${tableName} VALUES (15, 'e')"""
    sql """INSERT INTO ${tableName} VALUES (33, 'f')"""
    sql """INSERT INTO ${tableName} VALUES (45, 'g')"""
    sql """INSERT INTO ${tableName} VALUES (999, 'h')"""
    def result1 = connect(user=user, password=password, url=prepare_url) {
        def stmt = prepareStatement "select * from ${tableName} where k1 = ?"
        assertEquals(stmt.class, com.mysql.cj.jdbc.ServerPreparedStatement);
        stmt.setInt(1, 1)
        qe_point_select stmt
        stmt.setInt(1, 2)
        qe_point_select stmt
        stmt.setInt(1, 11)
        qe_point_select stmt
        stmt.setInt(1, -1)
        qe_point_select stmt
        stmt.setInt(1, 11)
        qe_point_select stmt
        stmt.setInt(1, 12)
        qe_point_select stmt
        stmt.setInt(1, 34)
        qe_point_select stmt
        stmt.setInt(1, 33)
        qe_point_select stmt
        stmt.setInt(1, 45)
        qe_point_select stmt
        stmt.setInt(1, 666)
        qe_point_select stmt
        stmt.setInt(1, 999)
        qe_point_select stmt
        stmt.setInt(1, 1000)
        qe_point_select stmt
    }

    sql "DROP TABLE IF EXISTS regression_test_serving_p0.customer";
    sql """
        CREATE TABLE regression_test_serving_p0.customer (
          `customer_key` BIGINT NULL,
          `customer_value_0` TEXT NULL,
          `customer_value_1` TEXT NULL,
          `customer_value_2` TEXT NULL,
          `customer_value_3` TEXT NULL,
          `customer_value_4` TEXT NULL,
          `customer_value_5` TEXT NULL,
          `customer_value_6` TEXT NULL,
          `customer_value_7` TEXT NULL,
          `customer_value_8` TEXT NULL,
          `customer_value_10` TEXT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`customer_key`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`customer_key`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "store_row_column" = "true"
        ); 
    """  
    sql """insert into regression_test_serving_p0.customer(customer_key, customer_value_0, customer_value_1) values(686612, "686612", "686612")"""
    sql """insert into regression_test_serving_p0.customer(customer_key, customer_value_0, customer_value_1) values(686613, "686613", "686613")"""
    def result3 = connect(user=user, password=password, url=prepare_url) {
        def stmt = prepareStatement "select /*+ SET_VAR(enable_nereids_planner=true) */ * from regression_test_serving_p0.customer where customer_key = ?"
        stmt.setInt(1, 686612)
        qe_point_selectxxx stmt 
        qe_point_selectyyy stmt 
        qe_point_selectzzz stmt 
        stmt.setInt(1, 686613)
        qe_point_selectmmm stmt 
        qe_point_selecteee stmt 
    }
} 