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

suite("test_create_dynamic_table", "dynamic_table") {
    def expect_result = { stmt, expect_success  -> 
       real_res = "success"
       try{
           sql stmt
       }catch(Exception ex){
           logger.info("create table catch exception: ${ex} stmt ${stmt}\n".toString())
           real_res = "false"
       }finally{
           assertEquals(expect_success, real_res)
           if(expect_success == "false"){
                logger.info("${stmt} expect fail")
                return
           }
       }
    }
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY HASH(y) BUCKETS 1 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int) ENGINE = Olap DUPLICATE KEY(x) DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int) ENGINE = Olap DUPLICATE KEY(x)  PARTITION BY RANGE(y) (partition `p1` values less than ("1000")) DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int) ENGINE = Olap DUPLICATE KEY(x)  PARTITION BY RANGE(x) (partition `p1` values less than ("1000")) DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int) ENGINE = Olap DUPLICATE KEY(x, y)  PARTITION BY RANGE(x, y) (partition `p1` values less than ("1000", "1000")) DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int, z array<int>) ENGINE = Olap DUPLICATE KEY(x, y) DISTRIBUTED BY HASH(x, y) BUCKETS 2 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success");
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
    expect_result("""CREATE TABLE dynamic_table_create_test (x int, y int, z array<date>) ENGINE = Olap DUPLICATE KEY(x, y)  PARTITION BY RANGE(x, y) (partition `p1` values less than ("1000", "1000")) DISTRIBUTED BY HASH(x, y) BUCKETS 2 PROPERTIES("replication_num" = "1", "deprecated_dynamic_schema" = "true");;""", "success")
    sql "DROP TABLE IF EXISTS dynamic_table_create_test"
}
