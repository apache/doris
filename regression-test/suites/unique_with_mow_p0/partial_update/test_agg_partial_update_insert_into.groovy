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

suite("test_agg_partial_update_insert_into", "p0") {

    String db = context.config.getDbNameByFile(context.file)
    sql "select 1;" // to create database

    connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
        sql "use ${db};"
        sql "set enable_agg_key_partial_update=true;"
        sql "set enable_insert_strict = false;"
        sql "sync;"

        def tableName = "agg_partial_update_insert_into"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
                        `id` int(11) NOT NULL COMMENT "用户 ID",
                        `name` varchar(65533) REPLACE_IF_NOT_NULL NOT NULL COMMENT "用户姓名",
                        `score` int(11) REPLACE_IF_NOT_NULL NOT NULL COMMENT "用户得分",
                        `test` int(11) REPLACE_IF_NOT_NULL NULL COMMENT "null test",
                        `dft` int(11) REPLACE_IF_NOT_NULL DEFAULT "4321")
                        AGGREGATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES("replication_num" = "1"); """

        sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
        qt_1 """ select * from ${tableName} order by id; """
        sql "sync;"
        sql """insert into ${tableName}(id,score) values(2,400),(1,200),(4,400)"""
        qt_2 """ select * from ${tableName} order by id; """
        test {
            sql """insert into ${tableName} values(2,400),(1,200),(4,400)"""
            exception "Column count doesn't match value count"
        }
        sql """ DROP TABLE IF EXISTS ${tableName} """

        def tableName2 = "agg_partial_update_insert_into2"
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """create table ${tableName2} (
                k int null,
                v int replace_if_not_null null,
                v2 int replace_if_not_null null,
                v3 int replace_if_not_null null
            ) aggregate key (k) distributed by hash(k) buckets 1
            properties("replication_num" = "1"); """
        sql "insert into ${tableName2} values(1,1,3,4),(2,2,4,5),(3,3,2,3),(4,4,1,2);"
        qt_3 "select * from ${tableName2} order by k;"
        sql "insert into ${tableName2}(k,v) select v2,v3 from ${tableName2};"
        qt_4 "select * from ${tableName2} order by k;"
        sql """ DROP TABLE IF EXISTS ${tableName2}; """

        def tableName3 = "agg_partial_update_insert_into3"
        sql """ DROP TABLE IF EXISTS ${tableName3} """
        sql """create table ${tableName3} (
                k1 int null,
                k2 int null,
                k3 int null,
                v1 int replace_if_not_null null,
                v2 int replace_if_not_null null
            ) aggregate key (k1,k2,k3) distributed by hash(k1,k2) buckets 4
            properties("replication_num" = "1"); """
        sql "insert into ${tableName3} values(1,1,1,3,4),(2,2,2,4,5),(3,3,3,2,3),(4,4,4,1,2);"
        qt_5 "select * from ${tableName3} order by k1;"
        test {
            sql "insert into ${tableName3}(k1,k2,v2) select k2,k3,v1 from ${tableName3};"
            exception "illegal partial update"
        }
        qt_6 "select * from ${tableName3} order by k1;"
        sql """ DROP TABLE IF EXISTS ${tableName3}; """

        sql "set enable_agg_key_partial_update=false;"
        sql "set enable_insert_strict = true;"
        sql "sync;"
    }
}
