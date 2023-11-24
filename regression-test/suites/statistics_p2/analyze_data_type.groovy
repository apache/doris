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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.

suite("test_auto_analyze", "analyze_legal_col_data_type") {
    def database_name = "state_test"
    def table_name_agg = "analyze_col_data_type_agg"
    def table_name_uniq = "analyze_col_data_type_uniq"
    def table_name_dup = "analyze_col_data_type_dup"
    sql """set enable_agg_state=true;"""

    sql """create database if not exists ${database_name}"""
    sql """use ${database_name}"""
    // agg表不接受复杂类型array，map，struct
    sql """CREATE TABLE `analyze_col_data_type_agg` (
            `k1` bigint(20) not NULL,
            `c_bool` boolean REPLACE_IF_NOT_NULL NULL,
            `c_tinyint` tinyint(4) REPLACE_IF_NOT_NULL NULL,
            `c_smallint` smallint(6) REPLACE_IF_NOT_NULL NULL,
            `c_int` int(11) REPLACE_IF_NOT_NULL NULL,
            `c_bigint` bigint(20) REPLACE_IF_NOT_NULL NULL,
            `c_largeint` largeint(40) REPLACE_IF_NOT_NULL NULL,
            `c_float` float REPLACE_IF_NOT_NULL NULL,
            `c_double` double REPLACE_IF_NOT_NULL NULL,
            `c_decimal` decimal(20, 3) REPLACE_IF_NOT_NULL NULL,
            `c_decimalv3` decimalv3(20, 3) REPLACE_IF_NOT_NULL NULL,
            `c_date` date REPLACE_IF_NOT_NULL NULL,
            `c_datetime` datetime REPLACE_IF_NOT_NULL NULL,
            `c_datev2` datev2 REPLACE_IF_NOT_NULL NULL,
            `c_datetimev2` datetimev2(0) REPLACE_IF_NOT_NULL NULL,
            `c_char` char(15) REPLACE_IF_NOT_NULL NULL,
            `c_varchar` varchar(100) REPLACE_IF_NOT_NULL NULL,
            `c_string` text REPLACE_IF_NOT_NULL NULL,
            `c_bitmap` bitmap BITMAP_UNION NOT NULL,
            `c_hll` HLL HLL_UNION NOT NULL,
            `c_qs` QUANTILE_STATE QUANTILE_UNION,
            `c_json` json REPLACE_IF_NOT_NULL NULL,
            `c_as` agg_state group_concat(string)
        ) ENGINE=OLAP
        AGGREGATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1"); """

    sql """insert into analyze_col_data_type_agg values (872139848948998830, false, 1, 1234, 1234, 87213984894899883, 87213984894899883011345, 45.3, 984.0, 37.001, 83.090, "2003-12-31", "2003-12-31 01:02:03", "2003-12-31", "2003-12-31 01:02:03", "kjoif", "iioihjhf", "oiqhohrv", to_bitmap(243), HLL_HASH('abc'), to_quantile_state(0, 2048), '{"k1":"jjj", "k2": 200}', group_concat_state('ccc'))"""
//    sql """analyze table analyze_col_data_type_agg with sync;"""

    // unique表不支持bitmap，HLL_UNION，QUANTILE_STATE，agg_state
    sql """CREATE TABLE `${table_name_uniq}` (
            `k1` bigint(20) not NULL,
            `c_bool` boolean not NULL,
            `c_tinyint` tinyint(4) not NULL,
            `c_smallint` smallint(6) not NULL,
            `c_int` int(11) not NULL,
            `c_bigint` bigint(20) not NULL,
            `c_largeint` largeint(40) not NULL,
            `c_float` float not NULL,
            `c_double` double not NULL,
            `c_decimal` decimal(20, 3) not NULL,
            `c_decimalv3` decimalv3(20, 3) not NULL,
            `c_date` date not NULL,
            `c_datetime` datetime not NULL,
            `c_datev2` datev2 not NULL,
            `c_datetimev2` datetimev2(0) not NULL,
            `c_char` char(15) not NULL,
            `c_varchar` varchar(100) not NULL,
            `c_string` text not NULL,
            `c_array` array<int(11)> not NULL,
            `c_map` MAP<text,int(11)> NULL,
            `c_struct` STRUCT<s_id:int(11),s_name:text,s_address:text> not NULL,
            `c_json` json not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1"); """

    sql """insert into ${table_name_uniq} values (872139848948998830, false, 1, 1234, 1234, 87213984894899883, 87213984894899883011345, 45.3, 984.0, 37.001, 83.090, "2003-12-31", "2003-12-31 01:02:03", "2003-12-31", "2003-12-31 01:02:03", "kjoif", "iioihjhf", "oiqhohrv", [1,2,3,4,5], {'a': 100, 'b': 200}, {1, 'sn1', 'sa1'}, '{"k1":"jjj", "k2": 200}')"""
//    sql """analyze table ${table_name_uniq} with sync;"""

    // dup表不支持bitmap，HLL_UNION，QUANTILE_STATE, agg_state
    sql """CREATE TABLE `${table_name_dup}` (
            `k1` bigint(20) not NULL,
            `c_bool` boolean not NULL,
            `c_tinyint` tinyint(4) not NULL,
            `c_smallint` smallint(6) not NULL,
            `c_int` int(11) not NULL,
            `c_bigint` bigint(20) not NULL,
            `c_largeint` largeint(40) not NULL,
            `c_float` float not NULL,
            `c_double` double not NULL,
            `c_decimal` decimal(20, 3) not NULL,
            `c_decimalv3` decimalv3(20, 3) not NULL,
            `c_date` date not NULL,
            `c_datetime` datetime not NULL,
            `c_datev2` datev2 not NULL,
            `c_datetimev2` datetimev2(0) not NULL,
            `c_char` char(15) not NULL,
            `c_varchar` varchar(100) not NULL,
            `c_string` text not NULL,
            `c_array` array<int(11)> not NULL,
            `c_map` MAP<text,int(11)> NULL,
            `c_struct` STRUCT<s_id:int(11),s_name:text,s_address:text> not NULL,
            `c_json` json not NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES("replication_num" = "1"); """

    sql """insert into ${table_name_dup} values (872139848948998830, false, 1, 1234, 1234, 87213984894899883, 87213984894899883011345, 45.3, 984.0, 37.001, 83.090, "2003-12-31", "2003-12-31 01:02:03", "2003-12-31", "2003-12-31 01:02:03", "kjoif", "iioihjhf", "oiqhohrv", [1,2,3,4,5], {'a': 100, 'b': 200}, {1, 'sn1', 'sa1'}, '{"k1":"jjj", "k2": 200}')"""
//    sql """analyze table ${table_name_dup} with sync;"""

    def auto_analyze_data_check = {
        def max_wait_time_in_minutes = 11 * 60
        def waiting_time = 0
        while (waiting_time < max_wait_time_in_minutes) {
            def sleep_time_num = 1 * 60
            sleep(sleep_time_num * 1000)
            waiting_time += sleep_time_num
            def i = 0;
            for (i = 0; i < 3; i++) {
                def r = sql """show table stats ${table_list[i]};"""
                if (r == [] || r.size() == 0) {
                    break
                }
            }
            if (i == 3) {
                break
            }

        }
        if (waiting_time >= max_wait_time_in_minutes) {
            logger.info("auto analyze failed due to time out")
            return false
        }
        return true
    }


    assertTrue(auto_analyze_data_check())

}
