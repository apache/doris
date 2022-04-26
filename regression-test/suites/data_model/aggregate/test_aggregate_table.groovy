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

suite("test_aggregate_table", "data_model") {
    def dbName = "test_aggregate_db"
    List<List<Object>> db = sql """show databases like '${dbName}'"""
    if (db.size() == 0) {
        sql """CREATE DATABASE ${dbName}"""
    }

    sql """use ${dbName}"""

    sql """DROP TABLE IF EXISTS int_agg"""
    sql """
            CREATE TABLE IF NOT EXISTS int_agg (
                k int,
                int_value_sum int sum,
                int_value_max int max,
                int_value_min int min,
                int_value_replace int replace,
                int_value_replace_if_not_null int replace_if_not_null
            )
            AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """insert into int_agg values(0, 1, 1, 1, 1, 1)"""
    sql """insert into int_agg values(0, 2, 2, 2, 2, 2)"""
    sql """insert into int_agg values(0, null, null, null, null, null)"""
    qt_int_agg_table """select * from int_agg"""
    qt_desc_date_table """desc int_agg"""
    sql """DROP TABLE int_agg"""

    sql """DROP TABLE IF EXISTS string_agg"""
    sql """
            CREATE TABLE IF NOT EXISTS string_agg (
                k int,
                char_value_max char(10) max,
                char_value_min char(10) min,
                char_value_replace char(10) replace,
                char_value_replace_if_not_null char(10) replace_if_not_null
            )
            AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """insert into string_agg values(0, '1', '1', '1', '1')"""
    sql """insert into string_agg values(0, '2', '2', '2', '2')"""
    sql """insert into string_agg values(0, '', '', '', '')"""
    sql """insert into string_agg values(0, null, null, null, null)"""
    qt_string_agg_table """select * from string_agg"""
    qt_desc_string_table """desc string_agg"""
    sql """DROP TABLE string_agg"""

    sql """DROP TABLE IF EXISTS date_agg"""
    sql """
            CREATE TABLE IF NOT EXISTS date_agg (
                k int,
                date_value_max date max,
                date_value_min date min,
                date_value_replace date replace,
                date_value_replace_if_not_null date replace_if_not_null
            )
            AGGREGATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """insert into date_agg values(0, '2000-01-01', '2000-01-01', '2000-01-01', '2000-01-01')"""
    sql """insert into date_agg values(0, '2000-12-31', '2000-12-31', '2000-12-31', '2000-12-31')"""
    sql """insert into date_agg values(0, null, null, null, null)"""
    qt_date_agg_table """select * from date_agg"""
    qt_desc_date_table """desc date_agg"""
    sql """DROP TABLE date_agg"""

    // sql "drop database ${dbName}"
}
