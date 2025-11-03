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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("test_and_or") {
    def tableName = "test_and_or"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE `test_and_or` (
            `id` int(11) NULL,
            `k_null` boolean NULL,
            `k_true` boolean NULL,
            `k_false` boolean NULL,
            `k_true_not` boolean not NULL,
            `k_false_not` boolean not NULL,
            `k_true_false` boolean not NULL,
            `k_true_false_not` boolean not NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
        """

    sql """ insert into test_and_or values(1,NULL,true,false,true,false,true,true);"""
    sql "insert into test_and_or values(2,NULL,true,false,true,false,false,false);"
    sql "insert into test_and_or values(3,NULL,true,false,true,false,false,true);"
    sql "insert into test_and_or values(4,NULL,true,false,true,false,true,false);"
    qt_select1 """select * from test_and_or order by 1,2,3;"""
    qt_select3 """ select (k_false_not and non_nullable(k_false)) and (k_null or k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_null and k_false) and (k_null or k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_null or k_true) and (k_false_not or k_true_not) from test_and_or order by 1; """
    qt_select3 """ select (k_true_not or k_false_not) and (k_null or k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_null or k_false) and (k_false_not or k_true_not and k_true_false_not) from test_and_or order by 1; """
    qt_select3 """ select (k_null or k_false) and (k_false_not or k_true_not and k_false) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false_not or k_true_false) and (k_true_not or k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_null or k_false) and (k_false_not or k_true_not) from test_and_or order by 1; """
    qt_select3 """ select (k_true_not and k_true_false_not or k_true_false) and (k_false_not or k_true_false_not) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false or k_false) and (k_false_not or k_true_false_not) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false_not and k_true_not) or (k_false and k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false_not and k_true_not) or (k_false and k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false and k_false) or (k_false_not and k_true_false_not) from test_and_or order by 1; """
    qt_select3 """ select (k_false_not and k_true_not) or (k_null and k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_false_not and k_true or k_true_false_not) or (k_true_not and non_nullable(k_true)) from test_and_or order by 1; """
    qt_select3 """ select (k_false_not and k_true_not or k_true_false_not) or (k_true_not and k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false_not and k_true_not) or (k_false and k_true) from test_and_or order by 1; """
    qt_select3 """ select (k_null and k_true) or (k_false_not and k_false) from test_and_or order by 1; """
    qt_select3 """ select (k_false_not and k_true_not or k_true_false) or (k_true_not and k_true_false_not) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false and k_true_false_not) or (k_false_not and k_true_not or k_false) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false and k_true_false_not) or (k_false_not and k_true_not or k_false) from test_and_or order by 1; """
    qt_select3 """ select (k_true_false and k_true_false_not) or (cast(id as boolean) and (k_true_false_not)) from test_and_or order by 1; """

    qt_select1 """ select (k_false_not and k_false_not) and (k_null or k_true) from test_and_or order by 1;"""
    qt_select2 """ select (k_null and k_false) and (k_null or k_true) from test_and_or order by 1;"""
    qt_select3 """ select (k_null or k_true) and (k_false_not or k_true_not) from test_and_or order by 1;"""
    qt_select4 """ select (k_true_not or k_false_not) and (k_null or k_true) from test_and_or order by 1;"""
    qt_select5 """ select (k_true_false_not or k_true_false) and (k_true_not or k_false_not) from test_and_or order by 1;"""
    qt_select6 """ select (k_true_not or k_true_false) and (k_true_false_not or k_false_not) from test_and_or order by 1;"""
    qt_select7 """ select (k_true_not and k_false_not) and (k_true_false_not or k_true) from test_and_or order by 1;"""
    qt_select8 """ select (k_null or k_false) and (k_false_not or k_true_not) from test_and_or order by 1; """
    qt_select9 """ select (k_false and k_true_not) or (k_true and k_true_false_not) from test_and_or order by 1;"""
    qt_select10 """ select (k_false_not and k_true_not) or (k_null and k_false) from test_and_or order by 1;"""
    qt_select11 """ select (k_false_not and k_false_not) or (k_null and k_true) from test_and_or order by 1;"""
    qt_select12 """ select (k_null and k_false) or (k_null and k_true) from test_and_or order by 1;"""
    qt_select13 """ select (k_null and k_true) or (k_false_not and k_true_not) from test_and_or order by 1;"""
    qt_select14 """ select (k_true_not and k_false_not) or (k_null and k_true) from test_and_or order by 1;"""
    qt_select15 """ select (k_true_false_not and k_true_false) or (k_true_not and k_false_not) from test_and_or order by 1;"""
    qt_select16 """ select (k_true_not and k_true_false) or(k_true_false_not and k_false_not) from test_and_or order by 1;"""
    qt_select17 """ select (k_true_not and k_false_not) or (k_true_false_not and k_true) from test_and_or order by 1;"""
    qt_select18 """ select (k_null and k_false) or (k_false_not and k_true_not) from test_and_or order by 1; """
}
