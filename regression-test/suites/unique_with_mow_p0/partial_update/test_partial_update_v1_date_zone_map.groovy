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

// A partial update that re-inserts a deleted key fills the NOT NULL columns it does not mention,
// and have no default, with the type's default value. The zone map of such a V1 DATE / DATETIME
// column must bound that value, or the predicates below prune the row away (or keep it wrongly).
suite("test_partial_update_v1_date_zone_map") {
    sql """ DROP TABLE IF EXISTS test_partial_update_v1_date_zone_map """
    sql """ CREATE TABLE test_partial_update_v1_date_zone_map (
            `k` INT NOT NULL,
            `v` INT NULL,
            `d` DATEV1 NOT NULL,
            `dt` DATETIMEV1 NOT NULL
        ) UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        ); """

    sql """ INSERT INTO test_partial_update_v1_date_zone_map VALUES (1, 1, '2020-01-01', '2020-01-01 00:00:00') """
    sql """ DELETE FROM test_partial_update_v1_date_zone_map WHERE k = 1 """
    sql "set enable_unique_key_partial_update = true"
    sql "set enable_insert_strict = false"
    sql """ INSERT INTO test_partial_update_v1_date_zone_map (k, v) VALUES (1, 2) """
    sql "set enable_unique_key_partial_update = false"
    sql "set enable_insert_strict = true"

    order_qt_all """ SELECT * FROM test_partial_update_v1_date_zone_map """
    order_qt_d_eq """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE d = '1970-01-01' """
    order_qt_d_gt """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE d > '1900-01-01' """
    order_qt_d_lt """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE d < '1970-01-01' """
    order_qt_dt_eq """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE dt = '1970-01-01 00:00:00' """
    order_qt_dt_gt """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE dt > '1900-01-01 00:00:00' """
    order_qt_dt_lt """ SELECT k FROM test_partial_update_v1_date_zone_map WHERE dt < '1970-01-01 00:00:00' """
}
