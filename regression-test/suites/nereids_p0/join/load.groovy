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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("load") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    def tables=["test_join", "full_join_table", "test_bucket_shuffle_join", "table_1", "table_2", "table_3", "left_table", "right_table"]

    for (String table in tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    sql """ insert into test_join values(1),(2),(3),(4),(5) """

    sql """ ALTER TABLE test_bucket_shuffle_join ADD PARTITION p202112 
            VALUES LESS THAN ("2022-01-01 00:00:00") DISTRIBUTED BY HASH(id) BUCKETS 2;"""
    sql """ insert into test_bucket_shuffle_join values(1, "2021-12-01 00:00:00"),
        (2, "2021-12-01 00:00:00"), (3, "2021-12-01 00:00:00")"""

    sql """ INSERT INTO full_join_table(x) VALUES (NULL) """;

    sql """ INSERT INTO table_2 VALUES ('H220427011909850160918','2022-04-27 16:00:33'),('T220427400109910160949','2022-04-27 16:00:54'),('T220427400123770120058','2022-04-27 16:00:56'),('T220427400126530112854','2022-04-27 16:00:34'),('T220427400127160144672','2022-04-27 16:00:10'),('T220427400127900184511','2022-04-27 16:00:34'),('T220427400129940120380','2022-04-27 16:00:23'),('T220427400139720192986','2022-04-27 16:00:34'),('T220427400140260152375','2022-04-27 16:00:02'),('T220427400153170104281','2022-04-27 16:00:31'),('H220427011909800104411','2022-04-27 16:00:14'),('H220427011909870184823','2022-04-27 16:00:36'),('T220427400115770144416','2022-04-27 16:00:12'),('T220427400126390112736','2022-04-27 16:00:19'),('T220427400128350120717','2022-04-27 16:00:56'),('T220427400129680120838','2022-04-27 16:00:39'),('T220427400136970192083','2022-04-27 16:00:51'),('H220427011909770192580','2022-04-27 16:00:04'),('H220427011909820192943','2022-04-27 16:00:23'),('T220427400109110184990','2022-04-27 16:00:29'),('T220427400109930192249','2022-04-27 16:00:56'),('T220427400123050168464','2022-04-27 16:00:37'),('T220427400124330112931','2022-04-27 16:00:56'),('T220427400124430144718','2022-04-27 16:00:07'),('T220427400130570160488','2022-04-27 16:00:34'),('T220427400130610112671','2022-04-27 16:00:30'),('T220427400137600160704','2022-04-27 16:00:35'),('T220427400144590176969','2022-04-27 16:00:49'),('T220427400146320176530','2022-04-27 16:00:34'),('T220427601780480120027','2022-04-27 16:00:58');"""

    sql """ INSERT INTO table_3 VALUES (1,1,'100','001-dd','44411@hotmail.com',1,0,3,4,'del'),(2,1,'100','001-dc','019992@hotmail.com',1,0,3,4,'del'),(3,1,'100','001-ok','019242@hotmail.com',2,0,1,2,'ins');"""

    sql """ INSERT INTO left_table values(1, "test") """;
}
