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

suite("group_unique_array") {
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
    sql "drop table if exists test_group_unique_array_table"
    sql """
    CREATE TABLE IF NOT EXISTS `test_group_unique_array_table` (
        k1 int,
        k2 date,
        k3 varchar
        ) ENGINE=OLAP
    DUPLICATE KEY(k1)
    DISTRIBUTED BY HASH(k1) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2"
    )
    """
    sql """
        insert into test_group_unique_array_table values 
        (1, "2023-01-01", "hello"),
        (2, "2023-01-01", null),
        (2, "2023-01-02", "hello"),
        (3, null, "world"),
        (3, "2023-01-02", "hello"),
        (4, "2023-01-02", "doris"),
        (4, "2023-01-03", "sql")
        """
    qt_1 """
    select collect_set(k1),collect_set(k1,2) from test_group_unique_array_table;
    """
    qt_2 """
    select k1,collect_set(k2),collect_set(k3,1) from test_group_unique_array_table group by k1 order by k1;
    """
    qt_3 """
    select collect_set(k3) over() from test_group_unique_array_table;
    """
}