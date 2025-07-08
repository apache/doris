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

suite("test_array_agg", "p0") {
    sql """ set enable_nereids_planner=true;"""
    sql """ set enable_fallback_to_original_planner=false;"""
    def tableName = "array_agg_table"
    sql """
        drop table if exists ${tableName};
    """

    sql """ set enable_decimal256 = true """
    sql """
       CREATE TABLE IF NOT EXISTS ${tableName} (
              `id` INT(11) null COMMENT "",
              `a` array<INT> null  COMMENT "",
              `b` array<array<INT>>  null COMMENT "",
              `s` array<String>  null  COMMENT "",
              `d` array<decimal(50,18)>  null  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into ${tableName} values
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"],[1.1,2.2,3.3]),
    (2,[20],[[2]],["cd"],[3.3]),
    (3,[100],[[1]],["efg"],[null,2.2,3.3]),
    (4,null,[null],null,null),
    (5,[null,2],[[2],null],[null,'c'],[1,null,3.3]);
   """

   qt_sql """
       select array_min(a), array_min(s), array_max(a), array_max(s) from ${tableName} order by id;
   """

   qt_sql """
       select array_join(a, ',', 'replaced'), array_join(s, ',', 'replaced') from ${tableName} order by id;
   """

    qt_sql """ select array_sum(s) from ${tableName} order by id; """

    qt_sql """ select array_avg(s) from ${tableName} order by id; """

    qt_sql """ select array_product(s) from ${tableName} order by id; """
}
