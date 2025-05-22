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

suite("test_array_min_max", "p0") {
    sql """ set enable_nereids_planner=true;"""
    sql """ set enable_fallback_to_original_planner=false;"""
    def tableName = "array_min_max_table"
    sql """
        drop table if exists ${tableName};
    """

    sql """
       CREATE TABLE IF NOT EXISTS ${tableName} (
              `id` INT(11) null COMMENT "",
              `a` array<INT> null  COMMENT "",
              `b` array<array<INT>>  null COMMENT "",
              `s` array<String>  null  COMMENT "",
              `d` array<boolean>  null  COMMENT ""
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
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"],[true,false,true]),
    (2,[20],[[2]],["cd"],[true,false,true]),
    (3,[100],[[1]],["efg"],[true,false,true]),
    (4,null,[null],null,null),
    (5,[null,2],[[2],null],[null,'c'],[true,false,true]);
   """

    qt_sql """
       select array_min(d), array_max(d) from ${tableName} order by id;
   """

   qt_sql """
       select array_min(a), array_min(s), array_max(a), array_max(s) from ${tableName} order by id;
   """


  
}
