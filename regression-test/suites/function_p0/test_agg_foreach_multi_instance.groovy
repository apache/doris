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

suite("test_agg_foreach_multi_instance") {
   sql """
       drop table if exists foreach_table_m;
   """

	sql """
       CREATE TABLE IF NOT EXISTS foreach_table_m (
              `id` INT(11) null COMMENT "",
              `a` array<INT> null  COMMENT "",
              `b` array<array<INT>>  null COMMENT "",
              `s` array<String>  null  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into foreach_table values
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"]),
    (2,[20],[[2]],["cd"]),
    (3,[100],[[1]],["efg"]) , 
    (4,null,[null],null),
    (5,[null,2],[[2],null],[null,'c']);
   """
    qt_sql """select array_sort(array_flatten(array_agg_foreach(a))) from foreach_table;"""
   	qt_sql """select array_sort(array_flatten(array_agg_foreach(s))) from foreach_table;"""
}
