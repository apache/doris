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

suite("test_sum") {
    qt_select """
                  select k1, sum(k5) over 
                      (partition by k1 order by k3 range between current row and unbounded following) as w 
                  from test_query_db.test order by k1, w
              """

    sql "create database if not exists multi_db"
    sql "use multi_db"
    sql "DROP TABLE IF EXISTS multi"
    sql """
        CREATE TABLE multi (
            id int,
            v1 int,
            v2 varchar
            ) ENGINE = OLAP
            DUPLICATE KEY(id) COMMENT 'OLAP'
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """ 
    sql """
        insert into multi values (1, 1, 'a'),(1, 1, 'a'), (2, 1, 'a'), (3, 1, 'a');
        """ 
    qt_sql_window_muti1 """   select multi_distinct_group_concat(v2) over() from multi; """
    qt_sql_window_muti2 """   select multi_distinct_sum(v1) over() from multi; """
    qt_sql_window_muti3 """   select multi_distinct_count(v1) over() from multi; """
}

