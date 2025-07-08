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

suite('multi_distinct_window') {
    sql """
    drop table if exists multi;
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

    insert into multi values (1, 2, 'a'),(1, 2, 'a'), (2, 2, 'a'), (3, 2, 'a');
    """

    qt_count "select id, count(distinct v1) over() from multi order by id;"

    qt_count_partition "select id, v1, count(distinct v1) over(partition by id) from multi order by id;"

    qt_sum "select id, v1, sum(distinct v1) over() from multi order by id;"

    qt_sum_partition "select id, v1, sum(distinct v1) over(partition by id) from multi order by id;"

    qt_distinct_group_concat "select id, v1, group_concat(distinct v2) over() from multi order by id;"

}