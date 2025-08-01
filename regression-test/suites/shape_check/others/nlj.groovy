/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("nlj") {
    sql 'set enable_parallel_result_sink=false;'
    sql """
    drop table if exists a;
    create table a (
        `id` int, 
        a_date dateV2,
        x int,
        y int
    )
    DISTRIBUTED BY HASH(id) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    alter table a modify column a_date set stats ('row_count'='1428418116', 'ndv'='498', 'min_value'='2023-12-31', 'max_value'='2025-05-18', 'avg_size'='4');


    drop table if exists b;
    create table b (
        `id` int, 
        b_date dateV2,
    )
    DISTRIBUTED BY HASH(b_date) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );


    alter table b modify column b_date set stats ('row_count'='505', 'ndv'='505', 'min_value'='2025-05-14', 'max_value'='2025-05-15', 'avg_size'='4');

    drop table if exists c;
    create table c (
        `id` int, 
        c_date dateV2,
    )
    DISTRIBUTED BY HASH(id) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    alter table c modify column c_date set stats ('row_count'='505', 'ndv'='505', 'min_value'='2025-05-14', 'max_value'='2025-05-15', 'avg_size'='4');
    
    set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
    set runtime_filter_mode=off;
    """

    // expected join order : a b
    // order b-a is good for aggregation, but usually this order is extremely bad for join.
    qt_shape """
    explain shape plan
    select b_date
    from a join b on a_date>b_date
    group by b_date, a_date, a.x
    """
}