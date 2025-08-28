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

suite("eliminate_const_join_condition") {
    sql """
        drop table if exists eliminate_cost_join_condition_t1;
        create table eliminate_cost_join_condition_t1 (
            k int,
            v varchar(20)
        )ENGINE=OLAP
        duplicate KEY(k)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into eliminate_cost_join_condition_t1(k) values(1);

        drop table if exists eliminate_cost_join_condition_t2;
        create table eliminate_cost_join_condition_t2 (
            k int,
            v varchar(20)
        )ENGINE=OLAP
        duplicate KEY(k)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(k) BUCKETS 2
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        insert into eliminate_cost_join_condition_t2(k) values(1);

        set runtime_filter_mode=off;
        set enable_parallel_result_sink=false;
    """

    qt_shape"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t1.k=1;
    """

    qt_null_safe_equal """
        select * 
        from eliminate_cost_join_condition_t1 t1 join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t1.k=1;
        """
    
    qt_equal """
    select * 
    from eliminate_cost_join_condition_t1 t1 join eliminate_cost_join_condition_t2 t2
        on t1.k=t2.k and t1.v = t2.v where t1.k=1;
    """

    qt_shape_left_join"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 left join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t1.k=1;
    """

    qt_shape_right_join"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 right join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t2.k=1;
    """


    qt_shape_anti_join"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 left anti join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t1.k=1;
    """

    qt_shape_semi_join"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 left semi join eliminate_cost_join_condition_t2 t2
            on t1.k=t2.k and t1.v <=> t2.v where t1.k=1;
    """

    qt_shape_mark_join"""
        explain shape plan
        select * 
        from eliminate_cost_join_condition_t1 t1 
        where (exists (
                select * 
                from eliminate_cost_join_condition_t2 t2
                where t1.k=t2.k and t2.k=1) 
                or t1.k > 10 
              )
            and t1.k=1;
    """
}