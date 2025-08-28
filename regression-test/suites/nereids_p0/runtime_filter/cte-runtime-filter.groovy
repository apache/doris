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

suite('cte-runtime-filter') {
    
    sql '''
    drop table if exists cte_runtime_filter_table;
    CREATE TABLE `cte_runtime_filter_table` (
    `part_dt` bigint NOT NULL COMMENT '日期',
    `group_id` bigint NOT NULL COMMENT '客群id',
    `user_id` bigint NOT NULL COMMENT '用户id'
    ) ENGINE=OLAP
    UNIQUE KEY(`part_dt`, `group_id`, `user_id`)
    DISTRIBUTED BY HASH(`user_id`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );

    insert into cte_runtime_filter_table values (1, 1, 1);

    set inline_cte_referenced_threshold=0;
    set disable_join_reorder = true;
    set enable_runtime_filter_prune=false;
    set runtime_filter_mode=global;
    set runtime_filter_type=2;
    '''
    
    qt_shape_onerow '''
        explain shape plan
        with cte as ((select 1 as id))
        select * 
        from cte a
        join cte_runtime_filter_table b on a.id=b.user_id ;
    '''

    qt_exec_onerow'''
        with cte as ((select 1 as id))
        select * 
        from cte a
        join cte_runtime_filter_table b on a.id=b.user_id ;
    '''

    qt_shape_cte_cte '''
        explain shape plan
        with cte as ((select * from cte_runtime_filter_table))
        select * 
        from cte a
        join cte_runtime_filter_table b on a.user_id=b.user_id ;
        '''

    qt_exec_cte_cte '''
        with cte as ((select * from cte_runtime_filter_table))
        select * 
        from cte a
        join cte_runtime_filter_table b on a.user_id=b.user_id ;
        '''
}