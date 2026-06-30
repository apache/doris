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

suite("rec_cte_parallel_targets", "rec_cte") {
    sql """set enable_local_shuffle=true"""
    sql """set ignore_storage_data_distribution=true"""
    sql """set parallel_pipeline_task_num=4"""

    sql """drop table if exists rec_cte_parallel_targets"""
    sql """
        create table rec_cte_parallel_targets (
            id int,
            parent_id int,
            dept_name varchar(50),
            budget decimal(18, 2)
        )
        duplicate key(id)
        distributed by hash(id) buckets 4
        properties ("replication_num" = "1")
    """

    sql """
        insert into rec_cte_parallel_targets values
        (1, null, 'headquarters', 10000.00),
        (10, 1, 'r_and_d', 5000.00),
        (11, 1, 'marketing', 4000.00),
        (101, 10, 'backend', 2000.00),
        (102, 10, 'frontend', 1500.00),
        (111, 11, 'promotion', 2000.00)
    """

    def result = sql """
        with recursive cte(curr_id, total_score, step_path) as (
            select
                id,
                cast(budget as double),
                cast(dept_name as char(200))
            from rec_cte_parallel_targets
            where parent_id is null

            union all

            select
                cast(t.id as int),
                cast(c.total_score + t.budget as double),
                cast(concat(c.step_path, '->', t.dept_name) as char(200))
            from rec_cte_parallel_targets t
            inner join cte c on t.parent_id = c.curr_id
            where c.total_score < 50000
        )
        select curr_id, cast(cast(total_score as decimal(18, 2)) as string), step_path
        from cte
        order by curr_id
    """

    assertEquals([
        [1, "10000.00", "headquarters"],
        [10, "15000.00", "headquarters->r_and_d"],
        [11, "14000.00", "headquarters->marketing"],
        [101, "17000.00", "headquarters->r_and_d->backend"],
        [102, "16500.00", "headquarters->r_and_d->frontend"],
        [111, "16000.00", "headquarters->marketing->promotion"]
    ], result)
}
