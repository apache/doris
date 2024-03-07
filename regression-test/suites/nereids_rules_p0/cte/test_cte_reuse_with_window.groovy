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

suite("test_cte_reuse_with_window") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_pipeline_engine=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql """
        drop table if exists test_cte_reuse_with_window;
    """

    sql """
        create table test_cte_reuse_with_window (id int default '10', c1 int default '10') distributed by hash(id) properties('replication_num'="1");
    """

    sql """
        with temp as (select * from test_cte_reuse_with_window)
        select * from
            (select t.id, row_number() over(order by t.id) as num from temp t limit 20) a
            left join
                (select t.id, row_number() over(order by t.id desc) as num from temp t where t.id = 5) b
                on a.id = b.id
    """
}
