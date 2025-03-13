/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("select_distinct") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    multi_sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
        drop table if exists test_distinct_window;
        create table test_distinct_window(id int) distributed by hash(id) properties('replication_num'='1');
        insert into test_distinct_window values(1), (2), (3);
        """

    test {
        sql "select distinct sum(value) over(partition by id) from (select 100 value, 1 id union all select 100, 2)a"
        result([[100L]])
    }

    test {
        sql "select distinct value+1 from (select 100 value, 1 id union all select 100, 2)a order by value+1"
        result([[101L]])
    }

    test {
        sql "select distinct 1, 2, 3 from test_distinct_window"
        result([[1, 2, 3]])
    }

    test {
        sql "select distinct sum(id) from test_distinct_window"
        result([[6L]])
    }
}
