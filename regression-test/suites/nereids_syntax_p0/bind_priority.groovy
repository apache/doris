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

suite("bind_priority") {
    sql "SET enable_nereids_planner=true"

    sql """
        DROP TABLE IF EXISTS bind_priority_tbl
       """

    sql """CREATE TABLE IF NOT EXISTS bind_priority_tbl (a int not null, b int not null)
        DISTRIBUTED BY HASH(a)
        BUCKETS 1
        PROPERTIES(
            "replication_num"="1"
        )
        """

    sql """
    insert into bind_priority_tbl values(1, 2),(3, 4)
    """

    sql "SET enable_fallback_to_original_planner=false"

    sql """sync"""

    qt_select """
        select a * -1 as a from bind_priority_tbl group by a order by a;
    """

    qt_select """
        select coalesce(a, 'all') as a, count(*) as cnt from (select  null as a  union all  select  'a' as a ) t group by grouping sets ((a),()) order by a;
    """

    qt_select """
        select (a-1) as a from bind_priority_tbl group by a having a=1;
    """

    test {
        sql """
            select sum(a) as v from bind_priority_tbl  group by v;
            """
        exception "Unexpected exception: cannot bind GROUP BY KEY: v"
    }
}
