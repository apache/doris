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

suite("test_agg_state_avg") {
    sql "set global enable_agg_state=true"
    sql """ DROP TABLE IF EXISTS a_table; """
    sql """
            create table a_table(
            k1 int not null,
            k2 agg_state<avg(int not null)> generic
        )
        aggregate key (k1)
        distributed BY hash(k1)
        properties("replication_num" = "1");
        """

    sql """insert into a_table
            select e1/1000,avg_state(e1) from 
                (select 1 k1) as t lateral view explode_numbers(8000) tmp1 as e1;"""


    sql"set enable_nereids_planner=true;"
    qt_select """ select k1,avg_merge(k2) from a_table group by k1 order by k1;
             """
    qt_select """ select avg_merge(tmp) from (select k1,avg_union(k2) tmp from a_table group by k1)t;
             """
    test {
        sql "select * from a_table;"
    }
}
