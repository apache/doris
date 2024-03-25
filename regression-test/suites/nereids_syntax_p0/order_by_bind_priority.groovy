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

suite("order_by_bind_priority") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    sql "drop table if exists t_order_by_bind_priority"
    sql """create table t_order_by_bind_priority (c1 int, c2 int) distributed by hash(c1) properties("replication_num"="1");"""
    sql "insert into t_order_by_bind_priority values(-2, -2),(1,2),(1,2),(3,3),(-5,5);"
    sql "sync"


    sql "select 2*abs(sum(c1)) as c1, c1,sum(c1)+c1 from t_order_by_bind_priority group by c1 order by sum(c1)+c1 asc;"
    sql "select 2*abs(sum(c1)) as c2, c1,sum(c1)+c2 from t_order_by_bind_priority group by c1,c2 order by sum(c1)+c2 asc;"
    sql "select abs(sum(c1)) as c1, c1,sum(c2) as c2 from t_order_by_bind_priority group by c1 order by sum(c1) asc;"
    test {
        sql "select abs(sum(c1)) as c1, c1,sum(c2) as c2 from t_order_by_bind_priority group by c1 order by sum(c1)+c2 asc;"
        exception "c2 should be grouped by."
    }


}