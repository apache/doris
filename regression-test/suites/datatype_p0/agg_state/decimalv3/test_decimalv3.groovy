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

suite("test_decimalv3") {
    sql "set enable_agg_state=true"
    sql "set enable_decimal256=false;"
    sql """ DROP TABLE IF EXISTS t01; """
    sql """
        create table t01(id int, col_sum agg_state<sum(decimalv3(20,6))> generic, col_avg agg_state<avg(decimalv3(20,6))> generic)  properties ("replication_num" = "1");
        """

    sql """insert into t01 values (1, sum_state(10.1), avg_state(10.1)), (1, sum_state(20.1), avg_state(20.1)), (2, sum_state(10.2), avg_state(10.2)), (2, sum_state(11.0), avg_state(11.0));
"""

    qt_sum0 """ select sum_merge(col_sum) from t01 group by id order by id;
             """
    qt_avg0 """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    /*
    // TODO: need to fix
    sql "set enable_decimal256=true;"
    qt_sum1 """ select sum_merge(col_sum) from t01 group by id order by id;
             """
    qt_avg1 """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    */

    sql "set enable_decimal256=true;"
    sql """ DROP TABLE IF EXISTS t01; """
    sql """
        create table t01(id int, col_sum agg_state<sum(decimalv3(76,6))> generic, col_avg agg_state<avg(decimalv3(76,6))> generic)  properties ("replication_num" = "1");
        """

    sql """
    insert into t01 values (1, sum_state(10.1), avg_state(10.1)), (1, sum_state(20.1), avg_state(20.1)), (2, sum_state(10.2), avg_state(10.2)), (2, sum_state(11.0), avg_state(11.0));
    """
    qt_sum_256_0 """
    select sum_merge(col_sum) from t01 group by id order by id;
    """
    qt_avg_256_0 """ select avg_merge(col_avg) from t01 group by id order by id;
             """

    /*
    // TODO: need to fix
    sql "set enable_decimal256=false;"
    qt_sum_256_1 """
    select sum_merge(col_sum) from t01 group by id order by id;
    """
    qt_avg_256_1 """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    */
}
