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

suite("variables_up_down_test_decimalv3", "restart_fe") {

    sql "set enable_agg_state=true"
    sql "set enable_decimal256=false;"

    order_qt_sum0_master_sql """ select sum_merge(col_sum) from t01 group by id order by id;"""
    order_qt_avg0_master_sql """ select avg_merge(col_avg) from t01 group by id order by id;"""

    sql "set enable_decimal256=true;"
    test {
        sql """ select sum_merge(col_sum) from t01 group by id order by id;"""
        exception "INTERNAL_ERROR"
    }

    test {
        sql """ select avg_merge(col_avg) from t01 group by id order by id;"""
        exception "INTERNAL_ERROR"
    }

    sql "set enable_decimal256=false;"
    order_qt_sum_256_0_master_sql """
    select sum_merge(col_sum) from t01 group by id order by id;
    """
    order_qt_avg_256_0_master_sql """ select avg_merge(col_avg) from t01 group by id order by id;
             """
    sql "set enable_decimal256=true;"
    test {
        sql """select sum_merge(col_sum) from t01 group by id order by id;"""
        exception "INTERNAL_ERROR"
    }

    test {
        sql """ select avg_merge(col_avg) from t01 group by id order by id;"""
        exception "INTERNAL_ERROR"
    }

}
