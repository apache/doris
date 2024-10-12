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

suite("test_arith_functions") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "use nereids_test_query_db"

    test {
        sql 'select add(1, 1), subtract(1, 1), multiply(2, 2), divide(3.0, 2.0), mod(3.0, 1.3)'
        result([[2, 0, 4, 1.50000, 0.4]])
    }
    test {
        sql 'select int_divide(1, 1), bitand(1, 1), bitor(2, 2), bitxor(3.0, 2.0), bitnot(3.0)'
        result([[1, 1, 2, 1L, -4L]])
    }
    test {
        sql 'select add(k1, k2) + subtract(k2, k3) + multiply(k3, k4), cast(divide(k4, k3) + mod(k4, k3) as bigint) from test order by k1 limit 1'
        result([[11022916880, 11902L]])
    }

    sql """
    CREATE TABLE testmoddb (
        K1 BIGINT,
        K2 FLOAT
    ) properties("replication_num" = "1");
   
    """

    sql """
    insert into testmoddb values(1,1.1);
    """

    qt_sql """ select mod(k1,k2) from testmoddb; """

//    test {
//        sql 'select int_divide(k1, k2), bitand(k2, k3), bitor(k3, k4), bitxor(k4, k3), bitnot(k4) from test order by k1'
//    }
}
