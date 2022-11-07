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

suite("function") {
    sql """
        SET enable_vectorized_engine=true
    """

    sql """
        SET enable_nereids_planner=true
    """
    sql "SET enable_fallback_to_original_planner=false"

    order_qt_max """
        SELECT max(lo_discount), max(lo_extendedprice) AS max_extendedprice FROM lineorder;
    """

    order_qt_min """
        SELECT min(lo_discount), min(lo_extendedprice) AS min_extendedprice FROM lineorder;
    """

    order_qt_max_and_min """
        SELECT max(lo_extendedprice), min(lo_discount) FROM lineorder;
    """

    order_qt_count """
        SELECT count(c_city), count(*) AS custdist FROM customer;
    """

    order_qt_distinct_count """
        SELECT count(distinct c_custkey + 1) AS custdist FROM customer group by c_city;
    """

    order_qt_distinct_count_group_by_distributed_key """
        SELECT c_custkey, count(distinct c_custkey + 1) AS custdist FROM customer group by c_custkey;
    """

    order_qt_avg """
        SELECT avg(lo_tax), avg(lo_extendedprice) AS avg_extendedprice FROM lineorder;
    """

    // nested function
    test {
        sql "select cast(date('1994-01-01') + interval '1' YEAR as varchar)"
        result([["1995-01-01 00:00:00"]])
    }

    test {
        sql "select substring(substring('1994-01-01', 5), 3)"
        result([["1-01"]])
    }
}

