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

suite("group_by_constant") {
    sql """
        SET enable_nereids_planner=true
    """
    sql "set parallel_fragment_exec_instance_num=8"
    sql "SET enable_fallback_to_original_planner=false"

    qt_select_1 """ 
        select 'str', sum(lo_tax), lo_orderkey, max(lo_discount), 1 from lineorder, customer group by 3, 5, 'str', 1, lo_orderkey order by lo_orderkey;
    """

    qt_sql """SELECT lo_custkey, lo_partkey, SUM(lo_tax) FROM lineorder GROUP BY 1, 2 order by lo_partkey"""

    qt_sql """SELECT lo_partkey, lo_custkey, SUM(lo_tax) FROM lineorder GROUP BY 1, 2 order by lo_partkey, lo_custkey"""

    qt_sql """SELECT lo_partkey, 1, SUM(lo_tax) FROM lineorder GROUP BY 1,  1 + 1 order by lo_partkey"""

    qt_sql """SELECT lo_partkey, 1, SUM(lo_tax) FROM lineorder GROUP BY 'g',  1 order by lo_partkey"""

    qt_sql """select 2 from lineorder group by 1"""

    qt_sql """select SUM(lo_tax) FROM lineorder group by null;"""

    qt_sql """select 5 FROM lineorder group by null;"""

    qt_sql """select lo_orderkey from lineorder order by lo_tax desc, lo_tax desc;"""

    test {
        sql "select SUM(lo_tax) FROM lineorder group by 1;"
        exception "GROUP BY expression must not contain aggregate functions: sum(lo_tax)"
    }

    test {
        sql "select SUM(lo_tax) FROM lineorder group by SUM(lo_tax);"
        exception "GROUP BY expression must not contain aggregate functions: sum(lo_tax)"
    }

    qt_sql """select SUM(if(lo_tax=1,lo_tax,0)) FROM lineorder where false;"""

    qt_sql """select 2 FROM lineorder group by 1;"""

    qt_sql """select 1 from lineorder group by 1 + 1"""
}
