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

suite("test_limit_join", "nereids_p0") {
    def DBname = "nereids_regression_test_limit_join"
    sql "DROP DATABASE IF EXISTS ${DBname}"
    sql "CREATE DATABASE IF NOT EXISTS ${DBname}"
    sql "use ${DBname}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tbName1 = "t1"
    def tbName2 = "t2"

    sql "DROP TABLE IF EXISTS ${tbName1};"
    sql "DROP TABLE IF EXISTS ${tbName2};"

    sql """create table if not exists ${tbName1} (c1 int, c2 int) DISTRIBUTED BY HASH(c1) properties("replication_num" = "1");"""
    sql """create table if not exists ${tbName2} (c1 int, c2 int, c3 int) DISTRIBUTED BY HASH(c1) properties("replication_num" = "1");"""

    sql "insert into ${tbName1} values (1,1);"
    sql "insert into ${tbName1} values (2,2);"
    sql "insert into ${tbName1} values (1,null);"
    sql "insert into ${tbName1} values (2,null);"
    sql "insert into ${tbName2} values (0,1,9999);"
    sql "insert into ${tbName2} values (1,1,9999);"
    sql "insert into ${tbName2} values (0,null,9999);"
    sql "insert into ${tbName2} values (1,null,9999);"


    /* test push limit-distinct through join */
    order_qt_join1 """
        SELECT t1.c1
        FROM ${tbName1} t1 left join ${tbName2} t2 on t1.c1 = t2.c1
        GROUP BY t1.c1
        limit 2;
        """

    sql """
        SELECT t1.c1
        FROM ${tbName1} t1 left join ${tbName2} t2 on t1.c1 = t2.c1
        GROUP BY t1.c1
        LIMIT 1 OFFSET 1;
        """

    order_qt_join3 """
        SELECT t2.c1
        FROM ${tbName1} t1 right join ${tbName2} t2 on t1.c1 = t2.c1
        GROUP BY t2.c1
        limit 2;
        """
    
    sql """
        SELECT t2.c1
        FROM ${tbName1} t1 right join ${tbName2} t2 on t1.c1 = t2.c1
        GROUP BY t2.c1
        LIMIT 1 OFFSET 1;
        """

    /* test push topN through join */
    qt_join5 """
        SELECT t1.c1
        FROM ${tbName1} t1 left join ${tbName2} t2 on t1.c1 = t2.c1
        ORDER BY t1.c1
        limit 2;
        """

    qt_join6 """
        SELECT t1.c1
        FROM ${tbName1} t1 left join ${tbName2} t2 on t1.c1 = t2.c1
        ORDER BY t1.c1
        LIMIT 1 OFFSET 1;
        """

    qt_join7 """
        SELECT t2.c1
        FROM ${tbName1} t1 right join ${tbName2} t2 on t1.c1 = t2.c1
        ORDER BY t2.c1
        limit 2;
        """

    qt_join8 """
        SELECT t2.c1
        FROM ${tbName1} t1 right join ${tbName2} t2 on t1.c1 = t2.c1
        ORDER BY t2.c1
        LIMIT 1 OFFSET 1;
        """

    sql "DROP DATABASE IF EXISTS ${DBname};"
}

