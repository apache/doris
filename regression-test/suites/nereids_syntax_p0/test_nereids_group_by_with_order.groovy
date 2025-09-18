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

suite("test_nereids_group_by_with_order") {
    sql "SET enable_nereids_planner=true"

    sql "DROP TABLE IF EXISTS testGroupByWithOrder"

    sql """
        CREATE TABLE `testGroupByWithOrder` (
        `k1` bigint(20) NULL,
        `k2` bigint(20) NULL,
        `k3` bigint(20) NULL,
        `k4` bigint(20) not null,
        `k5` varchar(10),
        `k6` varchar(10)
        ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k2`) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """

    sql """
        INSERT INTO testGroupByWithOrder VALUES
            (1, 1, 1, 3, 'a', 'b'),
            (1, 1, 2, 3, 'a', 'c'),
            (1, 1, 3, 4, 'a' , 'd'),
            (1, 0, null, 4, 'b' , 'b'),
            (2, 2, 2, 5, 'b', 'c'),
            (2, 2, 4, 5, 'b' , 'd'),
            (2, 2, 6, 4, 'c', 'b'),
            (2, 2, null, 4, 'c', 'c'),
            (3, 3, 3, 3, 'c', 'd'),
            (3, 3, 6, 3, 'd', 'b'),
            (3, 3, 9, 4, 'd', 'c'),
            (3, 0, null, 5, 'd', 'd')
    """

    qt_select1 """select k1, sum(k2) from testGroupByWithOrder group by k1 asc"""
    qt_select2 """select k1, sum(k2) from testGroupByWithOrder group by k1 desc"""
    qt_select3 """select k1, sum(k2) from testGroupByWithOrder group by k1 asc with rollup"""
    qt_select4 """select k1, sum(k2) from testGroupByWithOrder group by k1 desc with rollup"""

    qt_select5 """select k1, k2, min(k3) from testGroupByWithOrder group by k1 asc, k2 desc"""
    qt_select6 """select k1, k2, min(k3) from testGroupByWithOrder group by k1, k2 desc"""
    qt_select7 """select k1, k2, min(k3) from testGroupByWithOrder group by k1 asc, k2 desc with rollup"""
    qt_select8 """select k1, k2, min(k3) from testGroupByWithOrder group by k1, k2 desc with rollup"""

    qt_select9 """select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1 asc, k2 desc having s > 2"""
    qt_select10 """select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1 desc, k2 having s > 2"""
    qt_select11 """select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1 asc, k2 desc with rollup having s > 2"""
    qt_select12 """select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1, k2 desc with rollup having s > 2"""
    qt_select13 """select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1, k2 desc with rollup having s > 2 order by k2, k1 desc"""

    explain {
        sql("select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1, k2 having s > 2")
        notContains "VSORT"
    }

    explain {
        sql("select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1 desc, k2 having s > 2")
        contains "VSORT"
    }

    explain {
        sql("select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1, k2 with rollup having s > 2")
        notContains "VSORT"
    }

    explain {
        sql("select k1, k2, min(k3) as m, sum(k4) as s from testGroupByWithOrder group by k1 desc, k2 with rollup having s > 2")
        contains "VSORT"
    }

}
