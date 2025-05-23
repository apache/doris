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
suite("test_nthvalue_function") {

     def dbName = "test_nthvalue_function_db"
     sql "DROP DATABASE IF EXISTS ${dbName}"
     sql "CREATE DATABASE ${dbName}"
     sql "USE $dbName"

     sql "DROP TABLE IF EXISTS test_nthvalue_function"
     sql """
         CREATE TABLE IF NOT EXISTS `test_nthvalue_function` (
             `k0` boolean null comment "",
             `k1` tinyint(4) null comment "",
             `k2` smallint(6) null comment "",
             `k3` int(11) null comment "",
             `k4` bigint(20) null comment "",
             `k5` decimal(10, 6) null comment "",
             `k6` char(5) null comment "",
             `k10` date null comment "",
             `k11` datetime null comment "",
             `k7` varchar(20) null comment "",
             `k8` double max null comment "",
             `k9` float sum null comment "",
             `k12` string replace null comment "",
             `k13` largeint(40) replace null comment ""
         ) engine=olap
         DISTRIBUTED BY HASH(`k1`) BUCKETS 5 properties("replication_num" = "1")
         """

     streamLoad {
         table "test_nthvalue_function"
         db dbName
         set 'column_separator', ','
         file "../../baseall.txt"
     }
    sql "sync"

    qt_select "select count() from test_nthvalue_function;"

    test {
        sql "select k1, k2, k3, nth_value(k1,0) over (partition by k1 order by k2) as ntile from test_nthvalue_function order by k1, k2, k3 desc;"
        exception "positive"
    }

    test {
        sql "select k1, k2, k3, nth_value(k1,-1) over (partition by k1 order by k2) as ntile from test_nthvalue_function order by k1, k2, k3 desc;"
        exception "positive"
    }

    test {
        sql "select k1, k2, k3, nth_value(k1,NULL) over (partition by k1 order by k2) as ntile from test_nthvalue_function order by k1, k2, k3 desc;"
        exception "positive"
    }

    qt_select_1 "select k1, k2, k3, nth_value(k1,3) over (partition by k1 order by k2)  from test_nthvalue_function order by k1, k2, k3 desc;"
    qt_select_2 "select k1, k2, k3, nth_value(k1,5) over (partition by k1 order by k2)  from test_nthvalue_function order by k1, k2, k3 desc;"
    qt_select_3 "select k2, k1, k3, nth_value(k1,3) over (order by k2 rows BETWEEN 2 PRECEDING AND 2 following) from test_nthvalue_function order by k2,k1;"
    qt_select_4 "select k3, k2, k1, nth_value(k1,3) over (partition by k3 order by k2)  from test_nthvalue_function order by k3, k2, k1;"
    qt_select_6 "select k3, k2, k1, nth_value(k1,3) over (partition by k6 order by k2 rows between 10 preceding and 5 preceding) as res from test_nthvalue_function order by k6, k2, k1,res;"


}





