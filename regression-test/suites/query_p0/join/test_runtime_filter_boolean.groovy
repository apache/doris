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

suite("test_runtime_filter_boolean", "query_p0") {
    sql "drop table if exists test_runtime_filter_boolean0;"
    sql """ create table test_runtime_filter_boolean0(k1 int, v1 boolean)
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                properties("replication_num" = "1"); """
    
    sql """insert into test_runtime_filter_boolean0 values 
            (10, false),
            (10, false),
            (10, false),
            (110, false),
            (110, false),
            (110, false);"""

    sql "drop table if exists test_runtime_filter_boolean1;"
    sql """ create table test_runtime_filter_boolean1(k1 int, v1 boolean)
                DUPLICATE KEY(`k1`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                properties("replication_num" = "1"); """
    
    sql """insert into test_runtime_filter_boolean1 values 
            (11, false);"""


    qt_rf_bool """
        select
                t0.k1, t1.k1
        from
                (
                        select
                                k1,
                                v1
                        from
                                test_runtime_filter_boolean0
                ) t0
                inner join [shuffle] (
                        select
                                k1,
                                v1
                        from
                                test_runtime_filter_boolean1
                ) t1 on t0.v1 = t1.v1
        order by
                1,2;
    """
}
