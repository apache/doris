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

suite("hbo_failed_query_retry_test") {
    // TODO: current profile doesn't support failed query whole fragment info collection
    // add case to check partial plan's runtime stats is available and can be used in retry run
    // the following case doesn't work currently !!!
    sql "create database if not exists hbo_test;"
    sql "use hbo_test;"

    sql "drop table if exists hbo_failed_query_retry_test;"
    sql "create table hbo_failed_query_retry_test(a int, b int) distributed by hash(a) buckets 32 properties(" replication_num "=" 1 ");"
    sql "insert into hbo_failed_query_retry_test select number, number from numbers(" number " = " 100000000 ");"
    sql "analyze table hbo_failed_query_retry_test with full with sync;"

    sql "select count(1) from hbo_failed_query_retry_test t1, hbo_failed_query_retry_test t2 where t1.a = t2.a;"

    explain {
        sql "physical plan select count(1) from hbo_failed_query_retry_test t1, hbo_failed_query_retry_test t2 where t1.a = t2.a;"
        //nocontains("(hbo)")
    }

    sql "select count(1) from hbo_failed_query_retry_test t1, hbo_failed_query_retry_test t2 where t1.a = t2.a;"

    explain {
        sql "physical plan select count(1) from hbo_failed_query_retry_test t1, hbo_failed_query_retry_test t2 where t1.a = t2.a;"
        //nocontains("(hbo)")
    }
}