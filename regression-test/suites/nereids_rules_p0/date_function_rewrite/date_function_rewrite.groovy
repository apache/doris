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

suite("date_function_rewrite") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "drop table if exists test_date_func"
    sql """
        create table test_date_func(a int, test_time int(11)) distributed by hash (a) buckets 5
        properties("replication_num"="1");
        """
    sql "insert into test_date_func values(1,1690128000);\n"
    qt_test """
    select if (date(date_add(FROM_UNIXTIME(t1.test_time, '%Y-%m-%d'),2)) > '2023-07-25',1,0) from test_date_func t1;
    """
}