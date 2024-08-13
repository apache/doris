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

suite("test_create_table_without_distribution") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    multi_sql """
    drop table if exists test_create_table_without_distribution;
    create table test_create_table_without_distribution(a int, b int) properties ("replication_num"="1")   
    """
    qt_test_insert """
    insert into test_create_table_without_distribution values(1,2);
    """
    qt_test_select "select * from test_create_table_without_distribution;"
    def res1 = sql "show create table test_create_table_without_distribution;"
    mustContain(res1[0][1], "DISTRIBUTED BY RANDOM BUCKETS AUTO")

    sql "SET enable_nereids_planner=false;"
    multi_sql """
    drop table if exists test_create_table_without_distribution;
    create table test_create_table_without_distribution(a int, b int) properties ("replication_num"="1")   
    """
    qt_test_insert_old_planner """
    insert into test_create_table_without_distribution values(1,2);
    """
    qt_test_select_old_planner "select * from test_create_table_without_distribution;"
    def res2 = sql "show create table test_create_table_without_distribution;"
    mustContain(res2[0][1], "DISTRIBUTED BY RANDOM BUCKETS AUTO")
}