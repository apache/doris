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

    sql "drop table if exists test_date_func_boundary"
    sql """
        create table test_date_func_boundary(
            id int,
            dt datetime,
            dtv2 datetimev2(6)
        ) distributed by hash(id) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into test_date_func_boundary values
            (1, '9999-12-31 23:59:59', '9999-12-31 23:59:59.999999'),
            (2, null, null),
            (3, '9999-12-30 12:00:00', '9999-12-30 12:00:00.123456')
    """

    qt_date_v1_greater_than_max """
        select id from test_date_func_boundary where date(dt) > '9999-12-31'
    """

    qt_date_v2_greater_than_max """
        select id from test_date_func_boundary where date(dtv2) > '9999-12-31'
    """

    order_qt_date_greater_than_max_null_semantics """
        select id, date(dt) > '9999-12-31', date(dtv2) > '9999-12-31'
        from test_date_func_boundary
        order by id
    """
}
