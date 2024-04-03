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

suite("nereids_tvf") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    qt_sql_number '''
        select * from numbers("number" = "10")
    '''

    // when we set numbers to gather, coordinator could not process and set none scan range in thrift
    // so we add this test case to ensure this sql do not core dump
    sql """
        select * from numbers("number"="10") a right join numbers("number"="10") b on true;
    """

    sql """
        select query_id from information_schema.active_queries where `sql` like "%test_queries_tvf%";
    """

    sql """
        select * from numbers("number" = "1") union all select * from numbers("number" = "1");
    """
}