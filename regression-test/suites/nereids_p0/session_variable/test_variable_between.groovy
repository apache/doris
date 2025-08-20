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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('test_variable_between') {
    multi_sql """
        SET enable_nereids_planner=true;
        SET enable_fallback_to_original_planner=false;
        SET disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        SET ignore_shape_nodes='PhysicalDistribute';
        """

    sql "drop table if exists tbl_test_variable_between force"
    sql "create table tbl_test_variable_between(a int, b int) properties('replication_num' = '1')"
    sql "set @beginValue = 1"
    sql "set @endValue = 100"
    qt_1 "explain shape plan select * from tbl_test_variable_between where a between @beginValue and @endValue"
    sql "drop table if exists tbl_test_variable_between force"
}
