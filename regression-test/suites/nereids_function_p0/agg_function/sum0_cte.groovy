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

suite("sum0_cte") {
    sql 'use regression_test_nereids_function_p0'
    sql "set ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    qt_sum0_cte """with tmp as (select * from fn_test)
    select * from (select sum0(distinct kint) from tmp ) t cross join (select sum0(distinct ksint) from tmp) tt;
    """
    qt_shape """
    explain shape plan
    with tmp as (select * from fn_test)
    select * from (select sum0(distinct kint) from tmp ) t cross join (select sum0(distinct ksint) from tmp) tt;
    """
}