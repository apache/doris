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

suite("nereids_scalar_fn_Q") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'
	qt_sql_quarter_DateTime "select quarter(kdtm) from fn_test order by kdtm"
	qt_sql_quarter_DateTime_notnull "select quarter(kdtm) from fn_test_not_nullable order by kdtm"
	qt_sql_quarter_DateTimeV2 "select quarter(kdtmv2s1) from fn_test order by kdtmv2s1"
	qt_sql_quarter_DateTimeV2_notnull "select quarter(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
	qt_sql_quarter_DateV2 "select quarter(kdtv2) from fn_test order by kdtv2"
	qt_sql_quarter_DateV2_notnull "select quarter(kdtv2) from fn_test_not_nullable order by kdtv2"
}