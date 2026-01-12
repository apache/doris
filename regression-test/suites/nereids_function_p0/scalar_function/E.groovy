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

suite("nereids_scalar_fn_E") {
	sql 'use regression_test_nereids_function_p0'
	sql 'set enable_nereids_planner=true'
	sql 'set enable_fallback_to_original_planner=false'

	sql """
		CREATE ENCRYPTKEY if not exists my_key AS "ABCD123456789";
	"""
	qt_sql_elt_Integer_Varchar "select elt(kint, kvchrs1) from fn_test order by kint, kvchrs1"
	qt_sql_elt_Integer_Varchar_notnull "select elt(kint, kvchrs1) from fn_test_not_nullable order by kint, kvchrs1"
	qt_sql_elt_Integer_String "select elt(kint, kstr) from fn_test order by kint, kstr"
	qt_sql_elt_Integer_String_notnull "select elt(kint, kstr) from fn_test_not_nullable order by kint, kstr"
	qt_sql_ends_with_Varchar_Varchar "select ends_with(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_ends_with_Varchar_Varchar_notnull "select ends_with(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"
	qt_sql_ends_with_String_String "select ends_with(kstr, kstr) from fn_test order by kstr, kstr"
	qt_sql_ends_with_String_String_notnull "select ends_with(kstr, kstr) from fn_test_not_nullable order by kstr, kstr"
	qt_sql_even_Double "select even(kdbl) from fn_test order by kdbl"
	qt_sql_even_Double_notnull "select even(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_exp_Double "select exp(kdbl) from fn_test order by kdbl"
	qt_sql_exp_Double_notnull "select exp(kdbl) from fn_test_not_nullable order by kdbl"
	qt_sql_extract_url_parameter_Varchar_Varchar "select extract_url_parameter(kvchrs1, kvchrs1) from fn_test order by kvchrs1, kvchrs1"
	qt_sql_extract_url_parameter_Varchar_Varchar_notnull "select extract_url_parameter(kvchrs1, kvchrs1) from fn_test_not_nullable order by kvchrs1, kvchrs1"

	qt_sql_encryptkey "select key my_key, key regression_test_nereids_function_p0.my_key"
}