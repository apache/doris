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

suite("nereids_scalar_fn_G") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    qt_sql_gcd_TinyInt_TinyInt "select gcd(ktint, ktint) from fn_test order by ktint, ktint"
    qt_sql_gcd_TinyInt_TinyInt_notnull "select gcd(ktint, ktint) from fn_test_not_nullable order by ktint, ktint"
    qt_sql_gcd_SmallInt_SmallInt "select gcd(ksint, ksint) from fn_test order by ksint, ksint"
    qt_sql_gcd_SmallInt_SmallInt_notnull "select gcd(ksint, ksint) from fn_test_not_nullable order by ksint, ksint"
    qt_sql_gcd_Integer_Integer "select gcd(kint, kint) from fn_test order by kint, kint"
    qt_sql_gcd_Integer_Integer_notnull "select gcd(kint, kint) from fn_test_not_nullable order by kint, kint"
    qt_sql_gcd_BigInt_BigInt "select gcd(kbint, kbint) from fn_test order by kbint, kbint"
    qt_sql_gcd_BigInt_BigInt_notnull "select gcd(kbint, kbint) from fn_test_not_nullable order by kbint, kbint"  
    qt_sql_gcd_LargeInt_LargeInt "select gcd(klint, klint) from fn_test order by klint, klint"
    qt_sql_gcd_LargeInt_LargeInt_notnull "select gcd(klint, klint) from fn_test_not_nullable order by klint, klint"  
    qt_sql_get_json_bigint_Varchar_Varchar """SELECT get_json_bigint('{"k1":1678708107000, "k2":"2"}', "\$.k1")"""
    qt_sql_greatest_TinyInt "select greatest(ktint) from fn_test order by ktint"
    qt_sql_greatest_TinyInt_notnull "select greatest(ktint) from fn_test_not_nullable order by ktint"
    qt_sql_greatest_SmallInt "select greatest(ksint) from fn_test order by ksint"
    qt_sql_greatest_SmallInt_notnull "select greatest(ksint) from fn_test_not_nullable order by ksint"
    qt_sql_greatest_Integer "select greatest(kint) from fn_test order by kint"
    qt_sql_greatest_Integer_notnull "select greatest(kint) from fn_test_not_nullable order by kint"
    qt_sql_greatest_BigInt "select greatest(kbint) from fn_test order by kbint"
    qt_sql_greatest_BigInt_notnull "select greatest(kbint) from fn_test_not_nullable order by kbint"
    qt_sql_greatest_LargeInt "select greatest(klint) from fn_test order by klint"
    qt_sql_greatest_LargeInt_notnull "select greatest(klint) from fn_test_not_nullable order by klint"
    qt_sql_greatest_Float "select greatest(kfloat) from fn_test order by kfloat"
    qt_sql_greatest_Float_notnull "select greatest(kfloat) from fn_test_not_nullable order by kfloat"
    qt_sql_greatest_Double "select greatest(kdbl) from fn_test order by kdbl"
    qt_sql_greatest_Double_notnull "select greatest(kdbl) from fn_test_not_nullable order by kdbl"
    qt_sql_greatest_DecimalV2 "select greatest(kdcmls1) from fn_test order by kdcmls1"
    qt_sql_greatest_DecimalV2_notnull "select greatest(kdcmls1) from fn_test_not_nullable order by kdcmls1"
    qt_sql_greatest_Date "select greatest(kdt) from fn_test order by kdt"
    qt_sql_greatest_Date_notnull "select greatest(kdt) from fn_test_not_nullable order by kdt"
    qt_sql_greatest_DateV2 "select greatest(kdtv2) from fn_test order by kdtv2"
    qt_sql_greatest_DateV2_notnull "select greatest(kdtv2) from fn_test_not_nullable order by kdtv2"
    qt_sql_greatest_DateTime "select greatest(kdtm) from fn_test order by kdtm"
    qt_sql_greatest_DateTime_notnull "select greatest(kdtm) from fn_test_not_nullable order by kdtm"
    qt_sql_greatest_DateTimeV2 "select greatest(kdtmv2s1) from fn_test order by kdtmv2s1"
    qt_sql_greatest_DateTimeV2_notnull "select greatest(kdtmv2s1) from fn_test_not_nullable order by kdtmv2s1"
    qt_sql_greatest_Varchar "select greatest(kvchrs1) from fn_test order by kvchrs1"
    qt_sql_greatest_Varchar_notnull "select greatest(kvchrs1) from fn_test_not_nullable order by kvchrs1"
    qt_sql_greatest_String "select greatest(kstr) from fn_test order by kstr"
    qt_sql_greatest_String_notnull "select greatest(kstr) from fn_test_not_nullable order by kstr"
}
