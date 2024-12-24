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

suite("nereids_scalar_fn_Array") {
    sql 'use regression_test_nereids_function_p0'
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    // array avg
    order_qt_sql_array_avg_Double "select array_avg(kadbl) from fn_test"
    order_qt_sql_array_avg_Double_notnull "select array_avg(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_avg_Float "select array_avg(kafloat) from fn_test"
    order_qt_sql_array_avg_Float_notnull "select array_avg(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_avg_LargeInt "select array_avg(kalint) from fn_test"
    order_qt_sql_array_avg_LargeInt_notnull "select array_avg(kalint) from fn_test_not_nullable"
    order_qt_sql_array_avg_BigInt "select array_avg(kabint) from fn_test"
    order_qt_sql_array_avg_BigInt_notnull "select array_avg(kabint) from fn_test_not_nullable"
    order_qt_sql_array_avg_SmallInt "select array_avg(kasint) from fn_test"
    order_qt_sql_array_avg_SmallInt_notnull "select array_avg(kasint) from fn_test_not_nullable"
    order_qt_sql_array_avg_Integer "select array_avg(kaint) from fn_test"
    order_qt_sql_array_avg_Integer_notnull "select array_avg(kaint) from fn_test_not_nullable"
    order_qt_sql_array_avg_TinyInt "select array_avg(katint) from fn_test"
    order_qt_sql_array_avg_TinyInt_notnull "select array_avg(katint) from fn_test_not_nullable"
    order_qt_sql_array_avg_DecimalV3 "select array_avg(kadcml) from fn_test"
    order_qt_sql_array_avg_DecimalV3_notnull "select array_avg(kadcml) from fn_test_not_nullable"

    // array_compact
    order_qt_sql_array_compact_Double "select array_compact(kadbl) from fn_test"
    order_qt_sql_array_compact_Double_notnull "select array_compact(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_compact_Float "select array_compact(kafloat) from fn_test"
    order_qt_sql_array_compact_Float_notnull "select array_compact(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_compact_LargeInt "select array_compact(kalint) from fn_test"
    order_qt_sql_array_compact_LargeInt_notnull "select array_compact(kalint) from fn_test_not_nullable"
    order_qt_sql_array_compact_BigInt "select array_compact(kabint) from fn_test"
    order_qt_sql_array_compact_BigInt_notnull "select array_compact(kabint) from fn_test_not_nullable"
    order_qt_sql_array_compact_SmallInt "select array_compact(kasint) from fn_test"
    order_qt_sql_array_compact_SmallInt_notnull "select array_compact(kasint) from fn_test_not_nullable"
    order_qt_sql_array_compact_Integer "select array_compact(kaint) from fn_test"
    order_qt_sql_array_compact_Integer_notnull "select array_compact(kaint) from fn_test_not_nullable"
    order_qt_sql_array_compact_TinyInt "select array_compact(katint) from fn_test"
    order_qt_sql_array_compact_TinyInt_notnull "select array_compact(katint) from fn_test_not_nullable"
    order_qt_sql_array_compact_DecimalV3 "select array_compact(kadcml) from fn_test"
    order_qt_sql_array_compact_DecimalV3_notnull "select array_compact(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_compact_Boolean "select array_compact(kabool) from fn_test"
    order_qt_sql_array_compact_Boolean_notnull "select array_compact(kabool) from fn_test_not_nullable"

    order_qt_sql_array_compact_Char "select array_compact(kachr) from fn_test"
    order_qt_sql_array_compact_Char_notnull "select array_compact(kachr) from fn_test_not_nullable"
    order_qt_sql_array_compact_Varchar "select array_compact(kavchr) from fn_test"
    order_qt_sql_array_compact_Varchar_notnull "select array_compact(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_compact_String "select array_compact(kastr) from fn_test"
    order_qt_sql_array_compact_String_notnull "select array_compact(kastr) from fn_test_not_nullable"

    order_qt_sql_array_compact_DatetimeV2 "select array_compact(kadtmv2) from fn_test"
    order_qt_sql_array_compact_DatetimeV2_notnull "select array_compact(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_compact_DateV2 "select array_compact(kadtv2) from fn_test"
    order_qt_sql_array_compact_DateV2_notnull "select array_compact(kadtv2) from fn_test_not_nullable"

    // array_concat
    order_qt_sql_array_concat_Double "select array_concat(kadbl, kadbl) from fn_test"
    order_qt_sql_array_concat_Double_notnull "select array_concat(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_concat_Float "select array_concat(kafloat, kafloat) from fn_test"
    order_qt_sql_array_concat_Float_notnull "select array_concat(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_concat_LargeInt "select array_concat(kalint, kalint) from fn_test"
    order_qt_sql_array_concat_LargeInt_notnull "select array_concat(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_array_concat_BigInt "select array_concat(kabint, kabint) from fn_test"
    order_qt_sql_array_concat_BigInt_notnull "select array_concat(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_array_concat_SmallInt "select array_concat(kasint, kasint) from fn_test"
    order_qt_sql_array_concat_SmallInt_notnull "select array_concat(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_array_concat_Integer "select array_concat(kaint, kaint) from fn_test"
    order_qt_sql_array_concat_Integer_notnull "select array_concat(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_array_concat_TinyInt "select array_concat(katint, katint) from fn_test"
    order_qt_sql_array_concat_TinyInt_notnull "select array_concat(katint, katint) from fn_test_not_nullable"
    order_qt_sql_array_concat_DecimalV3 "select array_concat(kadcml, kadcml) from fn_test"
    order_qt_sql_array_concat_DecimalV3_notnull "select array_concat(kadcml, kadcml) from fn_test_not_nullable"

    order_qt_sql_array_concat_Boolean "select array_concat(kabool, kabool) from fn_test"
    order_qt_sql_array_concat_Boolean_notnull "select array_concat(kabool, kabool) from fn_test_not_nullable"

    order_qt_sql_array_concat_Char "select array_concat(kachr, kachr) from fn_test"
    order_qt_sql_array_concat_Char_notnull "select array_concat(kachr, kachr) from fn_test_not_nullable"
    order_qt_sql_array_concat_Varchar "select array_concat(kavchr, kavchr) from fn_test"
    order_qt_sql_array_concat_Varchar_notnull "select array_concat(kavchr, kavchr) from fn_test_not_nullable"
    order_qt_sql_array_concat_String "select array_concat(kastr, kastr) from fn_test"
    order_qt_sql_array_concat_String_notnull "select array_concat(kastr, kastr) from fn_test_not_nullable"

    order_qt_sql_array_concat_DatetimeV2 "select array_concat(kadtmv2, kadtmv2) from fn_test"
    order_qt_sql_array_concat_DatetimeV2_notnull "select array_concat(kadtmv2, kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_concat_DateV2 "select array_concat(kadtv2, kadtv2) from fn_test"
    order_qt_sql_array_concat_DateV2_notnull "select array_concat(kadtv2, kadtv2) from fn_test_not_nullable"

    // array_difference
    order_qt_sql_array_difference_Double "select array_difference(kadbl) from fn_test"
    order_qt_sql_array_difference_Double_notnull "select array_difference(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_difference_Float "select array_difference(kafloat) from fn_test"
    order_qt_sql_array_difference_Float_notnull "select array_difference(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_difference_LargeInt "select array_difference(kalint) from fn_test"
    order_qt_sql_array_difference_LargeInt_notnull "select array_difference(kalint) from fn_test_not_nullable"
    order_qt_sql_array_difference_BigInt "select array_difference(kabint) from fn_test"
    order_qt_sql_array_difference_BigInt_notnull "select array_difference(kabint) from fn_test_not_nullable"
    order_qt_sql_array_difference_SmallInt "select array_difference(kasint) from fn_test"
    order_qt_sql_array_difference_SmallInt_notnull "select array_difference(kasint) from fn_test_not_nullable"
    order_qt_sql_array_difference_Integer "select array_difference(kaint) from fn_test"
    order_qt_sql_array_difference_Integer_notnull "select array_difference(kaint) from fn_test_not_nullable"
    order_qt_sql_array_difference_TinyInt "select array_difference(katint) from fn_test"
    order_qt_sql_array_difference_TinyInt_notnull "select array_difference(katint) from fn_test_not_nullable"
    order_qt_sql_array_difference_DecimalV3 "select array_difference(kadcml) from fn_test"
    order_qt_sql_array_difference_DecimalV3_notnull "select array_difference(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_difference_Boolean "select array_difference(kabool) from fn_test"
    order_qt_sql_array_difference_Boolean_notnull "select array_difference(kabool) from fn_test_not_nullable"

    order_qt_sql_array_difference_Char "select array_difference(kachr) from fn_test"
    order_qt_sql_array_difference_Char_notnull "select array_difference(kachr) from fn_test_not_nullable"
    order_qt_sql_array_difference_Varchar "select array_difference(kavchr) from fn_test"
    order_qt_sql_array_difference_Varchar_notnull "select array_difference(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_difference_String "select array_difference(kastr) from fn_test"
    order_qt_sql_array_difference_String_notnull "select array_difference(kastr) from fn_test_not_nullable"

    order_qt_sql_array_difference_DatetimeV2 "select array_difference(kadtmv2) from fn_test"
    order_qt_sql_array_difference_DatetimeV2_notnull "select array_difference(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_difference_DateV2 "select array_difference(kadtv2) from fn_test"
    order_qt_sql_array_difference_DateV2_notnull "select array_difference(kadtv2) from fn_test_not_nullable"

    // array_distinct
    order_qt_sql_array_distinct_Double "select array_distinct(kadbl) from fn_test"
    order_qt_sql_array_distinct_Double_notnull "select array_distinct(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_distinct_Float "select array_distinct(kafloat) from fn_test"
    order_qt_sql_array_distinct_Float_notnull "select array_distinct(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_distinct_LargeInt "select array_distinct(kalint) from fn_test"
    order_qt_sql_array_distinct_LargeInt_notnull "select array_distinct(kalint) from fn_test_not_nullable"
    order_qt_sql_array_distinct_BigInt "select array_distinct(kabint) from fn_test"
    order_qt_sql_array_distinct_BigInt_notnull "select array_distinct(kabint) from fn_test_not_nullable"
    order_qt_sql_array_distinct_SmallInt "select array_distinct(kasint) from fn_test"
    order_qt_sql_array_distinct_SmallInt_notnull "select array_distinct(kasint) from fn_test_not_nullable"
    order_qt_sql_array_distinct_Integer "select array_distinct(kaint) from fn_test"
    order_qt_sql_array_distinct_Integer_notnull "select array_distinct(kaint) from fn_test_not_nullable"
    order_qt_sql_array_distinct_TinyInt "select array_distinct(katint) from fn_test"
    order_qt_sql_array_distinct_TinyInt_notnull "select array_distinct(katint) from fn_test_not_nullable"
    order_qt_sql_array_distinct_DecimalV3 "select array_distinct(kadcml) from fn_test"
    order_qt_sql_array_distinct_DecimalV3_notnull "select array_distinct(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_distinct_Boolean "select array_distinct(kabool) from fn_test"
    order_qt_sql_array_distinct_Boolean_notnull "select array_distinct(kabool) from fn_test_not_nullable"

    order_qt_sql_array_distinct_Char "select array_distinct(kachr) from fn_test"
    order_qt_sql_array_distinct_Char_notnull "select array_distinct(kachr) from fn_test_not_nullable"
    order_qt_sql_array_distinct_Varchar "select array_distinct(kavchr) from fn_test"
    order_qt_sql_array_distinct_Varchar_notnull "select array_distinct(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_distinct_String "select array_distinct(kastr) from fn_test"
    order_qt_sql_array_distinct_String_notnull "select array_distinct(kastr) from fn_test_not_nullable"

    order_qt_sql_array_distinct_DatetimeV2 "select array_distinct(kadtmv2) from fn_test"
    order_qt_sql_array_distinct_DatetimeV2_notnull "select array_distinct(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_distinct_DateV2 "select array_distinct(kadtv2) from fn_test"
    order_qt_sql_array_distinct_DateV2_notnull "select array_distinct(kadtv2) from fn_test_not_nullable"

    // array_except
    order_qt_sql_array_except_Double "select array_except(kadbl, kadbl) from fn_test"
    order_qt_sql_array_except_Double_notnull "select array_except(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_except_Float "select array_except(kafloat, kafloat) from fn_test"
    order_qt_sql_array_except_Float_notnull "select array_except(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_except_LargeInt "select array_except(kalint, kalint) from fn_test"
    order_qt_sql_array_except_LargeInt_notnull "select array_except(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_array_except_BigInt "select array_except(kabint, kabint) from fn_test"
    order_qt_sql_array_except_BigInt_notnull "select array_except(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_array_except_SmallInt "select array_except(kasint, kasint) from fn_test"
    order_qt_sql_array_except_SmallInt_notnull "select array_except(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_array_except_Integer "select array_except(kaint, kaint) from fn_test"
    order_qt_sql_array_except_Integer_notnull "select array_except(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_array_except_TinyInt "select array_except(katint, katint) from fn_test"
    order_qt_sql_array_except_TinyInt_notnull "select array_except(katint, katint) from fn_test_not_nullable"
    order_qt_sql_array_except_DecimalV3 "select array_except(kadcml, kadcml) from fn_test"
    order_qt_sql_array_except_DecimalV3_notnull "select array_except(kadcml, kadcml) from fn_test_not_nullable"

    order_qt_sql_array_except_Boolean "select array_except(kabool, kabool) from fn_test"
    order_qt_sql_array_except_Boolean_notnull "select array_except(kabool, kabool) from fn_test_not_nullable"

    order_qt_sql_array_except_Char "select array_except(kachr, kachr) from fn_test"
    order_qt_sql_array_except_Char_notnull "select array_except(kachr, kachr) from fn_test_not_nullable"
    order_qt_sql_array_except_Varchar "select array_except(kavchr, kavchr) from fn_test"
    order_qt_sql_array_except_Varchar_notnull "select array_except(kavchr, kavchr) from fn_test_not_nullable"
    order_qt_sql_array_except_String "select array_except(kastr, kastr) from fn_test"
    order_qt_sql_array_except_String_notnull "select array_except(kastr, kastr) from fn_test_not_nullable"

    order_qt_sql_array_except_DatetimeV2 "select array_except(kadtmv2, kadtmv2) from fn_test"
    order_qt_sql_array_except_DatetimeV2_notnull "select array_except(kadtmv2, kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_except_DateV2 "select array_except(kadtv2, kadtv2) from fn_test"
    order_qt_sql_array_except_DateV2_notnull "select array_except(kadtv2, kadtv2) from fn_test_not_nullable"

    // array_intersect
    order_qt_sql_array_intersect_Double "select array_intersect(kadbl, kadbl) from fn_test"
    order_qt_sql_array_intersect_Double_notnull "select array_intersect(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_intersect_Float "select array_intersect(kafloat, kafloat) from fn_test"
    order_qt_sql_array_intersect_Float_notnull "select array_intersect(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_intersect_LargeInt "select array_intersect(kalint, kalint) from fn_test"
    order_qt_sql_array_intersect_LargeInt_notnull "select array_intersect(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_array_intersect_BigInt "select array_intersect(kabint, kabint) from fn_test"
    order_qt_sql_array_intersect_BigInt_notnull "select array_intersect(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_array_intersect_SmallInt "select array_intersect(kasint, kasint) from fn_test"
    order_qt_sql_array_intersect_SmallInt_notnull "select array_intersect(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_array_intersect_Integer "select array_intersect(kaint, kaint) from fn_test"
    order_qt_sql_array_intersect_Integer_notnull "select array_intersect(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_array_intersect_TinyInt "select array_intersect(katint, katint) from fn_test"
    order_qt_sql_array_intersect_TinyInt_notnull "select array_intersect(katint, katint) from fn_test_not_nullable"
    order_qt_sql_array_intersect_DecimalV3 "select array_intersect(kadcml, kadcml) from fn_test"
    order_qt_sql_array_intersect_DecimalV3_notnull "select array_intersect(kadcml, kadcml) from fn_test_not_nullable"

    order_qt_sql_array_intersect_Boolean "select array_intersect(kabool, kabool) from fn_test"
    order_qt_sql_array_intersect_Boolean_notnull "select array_intersect(kabool, kabool) from fn_test_not_nullable"

    order_qt_sql_array_intersect_Char "select array_intersect(kachr, kachr) from fn_test"
    order_qt_sql_array_intersect_Char_notnull "select array_intersect(kachr, kachr) from fn_test_not_nullable"
    order_qt_sql_array_intersect_Varchar "select array_intersect(kavchr, kavchr) from fn_test"
    order_qt_sql_array_intersect_Varchar_notnull "select array_intersect(kavchr, kavchr) from fn_test_not_nullable"
    order_qt_sql_array_intersect_String "select array_intersect(kastr, kastr) from fn_test"
    order_qt_sql_array_intersect_String_notnull "select array_intersect(kastr, kastr) from fn_test_not_nullable"

    order_qt_sql_array_intersect_DatetimeV2 "select array_intersect(kadtmv2, kadtmv2) from fn_test"
    order_qt_sql_array_intersect_DatetimeV2_notnull "select array_intersect(kadtmv2, kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_intersect_DateV2 "select array_intersect(kadtv2, kadtv2) from fn_test"
    order_qt_sql_array_intersect_DateV2_notnull "select array_intersect(kadtv2, kadtv2) from fn_test_not_nullable"

    // array_join
    order_qt_sql_array_join_Double "select array_join(kadbl, ',', 'null') from fn_test"
    order_qt_sql_array_join_Double_notnull "select array_join(kadbl, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_Float "select array_join(kafloat, ',', 'null') from fn_test"
    order_qt_sql_array_join_Float_notnull "select array_join(kafloat, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_LargeInt "select array_join(kalint, ',', 'null') from fn_test"
    order_qt_sql_array_join_LargeInt_notnull "select array_join(kalint, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_BigInt "select array_join(kabint, ',', 'null') from fn_test"
    order_qt_sql_array_join_BigInt_notnull "select array_join(kabint, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_SmallInt "select array_join(kasint, ',', 'null') from fn_test"
    order_qt_sql_array_join_SmallInt_notnull "select array_join(kasint, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_Integer "select array_join(kaint, ',', 'null') from fn_test"
    order_qt_sql_array_join_Integer_notnull "select array_join(kaint, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_TinyInt "select array_join(katint, ',', 'null') from fn_test"
    order_qt_sql_array_join_TinyInt_notnull "select array_join(katint, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_DecimalV3 "select array_join(kadcml, ',', 'null') from fn_test"
    order_qt_sql_array_join_DecimalV3_notnull "select array_join(kadcml, ',', 'null') from fn_test_not_nullable"

    order_qt_sql_array_join_Boolean "select array_join(kabool, ',', 'null') from fn_test"
    order_qt_sql_array_join_Boolean_notnull "select array_join(kabool, ',', 'null') from fn_test_not_nullable"

    order_qt_sql_array_join_Char "select array_join(kachr, ',', 'null') from fn_test"
    order_qt_sql_array_join_Char_notnull "select array_join(kachr, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_Varchar "select array_join(kavchr, ',', 'null') from fn_test"
    order_qt_sql_array_join_Varchar_notnull "select array_join(kavchr, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_String "select array_join(kastr, ',', 'null') from fn_test"
    order_qt_sql_array_join_String_notnull "select array_join(kastr, ',', 'null') from fn_test_not_nullable"

    order_qt_sql_array_join_DatetimeV2 "select array_join(kadtmv2, ',', 'null') from fn_test"
    order_qt_sql_array_join_DatetimeV2_notnull "select array_join(kadtmv2, ',', 'null') from fn_test_not_nullable"
    order_qt_sql_array_join_DateV2 "select array_join(kadtv2, ',', 'null') from fn_test"
    order_qt_sql_array_join_DateV2_notnull "select array_join(kadtv2, ',', 'null') from fn_test_not_nullable"

    order_qt_sql_array_join_two_params_Double "select array_join(kadbl, ',') from fn_test"
    order_qt_sql_array_join_two_params_Double_notnull "select array_join(kadbl, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_Float "select array_join(kafloat, ',') from fn_test"
    order_qt_sql_array_join_two_params_Float_notnull "select array_join(kafloat, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_LargeInt "select array_join(kalint, ',') from fn_test"
    order_qt_sql_array_join_two_params_LargeInt_notnull "select array_join(kalint, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_BigInt "select array_join(kabint, ',') from fn_test"
    order_qt_sql_array_join_two_params_BigInt_notnull "select array_join(kabint, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_SmallInt "select array_join(kasint, ',') from fn_test"
    order_qt_sql_array_join_two_params_SmallInt_notnull "select array_join(kasint, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_Integer "select array_join(kaint, ',') from fn_test"
    order_qt_sql_array_join_two_params_Integer_notnull "select array_join(kaint, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_TinyInt "select array_join(katint, ',') from fn_test"
    order_qt_sql_array_join_two_params_TinyInt_notnull "select array_join(katint, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_DecimalV3 "select array_join(kadcml, ',') from fn_test"
    order_qt_sql_array_join_two_params_DecimalV3_notnull "select array_join(kadcml, ',') from fn_test_not_nullable"

    order_qt_sql_array_join_two_params_Boolean "select array_join(kabool, ',') from fn_test"
    order_qt_sql_array_join_two_params_Boolean_notnull "select array_join(kabool, ',') from fn_test_not_nullable"

    order_qt_sql_array_join_two_params_Char "select array_join(kachr, ',') from fn_test"
    order_qt_sql_array_join_two_params_Char_notnull "select array_join(kachr, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_Varchar "select array_join(kavchr, ',') from fn_test"
    order_qt_sql_array_join_two_params_Varchar_notnull "select array_join(kavchr, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_String "select array_join(kastr, ',') from fn_test"
    order_qt_sql_array_join_two_params_String_notnull "select array_join(kastr, ',') from fn_test_not_nullable"

    order_qt_sql_array_join_two_params_DatetimeV2 "select array_join(kadtmv2, ',') from fn_test"
    order_qt_sql_array_join_two_params_DatetimeV2_notnull "select array_join(kadtmv2, ',') from fn_test_not_nullable"
    order_qt_sql_array_join_two_params_DateV2 "select array_join(kadtv2, ',') from fn_test"
    order_qt_sql_array_join_two_params_DateV2_notnull "select array_join(kadtv2, ',') from fn_test_not_nullable"

    // l1_distance
    order_qt_sql_l1_distance_Double "select l1_distance(kadbl, kadbl) from fn_test"
    order_qt_sql_l1_distance_Double_notnull "select l1_distance(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_l1_distance_Float "select l1_distance(kafloat, kafloat) from fn_test"
    order_qt_sql_l1_distance_Float_notnull "select l1_distance(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_l1_distance_LargeInt "select l1_distance(kalint, kalint) from fn_test"
    order_qt_sql_l1_distance_LargeInt_notnull "select l1_distance(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_l1_distance_BigInt "select l1_distance(kabint, kabint) from fn_test"
    order_qt_sql_l1_distance_BigInt_notnull "select l1_distance(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_l1_distance_SmallInt "select l1_distance(kasint, kasint) from fn_test"
    order_qt_sql_l1_distance_SmallInt_notnull "select l1_distance(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_l1_distance_Integer "select l1_distance(kaint, kaint) from fn_test"
    order_qt_sql_l1_distance_Integer_notnull "select l1_distance(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_l1_distance_TinyInt "select l1_distance(katint, katint) from fn_test"
    order_qt_sql_l1_distance_TinyInt_notnull "select l1_distance(katint, katint) from fn_test_not_nullable"

    // l2_distance
    order_qt_sql_l2_distance_Double "select l2_distance(kadbl, kadbl) from fn_test"
    order_qt_sql_l2_distance_Double_notnull "select l2_distance(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_l2_distance_Float "select l2_distance(kafloat, kafloat) from fn_test"
    order_qt_sql_l2_distance_Float_notnull "select l2_distance(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_l2_distance_LargeInt "select l2_distance(kalint, kalint) from fn_test"
    order_qt_sql_l2_distance_LargeInt_notnull "select l2_distance(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_l2_distance_BigInt "select l2_distance(kabint, kabint) from fn_test"
    order_qt_sql_l2_distance_BigInt_notnull "select l2_distance(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_l2_distance_SmallInt "select l2_distance(kasint, kasint) from fn_test"
    order_qt_sql_l2_distance_SmallInt_notnull "select l2_distance(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_l2_distance_Integer "select l2_distance(kaint, kaint) from fn_test"
    order_qt_sql_l2_distance_Integer_notnull "select l2_distance(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_l2_distance_TinyInt "select l2_distance(katint, katint) from fn_test"
    order_qt_sql_l2_distance_TinyInt_notnull "select l2_distance(katint, katint) from fn_test_not_nullable"

    // cosine_distance
    order_qt_sql_cosine_distance_Double "select cosine_distance(kadbl, kadbl) from fn_test"
    order_qt_sql_cosine_distance_Double_notnull "select cosine_distance(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_Float "select cosine_distance(kafloat, kafloat) from fn_test"
    order_qt_sql_cosine_distance_Float_notnull "select cosine_distance(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_LargeInt "select cosine_distance(kalint, kalint) from fn_test"
    order_qt_sql_cosine_distance_LargeInt_notnull "select cosine_distance(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_BigInt "select cosine_distance(kabint, kabint) from fn_test"
    order_qt_sql_cosine_distance_BigInt_notnull "select cosine_distance(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_SmallInt "select cosine_distance(kasint, kasint) from fn_test"
    order_qt_sql_cosine_distance_SmallInt_notnull "select cosine_distance(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_Integer "select cosine_distance(kaint, kaint) from fn_test"
    order_qt_sql_cosine_distance_Integer_notnull "select cosine_distance(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_cosine_distance_TinyInt "select cosine_distance(katint, katint) from fn_test"
    order_qt_sql_cosine_distance_TinyInt_notnull "select cosine_distance(katint, katint) from fn_test_not_nullable"

    // inner_product
    order_qt_sql_inner_product_Double "select inner_product(kadbl, kadbl) from fn_test"
    order_qt_sql_inner_product_Double_notnull "select inner_product(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_inner_product_Float "select inner_product(kafloat, kafloat) from fn_test"
    order_qt_sql_inner_product_Float_notnull "select inner_product(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_inner_product_LargeInt "select inner_product(kalint, kalint) from fn_test"
    order_qt_sql_inner_product_LargeInt_notnull "select inner_product(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_inner_product_BigInt "select inner_product(kabint, kabint) from fn_test"
    order_qt_sql_inner_product_BigInt_notnull "select inner_product(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_inner_product_SmallInt "select inner_product(kasint, kasint) from fn_test"
    order_qt_sql_inner_product_SmallInt_notnull "select inner_product(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_inner_product_Integer "select inner_product(kaint, kaint) from fn_test"
    order_qt_sql_inner_product_Integer_notnull "select inner_product(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_inner_product_TinyInt "select inner_product(katint, katint) from fn_test"
    order_qt_sql_inner_product_TinyInt_notnull "select inner_product(katint, katint) from fn_test_not_nullable"

    // array_max
    order_qt_sql_array_max_Double "select array_max(kadbl) from fn_test"
    order_qt_sql_array_max_Double_notnull "select array_max(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_max_Float "select array_max(kafloat) from fn_test"
    order_qt_sql_array_max_Float_notnull "select array_max(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_max_LargeInt "select array_max(kalint) from fn_test"
    order_qt_sql_array_max_LargeInt_notnull "select array_max(kalint) from fn_test_not_nullable"
    order_qt_sql_array_max_BigInt "select array_max(kabint) from fn_test"
    order_qt_sql_array_max_BigInt_notnull "select array_max(kabint) from fn_test_not_nullable"
    order_qt_sql_array_max_SmallInt "select array_max(kasint) from fn_test"
    order_qt_sql_array_max_SmallInt_notnull "select array_max(kasint) from fn_test_not_nullable"
    order_qt_sql_array_max_Integer "select array_max(kaint) from fn_test"
    order_qt_sql_array_max_Integer_notnull "select array_max(kaint) from fn_test_not_nullable"
    order_qt_sql_array_max_TinyInt "select array_max(katint) from fn_test"
    order_qt_sql_array_max_TinyInt_notnull "select array_max(katint) from fn_test_not_nullable"
    order_qt_sql_array_max_DecimalV3 "select array_max(kadcml) from fn_test"
    order_qt_sql_array_max_DecimalV3_notnull "select array_max(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_max_Boolean "select array_max(kabool) from fn_test"
    order_qt_sql_array_max_Boolean_notnull "select array_max(kabool) from fn_test_not_nullable"

    // TODO enable it when be support character like type
    // order_qt_sql_array_max_Char "select array_max(kachr) from fn_test"
    // order_qt_sql_array_max_Char_notnull "select array_max(kachr) from fn_test_not_nullable"
    // order_qt_sql_array_max_Varchar "select array_max(kavchr) from fn_test"
    // order_qt_sql_array_max_Varchar_notnull "select array_max(kavchr) from fn_test_not_nullable"
    // order_qt_sql_array_max_String "select array_max(kastr) from fn_test"
    // order_qt_sql_array_max_String_notnull "select array_max(kastr) from fn_test_not_nullable"

    order_qt_sql_array_max_DatetimeV2 "select array_max(kadtmv2) from fn_test"
    order_qt_sql_array_max_DatetimeV2_notnull "select array_max(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_max_DateV2 "select array_max(kadtv2) from fn_test"
    order_qt_sql_array_max_DateV2_notnull "select array_max(kadtv2) from fn_test_not_nullable"

    // array_min
    order_qt_sql_array_min_Double "select array_min(kadbl) from fn_test"
    order_qt_sql_array_min_Double_notnull "select array_min(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_min_Float "select array_min(kafloat) from fn_test"
    order_qt_sql_array_min_Float_notnull "select array_min(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_min_LargeInt "select array_min(kalint) from fn_test"
    order_qt_sql_array_min_LargeInt_notnull "select array_min(kalint) from fn_test_not_nullable"
    order_qt_sql_array_min_BigInt "select array_min(kabint) from fn_test"
    order_qt_sql_array_min_BigInt_notnull "select array_min(kabint) from fn_test_not_nullable"
    order_qt_sql_array_min_SmallInt "select array_min(kasint) from fn_test"
    order_qt_sql_array_min_SmallInt_notnull "select array_min(kasint) from fn_test_not_nullable"
    order_qt_sql_array_min_Integer "select array_min(kaint) from fn_test"
    order_qt_sql_array_min_Integer_notnull "select array_min(kaint) from fn_test_not_nullable"
    order_qt_sql_array_min_TinyInt "select array_min(katint) from fn_test"
    order_qt_sql_array_min_TinyInt_notnull "select array_min(katint) from fn_test_not_nullable"
    order_qt_sql_array_min_DecimalV3 "select array_min(kadcml) from fn_test"
    order_qt_sql_array_min_DecimalV3_notnull "select array_min(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_min_Boolean "select array_min(kabool) from fn_test"
    order_qt_sql_array_min_Boolean_notnull "select array_min(kabool) from fn_test_not_nullable"

    // TODO enable it when be support character like type
    // order_qt_sql_array_min_Char "select array_min(kachr) from fn_test"
    // order_qt_sql_array_min_Char_notnull "select array_min(kachr) from fn_test_not_nullable"
    // order_qt_sql_array_min_Varchar "select array_min(kavchr) from fn_test"
    // order_qt_sql_array_min_Varchar_notnull "select array_min(kavchr) from fn_test_not_nullable"
    // order_qt_sql_array_min_String "select array_min(kastr) from fn_test"
    // order_qt_sql_array_min_String_notnull "select array_min(kastr) from fn_test_not_nullable"

    order_qt_sql_array_min_DatetimeV2 "select array_min(kadtmv2) from fn_test"
    order_qt_sql_array_min_DatetimeV2_notnull "select array_min(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_min_DateV2 "select array_min(kadtv2) from fn_test"
    order_qt_sql_array_min_DateV2_notnull "select array_min(kadtv2) from fn_test_not_nullable"

    // array_popback
    order_qt_sql_array_popback_Double "select array_popback(kadbl) from fn_test"
    order_qt_sql_array_popback_Double_notnull "select array_popback(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_popback_Float "select array_popback(kafloat) from fn_test"
    order_qt_sql_array_popback_Float_notnull "select array_popback(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_popback_LargeInt "select array_popback(kalint) from fn_test"
    order_qt_sql_array_popback_LargeInt_notnull "select array_popback(kalint) from fn_test_not_nullable"
    order_qt_sql_array_popback_BigInt "select array_popback(kabint) from fn_test"
    order_qt_sql_array_popback_BigInt_notnull "select array_popback(kabint) from fn_test_not_nullable"
    order_qt_sql_array_popback_SmallInt "select array_popback(kasint) from fn_test"
    order_qt_sql_array_popback_SmallInt_notnull "select array_popback(kasint) from fn_test_not_nullable"
    order_qt_sql_array_popback_Integer "select array_popback(kaint) from fn_test"
    order_qt_sql_array_popback_Integer_notnull "select array_popback(kaint) from fn_test_not_nullable"
    order_qt_sql_array_popback_TinyInt "select array_popback(katint) from fn_test"
    order_qt_sql_array_popback_TinyInt_notnull "select array_popback(katint) from fn_test_not_nullable"
    order_qt_sql_array_popback_DecimalV3 "select array_popback(kadcml) from fn_test"
    order_qt_sql_array_popback_DecimalV3_notnull "select array_popback(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_popback_Boolean "select array_popback(kabool) from fn_test"
    order_qt_sql_array_popback_Boolean_notnull "select array_popback(kabool) from fn_test_not_nullable"

    order_qt_sql_array_popback_Char "select array_popback(kachr) from fn_test"
    order_qt_sql_array_popback_Char_notnull "select array_popback(kachr) from fn_test_not_nullable"
    order_qt_sql_array_popback_Varchar "select array_popback(kavchr) from fn_test"
    order_qt_sql_array_popback_Varchar_notnull "select array_popback(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_popback_String "select array_popback(kastr) from fn_test"
    order_qt_sql_array_popback_String_notnull "select array_popback(kastr) from fn_test_not_nullable"

    order_qt_sql_array_popback_DatetimeV2 "select array_popback(kadtmv2) from fn_test"
    order_qt_sql_array_popback_DatetimeV2_notnull "select array_popback(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_popback_DateV2 "select array_popback(kadtv2) from fn_test"
    order_qt_sql_array_popback_DateV2_notnull "select array_popback(kadtv2) from fn_test_not_nullable"

    // array_popfront
    order_qt_sql_array_popfront_Double "select array_popfront(kadbl) from fn_test"
    order_qt_sql_array_popfront_Double_notnull "select array_popfront(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_popfront_Float "select array_popfront(kafloat) from fn_test"
    order_qt_sql_array_popfront_Float_notnull "select array_popfront(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_popfront_LargeInt "select array_popfront(kalint) from fn_test"
    order_qt_sql_array_popfront_LargeInt_notnull "select array_popfront(kalint) from fn_test_not_nullable"
    order_qt_sql_array_popfront_BigInt "select array_popfront(kabint) from fn_test"
    order_qt_sql_array_popfront_BigInt_notnull "select array_popfront(kabint) from fn_test_not_nullable"
    order_qt_sql_array_popfront_SmallInt "select array_popfront(kasint) from fn_test"
    order_qt_sql_array_popfront_SmallInt_notnull "select array_popfront(kasint) from fn_test_not_nullable"
    order_qt_sql_array_popfront_Integer "select array_popfront(kaint) from fn_test"
    order_qt_sql_array_popfront_Integer_notnull "select array_popfront(kaint) from fn_test_not_nullable"
    order_qt_sql_array_popfront_TinyInt "select array_popfront(katint) from fn_test"
    order_qt_sql_array_popfront_TinyInt_notnull "select array_popfront(katint) from fn_test_not_nullable"
    order_qt_sql_array_popfront_DecimalV3 "select array_popfront(kadcml) from fn_test"
    order_qt_sql_array_popfront_DecimalV3_notnull "select array_popfront(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_popfront_Boolean "select array_popfront(kabool) from fn_test"
    order_qt_sql_array_popfront_Boolean_notnull "select array_popfront(kabool) from fn_test_not_nullable"

    order_qt_sql_array_popfront_Char "select array_popfront(kachr) from fn_test"
    order_qt_sql_array_popfront_Char_notnull "select array_popfront(kachr) from fn_test_not_nullable"
    order_qt_sql_array_popfront_Varchar "select array_popfront(kavchr) from fn_test"
    order_qt_sql_array_popfront_Varchar_notnull "select array_popfront(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_popfront_String "select array_popfront(kastr) from fn_test"
    order_qt_sql_array_popfront_String_notnull "select array_popfront(kastr) from fn_test_not_nullable"

    order_qt_sql_array_popfront_DatetimeV2 "select array_popfront(kadtmv2) from fn_test"
    order_qt_sql_array_popfront_DatetimeV2_notnull "select array_popfront(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_popfront_DateV2 "select array_popfront(kadtv2) from fn_test"
    order_qt_sql_array_popfront_DateV2_notnull "select array_popfront(kadtv2) from fn_test_not_nullable"

    // array_position
    order_qt_sql_array_position_Double "select array_position(kadbl, kdbl) from fn_test"
    order_qt_sql_array_position_Double_notnull "select array_position(kadbl, kdbl) from fn_test_not_nullable"
    order_qt_sql_array_position_Float "select array_position(kafloat, kfloat) from fn_test"
    order_qt_sql_array_position_Float_notnull "select array_position(kafloat, kfloat) from fn_test_not_nullable"
    order_qt_sql_array_position_LargeInt "select array_position(kalint, klint) from fn_test"
    order_qt_sql_array_position_LargeInt_notnull "select array_position(kalint, klint) from fn_test_not_nullable"
    order_qt_sql_array_position_BigInt "select array_position(kabint, kbint) from fn_test"
    order_qt_sql_array_position_BigInt_notnull "select array_position(kabint, kbint) from fn_test_not_nullable"
    order_qt_sql_array_position_SmallInt "select array_position(kasint, ksint) from fn_test"
    order_qt_sql_array_position_SmallInt_notnull "select array_position(kasint, ksint) from fn_test_not_nullable"
    order_qt_sql_array_position_Integer "select array_position(kaint, kint) from fn_test"
    order_qt_sql_array_position_Integer_notnull "select array_position(kaint, kint) from fn_test_not_nullable"
    order_qt_sql_array_position_TinyInt "select array_position(katint, ktint) from fn_test"
    order_qt_sql_array_position_TinyInt_notnull "select array_position(katint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_position_DecimalV3 "select array_position(kadcml, kdcmls1) from fn_test"
    order_qt_sql_array_position_DecimalV3_notnull "select array_position(kadcml, kdcmls1) from fn_test_not_nullable"

    order_qt_sql_array_position_Boolean "select array_position(kabool, kbool) from fn_test"
    order_qt_sql_array_position_Boolean_notnull "select array_position(kabool, kbool) from fn_test_not_nullable"

    order_qt_sql_array_position_Char "select array_position(kachr, kchrs1) from fn_test"
    order_qt_sql_array_position_Char_notnull "select array_position(kachr, kchrs1) from fn_test_not_nullable"
    order_qt_sql_array_position_Varchar "select array_position(kavchr, kvchrs1) from fn_test"
    order_qt_sql_array_position_Varchar_notnull "select array_position(kavchr, kvchrs1) from fn_test_not_nullable"
    order_qt_sql_array_position_String "select array_position(kastr, kstr) from fn_test"
    order_qt_sql_array_position_String_notnull "select array_position(kastr, kstr) from fn_test_not_nullable"

    order_qt_sql_array_position_DatetimeV2 "select array_position(kadtmv2, kdtmv2s1) from fn_test"
    order_qt_sql_array_position_DatetimeV2_notnull "select array_position(kadtmv2, kdtmv2s1) from fn_test_not_nullable"
    order_qt_sql_array_position_DateV2 "select array_position(kadtv2, kdtv2) from fn_test"
    order_qt_sql_array_position_DateV2_notnull "select array_position(kadtv2, kdtv2) from fn_test_not_nullable"

    // array_product
    order_qt_sql_array_product_Double "select array_product(kadbl) from fn_test"
    order_qt_sql_array_product_Double_notnull "select array_product(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_product_Float "select array_product(kafloat) from fn_test"
    order_qt_sql_array_product_Float_notnull "select array_product(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_product_LargeInt "select array_product(kalint) from fn_test"
    order_qt_sql_array_product_LargeInt_notnull "select array_product(kalint) from fn_test_not_nullable"
    order_qt_sql_array_product_BigInt "select array_product(kabint) from fn_test"
    order_qt_sql_array_product_BigInt_notnull "select array_product(kabint) from fn_test_not_nullable"
    order_qt_sql_array_product_SmallInt "select array_product(kasint) from fn_test"
    order_qt_sql_array_product_SmallInt_notnull "select array_product(kasint) from fn_test_not_nullable"
    order_qt_sql_array_product_Integer "select array_product(kaint) from fn_test"
    order_qt_sql_array_product_Integer_notnull "select array_product(kaint) from fn_test_not_nullable"
    order_qt_sql_array_product_TinyInt "select array_product(katint) from fn_test"
    order_qt_sql_array_product_TinyInt_notnull "select array_product(katint) from fn_test_not_nullable"
    order_qt_sql_array_product_DecimalV3 "select array_product(kadcml) from fn_test"
    order_qt_sql_array_product_DecimalV3_notnull "select array_product(kadcml) from fn_test_not_nullable"

    // array_pushback
    order_qt_sql_array_pushback_Double "select array_pushback(kadbl, kdbl) from fn_test"
    order_qt_sql_array_pushback_Double_notnull "select array_pushback(kadbl, kdbl) from fn_test_not_nullable"
    order_qt_sql_array_pushback_Float "select array_pushback(kafloat, kfloat) from fn_test"
    order_qt_sql_array_pushback_Float_notnull "select array_pushback(kafloat, kfloat) from fn_test_not_nullable"
    order_qt_sql_array_pushback_LargeInt "select array_pushback(kalint, klint) from fn_test"
    order_qt_sql_array_pushback_LargeInt_notnull "select array_pushback(kalint, klint) from fn_test_not_nullable"
    order_qt_sql_array_pushback_BigInt "select array_pushback(kabint, kbint) from fn_test"
    order_qt_sql_array_pushback_BigInt_notnull "select array_pushback(kabint, kbint) from fn_test_not_nullable"
    order_qt_sql_array_pushback_SmallInt "select array_pushback(kasint, ksint) from fn_test"
    order_qt_sql_array_pushback_SmallInt_notnull "select array_pushback(kasint, ksint) from fn_test_not_nullable"
    order_qt_sql_array_pushback_Integer "select array_pushback(kaint, kint) from fn_test"
    order_qt_sql_array_pushback_Integer_notnull "select array_pushback(kaint, kint) from fn_test_not_nullable"
    order_qt_sql_array_pushback_TinyInt "select array_pushback(katint, ktint) from fn_test"
    order_qt_sql_array_pushback_TinyInt_notnull "select array_pushback(katint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_pushback_DecimalV3 "select array_pushback(kadcml, kdcmls1) from fn_test"
    order_qt_sql_array_pushback_DecimalV3_notnull "select array_pushback(kadcml, kdcmls1) from fn_test_not_nullable"

    order_qt_sql_array_pushback_Boolean "select array_pushback(kabool, kbool) from fn_test"
    order_qt_sql_array_pushback_Boolean_notnull "select array_pushback(kabool, kbool) from fn_test_not_nullable"

    order_qt_sql_array_pushback_Char "select array_pushback(kachr, kchrs1) from fn_test"
    order_qt_sql_array_pushback_Char_notnull "select array_pushback(kachr, kchrs1) from fn_test_not_nullable"
    order_qt_sql_array_pushback_Varchar "select array_pushback(kavchr, kvchrs1) from fn_test"
    order_qt_sql_array_pushback_Varchar_notnull "select array_pushback(kavchr, kvchrs1) from fn_test_not_nullable"
    order_qt_sql_array_pushback_String "select array_pushback(kastr, kstr) from fn_test"
    order_qt_sql_array_pushback_String_notnull "select array_pushback(kastr, kstr) from fn_test_not_nullable"

    order_qt_sql_array_pushback_DatetimeV2 "select array_pushback(kadtmv2, kdtmv2s1) from fn_test"
    order_qt_sql_array_pushback_DatetimeV2_notnull "select array_pushback(kadtmv2, kdtmv2s1) from fn_test_not_nullable"
    order_qt_sql_array_pushback_DateV2 "select array_pushback(kadtv2, kdtv2) from fn_test"
    order_qt_sql_array_pushback_DateV2_notnull "select array_pushback(kadtv2, kdtv2) from fn_test_not_nullable"

    // array_pushfront
    order_qt_sql_array_pushfront_Double "select array_pushfront(kadbl, kdbl) from fn_test"
    order_qt_sql_array_pushfront_Double_notnull "select array_pushfront(kadbl, kdbl) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_Float "select array_pushfront(kafloat, kfloat) from fn_test"
    order_qt_sql_array_pushfront_Float_notnull "select array_pushfront(kafloat, kfloat) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_LargeInt "select array_pushfront(kalint, klint) from fn_test"
    order_qt_sql_array_pushfront_LargeInt_notnull "select array_pushfront(kalint, klint) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_BigInt "select array_pushfront(kabint, kbint) from fn_test"
    order_qt_sql_array_pushfront_BigInt_notnull "select array_pushfront(kabint, kbint) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_SmallInt "select array_pushfront(kasint, ksint) from fn_test"
    order_qt_sql_array_pushfront_SmallInt_notnull "select array_pushfront(kasint, ksint) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_Integer "select array_pushfront(kaint, kint) from fn_test"
    order_qt_sql_array_pushfront_Integer_notnull "select array_pushfront(kaint, kint) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_TinyInt "select array_pushfront(katint, ktint) from fn_test"
    order_qt_sql_array_pushfront_TinyInt_notnull "select array_pushfront(katint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_DecimalV3 "select array_pushfront(kadcml, kdcmls1) from fn_test"
    order_qt_sql_array_pushfront_DecimalV3_notnull "select array_pushfront(kadcml, kdcmls1) from fn_test_not_nullable"

    order_qt_sql_array_pushfront_Boolean "select array_pushfront(kabool, kbool) from fn_test"
    order_qt_sql_array_pushfront_Boolean_notnull "select array_pushfront(kabool, kbool) from fn_test_not_nullable"

    order_qt_sql_array_pushfront_Char "select array_pushfront(kachr, kchrs1) from fn_test"
    order_qt_sql_array_pushfront_Char_notnull "select array_pushfront(kachr, kchrs1) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_Varchar "select array_pushfront(kavchr, kvchrs1) from fn_test"
    order_qt_sql_array_pushfront_Varchar_notnull "select array_pushfront(kavchr, kvchrs1) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_String "select array_pushfront(kastr, kstr) from fn_test"
    order_qt_sql_array_pushfront_String_notnull "select array_pushfront(kastr, kstr) from fn_test_not_nullable"

    order_qt_sql_array_pushfront_DatetimeV2 "select array_pushfront(kadtmv2, kdtmv2s1) from fn_test"
    order_qt_sql_array_pushfront_DatetimeV2_notnull "select array_pushfront(kadtmv2, kdtmv2s1) from fn_test_not_nullable"
    order_qt_sql_array_pushfront_DateV2 "select array_pushfront(kadtv2, kdtv2) from fn_test"
    order_qt_sql_array_pushfront_DateV2_notnull "select array_pushfront(kadtv2, kdtv2) from fn_test_not_nullable"

    // array_range
    order_qt_sql_array_range_one_param "select array_range(kint) from fn_test order by id"
    order_qt_sql_array_range_one_param_notnull "select array_range(kint) from fn_test_not_nullable order by id"
    order_qt_sql_array_range_two_param "select array_range(kint, 1000) from fn_test order by id"
    order_qt_sql_array_range_two_param_notnull "select array_range(kint, 1000) from fn_test_not_nullable order by id"
    order_qt_sql_array_range_three_param "select array_range(kint, 10000, ktint) from fn_test order by id"
    order_qt_sql_array_range_three_param_notnull "select array_range(kint, 10000, ktint) from fn_test_not_nullable order by id"
    // make a large size of array element, expect to throw error
    test  {
        sql "select array_range(kint, 1000000000) from fn_test"
        exception ('Array size exceeds the limit 1000000')
    }

    // array_remove
    order_qt_sql_array_remove_Double "select array_remove(kadbl, kdbl) from fn_test"
    order_qt_sql_array_remove_Double_notnull "select array_remove(kadbl, kdbl) from fn_test_not_nullable"
    order_qt_sql_array_remove_Float "select array_remove(kafloat, kfloat) from fn_test"
    order_qt_sql_array_remove_Float_notnull "select array_remove(kafloat, kfloat) from fn_test_not_nullable"
    order_qt_sql_array_remove_LargeInt "select array_remove(kalint, klint) from fn_test"
    order_qt_sql_array_remove_LargeInt_notnull "select array_remove(kalint, klint) from fn_test_not_nullable"
    order_qt_sql_array_remove_BigInt "select array_remove(kabint, kbint) from fn_test"
    order_qt_sql_array_remove_BigInt_notnull "select array_remove(kabint, kbint) from fn_test_not_nullable"
    order_qt_sql_array_remove_SmallInt "select array_remove(kasint, ksint) from fn_test"
    order_qt_sql_array_remove_SmallInt_notnull "select array_remove(kasint, ksint) from fn_test_not_nullable"
    order_qt_sql_array_remove_Integer "select array_remove(kaint, kint) from fn_test"
    order_qt_sql_array_remove_Integer_notnull "select array_remove(kaint, kint) from fn_test_not_nullable"
    order_qt_sql_array_remove_TinyInt "select array_remove(katint, ktint) from fn_test"
    order_qt_sql_array_remove_TinyInt_notnull "select array_remove(katint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_remove_DecimalV3 "select array_remove(kadcml, kdcmls1) from fn_test"
    order_qt_sql_array_remove_DecimalV3_notnull "select array_remove(kadcml, kdcmls1) from fn_test_not_nullable"

    order_qt_sql_array_remove_Boolean "select array_remove(kabool, kbool) from fn_test"
    order_qt_sql_array_remove_Boolean_notnull "select array_remove(kabool, kbool) from fn_test_not_nullable"

    order_qt_sql_array_remove_Char "select array_remove(kachr, kchrs1) from fn_test"
    order_qt_sql_array_remove_Char_notnull "select array_remove(kachr, kchrs1) from fn_test_not_nullable"
    order_qt_sql_array_remove_Varchar "select array_remove(kavchr, kvchrs1) from fn_test"
    order_qt_sql_array_remove_Varchar_notnull "select array_remove(kavchr, kvchrs1) from fn_test_not_nullable"
    order_qt_sql_array_remove_String "select array_remove(kastr, kstr) from fn_test"
    order_qt_sql_array_remove_String_notnull "select array_remove(kastr, kstr) from fn_test_not_nullable"

    order_qt_sql_array_remove_DatetimeV2 "select array_remove(kadtmv2, kdtmv2s1) from fn_test"
    order_qt_sql_array_remove_DatetimeV2_notnull "select array_remove(kadtmv2, kdtmv2s1) from fn_test_not_nullable"
    order_qt_sql_array_remove_DateV2 "select array_remove(kadtv2, kdtv2) from fn_test"
    order_qt_sql_array_remove_DateV2_notnull "select array_remove(kadtv2, kdtv2) from fn_test_not_nullable"

    // array_reverse_sort
    order_qt_sql_array_reverse_sort_Double "select array_reverse_sort(kadbl) from fn_test"
    order_qt_sql_array_reverse_sort_Double_notnull "select array_reverse_sort(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_Float "select array_reverse_sort(kafloat) from fn_test"
    order_qt_sql_array_reverse_sort_Float_notnull "select array_reverse_sort(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_LargeInt "select array_reverse_sort(kalint) from fn_test"
    order_qt_sql_array_reverse_sort_LargeInt_notnull "select array_reverse_sort(kalint) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_BigInt "select array_reverse_sort(kabint) from fn_test"
    order_qt_sql_array_reverse_sort_BigInt_notnull "select array_reverse_sort(kabint) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_SmallInt "select array_reverse_sort(kasint) from fn_test"
    order_qt_sql_array_reverse_sort_SmallInt_notnull "select array_reverse_sort(kasint) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_Integer "select array_reverse_sort(kaint) from fn_test"
    order_qt_sql_array_reverse_sort_Integer_notnull "select array_reverse_sort(kaint) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_TinyInt "select array_reverse_sort(katint) from fn_test"
    order_qt_sql_array_reverse_sort_TinyInt_notnull "select array_reverse_sort(katint) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_DecimalV3 "select array_reverse_sort(kadcml) from fn_test"
    order_qt_sql_array_reverse_sort_DecimalV3_notnull "select array_reverse_sort(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_reverse_sort_Boolean "select array_reverse_sort(kabool) from fn_test"
    order_qt_sql_array_reverse_sort_Boolean_notnull "select array_reverse_sort(kabool) from fn_test_not_nullable"

    order_qt_sql_array_reverse_sort_Char "select array_reverse_sort(kachr) from fn_test"
    order_qt_sql_array_reverse_sort_Char_notnull "select array_reverse_sort(kachr) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_Varchar "select array_reverse_sort(kavchr) from fn_test"
    order_qt_sql_array_reverse_sort_Varchar_notnull "select array_reverse_sort(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_String "select array_reverse_sort(kastr) from fn_test"
    order_qt_sql_array_reverse_sort_String_notnull "select array_reverse_sort(kastr) from fn_test_not_nullable"

    order_qt_sql_array_reverse_sort_DatetimeV2 "select array_reverse_sort(kadtmv2) from fn_test"
    order_qt_sql_array_reverse_sort_DatetimeV2_notnull "select array_reverse_sort(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_reverse_sort_DateV2 "select array_reverse_sort(kadtv2) from fn_test"
    order_qt_sql_array_reverse_sort_DateV2_notnull "select array_reverse_sort(kadtv2) from fn_test_not_nullable"

    // array_slice
    order_qt_sql_array_slice_Double "select array_slice(kadbl, kint) from fn_test"
    order_qt_sql_array_slice_Double_notnull "select array_slice(kadbl, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_Float "select array_slice(kafloat, kint) from fn_test"
    order_qt_sql_array_slice_Float_notnull "select array_slice(kafloat, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_LargeInt "select array_slice(kalint, kint) from fn_test"
    order_qt_sql_array_slice_LargeInt_notnull "select array_slice(kalint, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_BigInt "select array_slice(kabint, kint) from fn_test"
    order_qt_sql_array_slice_BigInt_notnull "select array_slice(kabint, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_SmallInt "select array_slice(kasint, kint) from fn_test"
    order_qt_sql_array_slice_SmallInt_notnull "select array_slice(kasint, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_Integer "select array_slice(kaint, kint) from fn_test"
    order_qt_sql_array_slice_Integer_notnull "select array_slice(kaint, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_TinyInt "select array_slice(katint, kint) from fn_test"
    order_qt_sql_array_slice_TinyInt_notnull "select array_slice(katint, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_DecimalV3 "select array_slice(kadcml, kint) from fn_test"
    order_qt_sql_array_slice_DecimalV3_notnull "select array_slice(kadcml, kint) from fn_test_not_nullable"

    order_qt_sql_array_slice_Boolean "select array_slice(kabool, kint) from fn_test"
    order_qt_sql_array_slice_Boolean_notnull "select array_slice(kabool, kint) from fn_test_not_nullable"

    order_qt_sql_array_slice_Char "select array_slice(kachr, kint) from fn_test"
    order_qt_sql_array_slice_Char_notnull "select array_slice(kachr, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_Varchar "select array_slice(kavchr, kint) from fn_test"
    order_qt_sql_array_slice_Varchar_notnull "select array_slice(kavchr, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_String "select array_slice(kastr, kint) from fn_test"
    order_qt_sql_array_slice_String_notnull "select array_slice(kastr, kint) from fn_test_not_nullable"

    order_qt_sql_array_slice_DatetimeV2 "select array_slice(kadtmv2, kint) from fn_test"
    order_qt_sql_array_slice_DatetimeV2_notnull "select array_slice(kadtmv2, kint) from fn_test_not_nullable"
    order_qt_sql_array_slice_DateV2 "select array_slice(kadtv2, kint) from fn_test"
    order_qt_sql_array_slice_DateV2_notnull "select array_slice(kadtv2, kint) from fn_test_not_nullable"

    // array_slice
    order_qt_sql_array_slice_three_params_Double "select array_slice(kadbl, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Double_notnull "select array_slice(kadbl, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_Float "select array_slice(kafloat, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Float_notnull "select array_slice(kafloat, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_LargeInt "select array_slice(kalint, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_LargeInt_notnull "select array_slice(kalint, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_BigInt "select array_slice(kabint, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_BigInt_notnull "select array_slice(kabint, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_SmallInt "select array_slice(kasint, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_SmallInt_notnull "select array_slice(kasint, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_Integer "select array_slice(kaint, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Integer_notnull "select array_slice(kaint, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_TinyInt "select array_slice(katint, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_TinyInt_notnull "select array_slice(katint, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_DecimalV3 "select array_slice(kadcml, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_DecimalV3_notnull "select array_slice(kadcml, kint, ktint) from fn_test_not_nullable"

    order_qt_sql_array_slice_three_params_Boolean "select array_slice(kabool, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Boolean_notnull "select array_slice(kabool, kint, ktint) from fn_test_not_nullable"

    order_qt_sql_array_slice_three_params_Char "select array_slice(kachr, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Char_notnull "select array_slice(kachr, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_Varchar "select array_slice(kavchr, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_Varchar_notnull "select array_slice(kavchr, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_String "select array_slice(kastr, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_String_notnull "select array_slice(kastr, kint, ktint) from fn_test_not_nullable"

    order_qt_sql_array_slice_three_params_DatetimeV2 "select array_slice(kadtmv2, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_DatetimeV2_notnull "select array_slice(kadtmv2, kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_slice_three_params_DateV2 "select array_slice(kadtv2, kint, ktint) from fn_test"
    order_qt_sql_array_slice_three_params_DateV2_notnull "select array_slice(kadtv2, kint, ktint) from fn_test_not_nullable"

    // array_sort
    order_qt_sql_array_sort_Double "select array_sort(kadbl) from fn_test"
    order_qt_sql_array_sort_Double_notnull "select array_sort(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_sort_Float "select array_sort(kafloat) from fn_test"
    order_qt_sql_array_sort_Float_notnull "select array_sort(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_sort_LargeInt "select array_sort(kalint) from fn_test"
    order_qt_sql_array_sort_LargeInt_notnull "select array_sort(kalint) from fn_test_not_nullable"
    order_qt_sql_array_sort_BigInt "select array_sort(kabint) from fn_test"
    order_qt_sql_array_sort_BigInt_notnull "select array_sort(kabint) from fn_test_not_nullable"
    order_qt_sql_array_sort_SmallInt "select array_sort(kasint) from fn_test"
    order_qt_sql_array_sort_SmallInt_notnull "select array_sort(kasint) from fn_test_not_nullable"
    order_qt_sql_array_sort_Integer "select array_sort(kaint) from fn_test"
    order_qt_sql_array_sort_Integer_notnull "select array_sort(kaint) from fn_test_not_nullable"
    order_qt_sql_array_sort_TinyInt "select array_sort(katint) from fn_test"
    order_qt_sql_array_sort_TinyInt_notnull "select array_sort(katint) from fn_test_not_nullable"
    order_qt_sql_array_sort_DecimalV3 "select array_sort(kadcml) from fn_test"
    order_qt_sql_array_sort_DecimalV3_notnull "select array_sort(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_sort_Boolean "select array_sort(kabool) from fn_test"
    order_qt_sql_array_sort_Boolean_notnull "select array_sort(kabool) from fn_test_not_nullable"

    order_qt_sql_array_sort_Char "select array_sort(kachr) from fn_test"
    order_qt_sql_array_sort_Char_notnull "select array_sort(kachr) from fn_test_not_nullable"
    order_qt_sql_array_sort_Varchar "select array_sort(kavchr) from fn_test"
    order_qt_sql_array_sort_Varchar_notnull "select array_sort(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_sort_String "select array_sort(kastr) from fn_test"
    order_qt_sql_array_sort_String_notnull "select array_sort(kastr) from fn_test_not_nullable"

    order_qt_sql_array_sort_DatetimeV2 "select array_sort(kadtmv2) from fn_test"
    order_qt_sql_array_sort_DatetimeV2_notnull "select array_sort(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_sort_DateV2 "select array_sort(kadtv2) from fn_test"
    order_qt_sql_array_sort_DateV2_notnull "select array_sort(kadtv2) from fn_test_not_nullable"

    // array_sum
    order_qt_sql_array_sum_Double "select array_sum(kadbl) from fn_test"
    order_qt_sql_array_sum_Double_notnull "select array_sum(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_sum_Float "select array_sum(kafloat) from fn_test"
    order_qt_sql_array_sum_Float_notnull "select array_sum(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_sum_LargeInt "select array_sum(kalint) from fn_test"
    order_qt_sql_array_sum_LargeInt_notnull "select array_sum(kalint) from fn_test_not_nullable"
    order_qt_sql_array_sum_BigInt "select array_sum(kabint) from fn_test"
    order_qt_sql_array_sum_BigInt_notnull "select array_sum(kabint) from fn_test_not_nullable"
    order_qt_sql_array_sum_SmallInt "select array_sum(kasint) from fn_test"
    order_qt_sql_array_sum_SmallInt_notnull "select array_sum(kasint) from fn_test_not_nullable"
    order_qt_sql_array_sum_Integer "select array_sum(kaint) from fn_test"
    order_qt_sql_array_sum_Integer_notnull "select array_sum(kaint) from fn_test_not_nullable"
    order_qt_sql_array_sum_TinyInt "select array_sum(katint) from fn_test"
    order_qt_sql_array_sum_TinyInt_notnull "select array_sum(katint) from fn_test_not_nullable"
    order_qt_sql_array_sum_DecimalV3 "select array_sum(kadcml) from fn_test"
    order_qt_sql_array_sum_DecimalV3_notnull "select array_sum(kadcml) from fn_test_not_nullable"

    // array_union
    order_qt_sql_array_union_Double "select array_union(kadbl, kadbl) from fn_test"
    order_qt_sql_array_union_Double_notnull "select array_union(kadbl, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_union_Float "select array_union(kafloat, kafloat) from fn_test"
    order_qt_sql_array_union_Float_notnull "select array_union(kafloat, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_union_LargeInt "select array_union(kalint, kalint) from fn_test"
    order_qt_sql_array_union_LargeInt_notnull "select array_union(kalint, kalint) from fn_test_not_nullable"
    order_qt_sql_array_union_BigInt "select array_union(kabint, kabint) from fn_test"
    order_qt_sql_array_union_BigInt_notnull "select array_union(kabint, kabint) from fn_test_not_nullable"
    order_qt_sql_array_union_SmallInt "select array_union(kasint, kasint) from fn_test"
    order_qt_sql_array_union_SmallInt_notnull "select array_union(kasint, kasint) from fn_test_not_nullable"
    order_qt_sql_array_union_Integer "select array_union(kaint, kaint) from fn_test"
    order_qt_sql_array_union_Integer_notnull "select array_union(kaint, kaint) from fn_test_not_nullable"
    order_qt_sql_array_union_TinyInt "select array_union(katint, katint) from fn_test"
    order_qt_sql_array_union_TinyInt_notnull "select array_union(katint, katint) from fn_test_not_nullable"
    order_qt_sql_array_union_DecimalV3 "select array_union(kadcml, kadcml) from fn_test"
    order_qt_sql_array_union_DecimalV3_notnull "select array_union(kadcml, kadcml) from fn_test_not_nullable"

    order_qt_sql_array_union_Boolean "select array_union(kabool, kabool) from fn_test"
    order_qt_sql_array_union_Boolean_notnull "select array_union(kabool, kabool) from fn_test_not_nullable"

    order_qt_sql_array_union_Char "select array_union(kachr, kachr) from fn_test"
    order_qt_sql_array_union_Char_notnull "select array_union(kachr, kachr) from fn_test_not_nullable"
    order_qt_sql_array_union_Varchar "select array_union(kavchr, kavchr) from fn_test"
    order_qt_sql_array_union_Varchar_notnull "select array_union(kavchr, kavchr) from fn_test_not_nullable"
    order_qt_sql_array_union_String "select array_union(kastr, kastr) from fn_test"
    order_qt_sql_array_union_String_notnull "select array_union(kastr, kastr) from fn_test_not_nullable"

    order_qt_sql_array_union_DatetimeV2 "select array_union(kadtmv2, kadtmv2) from fn_test"
    order_qt_sql_array_union_DatetimeV2_notnull "select array_union(kadtmv2, kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_union_DateV2 "select array_union(kadtv2, kadtv2) from fn_test"
    order_qt_sql_array_union_DateV2_notnull "select array_union(kadtv2, kadtv2) from fn_test_not_nullable"

    // array_with_constant
    order_qt_sql_array_with_constant_Double "select array_with_constant(kint, kdbl) from fn_test"
    order_qt_sql_array_with_constant_Double_notnull "select array_with_constant(kint, kdbl) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_Float "select array_with_constant(kint, kfloat) from fn_test"
    order_qt_sql_array_with_constant_Float_notnull "select array_with_constant(kint, kfloat) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_LargeInt "select array_with_constant(kint, klint) from fn_test"
    order_qt_sql_array_with_constant_LargeInt_notnull "select array_with_constant(kint, klint) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_BigInt "select array_with_constant(kint, kbint) from fn_test"
    order_qt_sql_array_with_constant_BigInt_notnull "select array_with_constant(kint, kbint) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_SmallInt "select array_with_constant(kint, ksint) from fn_test"
    order_qt_sql_array_with_constant_SmallInt_notnull "select array_with_constant(kint, ksint) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_Integer "select array_with_constant(kint, kint) from fn_test"
    order_qt_sql_array_with_constant_Integer_notnull "select array_with_constant(kint, kint) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_TinyInt "select array_with_constant(kint, ktint) from fn_test"
    order_qt_sql_array_with_constant_TinyInt_notnull "select array_with_constant(kint, ktint) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_DecimalV3 "select array_with_constant(kint, kdcmls1) from fn_test"
    order_qt_sql_array_with_constant_DecimalV3_notnull "select array_with_constant(kint, kdcmls1) from fn_test_not_nullable"

    order_qt_sql_array_with_constant_Boolean "select array_with_constant(kint, kbool) from fn_test"
    order_qt_sql_array_with_constant_Boolean_notnull "select array_with_constant(kint, kbool) from fn_test_not_nullable"

    order_qt_sql_array_with_constant_Char "select array_with_constant(kint, kchrs1) from fn_test"
    order_qt_sql_array_with_constant_Char_notnull "select array_with_constant(kint, kchrs1) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_Varchar "select array_with_constant(kint, kvchrs1) from fn_test"
    order_qt_sql_array_with_constant_Varchar_notnull "select array_with_constant(kint, kvchrs1) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_String "select array_with_constant(kint, kstr) from fn_test"
    order_qt_sql_array_with_constant_String_notnull "select array_with_constant(kint, kstr) from fn_test_not_nullable"

    order_qt_sql_array_with_constant_DatetimeV2 "select array_with_constant(kint, kdtmv2s1) from fn_test"
    order_qt_sql_array_with_constant_DatetimeV2_notnull "select array_with_constant(kint, kdtmv2s1) from fn_test_not_nullable"
    order_qt_sql_array_with_constant_DateV2 "select array_with_constant(kint, kdtv2) from fn_test"
    order_qt_sql_array_with_constant_DateV2_notnull "select array_with_constant(kint, kdtv2) from fn_test_not_nullable"

    // cardinality
    order_qt_sql_cardinality_Double "select cardinality(kadbl) from fn_test"
    order_qt_sql_cardinality_Double_notnull "select cardinality(kadbl) from fn_test_not_nullable"
    order_qt_sql_cardinality_Float "select cardinality(kafloat) from fn_test"
    order_qt_sql_cardinality_Float_notnull "select cardinality(kafloat) from fn_test_not_nullable"
    order_qt_sql_cardinality_LargeInt "select cardinality(kalint) from fn_test"
    order_qt_sql_cardinality_LargeInt_notnull "select cardinality(kalint) from fn_test_not_nullable"
    order_qt_sql_cardinality_BigInt "select cardinality(kabint) from fn_test"
    order_qt_sql_cardinality_BigInt_notnull "select cardinality(kabint) from fn_test_not_nullable"
    order_qt_sql_cardinality_SmallInt "select cardinality(kasint) from fn_test"
    order_qt_sql_cardinality_SmallInt_notnull "select cardinality(kasint) from fn_test_not_nullable"
    order_qt_sql_cardinality_Integer "select cardinality(kaint) from fn_test"
    order_qt_sql_cardinality_Integer_notnull "select cardinality(kaint) from fn_test_not_nullable"
    order_qt_sql_cardinality_TinyInt "select cardinality(katint) from fn_test"
    order_qt_sql_cardinality_TinyInt_notnull "select cardinality(katint) from fn_test_not_nullable"
    order_qt_sql_cardinality_DecimalV3 "select cardinality(kadcml) from fn_test"
    order_qt_sql_cardinality_DecimalV3_notnull "select cardinality(kadcml) from fn_test_not_nullable"

    order_qt_sql_cardinality_Boolean "select cardinality(kabool) from fn_test"
    order_qt_sql_cardinality_Boolean_notnull "select cardinality(kabool) from fn_test_not_nullable"

    order_qt_sql_cardinality_Char "select cardinality(kachr) from fn_test"
    order_qt_sql_cardinality_Char_notnull "select cardinality(kachr) from fn_test_not_nullable"
    order_qt_sql_cardinality_Varchar "select cardinality(kavchr) from fn_test"
    order_qt_sql_cardinality_Varchar_notnull "select cardinality(kavchr) from fn_test_not_nullable"
    order_qt_sql_cardinality_String "select cardinality(kastr) from fn_test"
    order_qt_sql_cardinality_String_notnull "select cardinality(kastr) from fn_test_not_nullable"

    order_qt_sql_cardinality_DatetimeV2 "select cardinality(kadtmv2) from fn_test"
    order_qt_sql_cardinality_DatetimeV2_notnull "select cardinality(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_cardinality_DateV2 "select cardinality(kadtv2) from fn_test"
    order_qt_sql_cardinality_DateV2_notnull "select cardinality(kadtv2) from fn_test_not_nullable"

    // array_size
    order_qt_sql_array_size_Double "select array_size(kadbl) from fn_test"
    order_qt_sql_array_size_Double_notnull "select array_size(kadbl) from fn_test_not_nullable"
    order_qt_sql_array_size_Float "select array_size(kafloat) from fn_test"
    order_qt_sql_array_size_Float_notnull "select array_size(kafloat) from fn_test_not_nullable"
    order_qt_sql_array_size_LargeInt "select array_size(kalint) from fn_test"
    order_qt_sql_array_size_LargeInt_notnull "select array_size(kalint) from fn_test_not_nullable"
    order_qt_sql_array_size_BigInt "select array_size(kabint) from fn_test"
    order_qt_sql_array_size_BigInt_notnull "select array_size(kabint) from fn_test_not_nullable"
    order_qt_sql_array_size_SmallInt "select array_size(kasint) from fn_test"
    order_qt_sql_array_size_SmallInt_notnull "select array_size(kasint) from fn_test_not_nullable"
    order_qt_sql_array_size_Integer "select array_size(kaint) from fn_test"
    order_qt_sql_array_size_Integer_notnull "select array_size(kaint) from fn_test_not_nullable"
    order_qt_sql_array_size_TinyInt "select array_size(katint) from fn_test"
    order_qt_sql_array_size_TinyInt_notnull "select array_size(katint) from fn_test_not_nullable"
    order_qt_sql_array_size_DecimalV3 "select array_size(kadcml) from fn_test"
    order_qt_sql_array_size_DecimalV3_notnull "select array_size(kadcml) from fn_test_not_nullable"

    order_qt_sql_array_size_Boolean "select array_size(kabool) from fn_test"
    order_qt_sql_array_size_Boolean_notnull "select array_size(kabool) from fn_test_not_nullable"

    order_qt_sql_array_size_Char "select array_size(kachr) from fn_test"
    order_qt_sql_array_size_Char_notnull "select array_size(kachr) from fn_test_not_nullable"
    order_qt_sql_array_size_Varchar "select array_size(kavchr) from fn_test"
    order_qt_sql_array_size_Varchar_notnull "select array_size(kavchr) from fn_test_not_nullable"
    order_qt_sql_array_size_String "select array_size(kastr) from fn_test"
    order_qt_sql_array_size_String_notnull "select array_size(kastr) from fn_test_not_nullable"

    order_qt_sql_array_size_DatetimeV2 "select array_size(kadtmv2) from fn_test"
    order_qt_sql_array_size_DatetimeV2_notnull "select array_size(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_array_size_DateV2 "select array_size(kadtv2) from fn_test"
    order_qt_sql_array_size_DateV2_notnull "select array_size(kadtv2) from fn_test_not_nullable"

    // size
    order_qt_sql_size_Double "select size(kadbl) from fn_test"
    order_qt_sql_size_Double_notnull "select size(kadbl) from fn_test_not_nullable"
    order_qt_sql_size_Float "select size(kafloat) from fn_test"
    order_qt_sql_size_Float_notnull "select size(kafloat) from fn_test_not_nullable"
    order_qt_sql_size_LargeInt "select size(kalint) from fn_test"
    order_qt_sql_size_LargeInt_notnull "select size(kalint) from fn_test_not_nullable"
    order_qt_sql_size_BigInt "select size(kabint) from fn_test"
    order_qt_sql_size_BigInt_notnull "select size(kabint) from fn_test_not_nullable"
    order_qt_sql_size_SmallInt "select size(kasint) from fn_test"
    order_qt_sql_size_SmallInt_notnull "select size(kasint) from fn_test_not_nullable"
    order_qt_sql_size_Integer "select size(kaint) from fn_test"
    order_qt_sql_size_Integer_notnull "select size(kaint) from fn_test_not_nullable"
    order_qt_sql_size_TinyInt "select size(katint) from fn_test"
    order_qt_sql_size_TinyInt_notnull "select size(katint) from fn_test_not_nullable"
    order_qt_sql_size_DecimalV3 "select size(kadcml) from fn_test"
    order_qt_sql_size_DecimalV3_notnull "select size(kadcml) from fn_test_not_nullable"

    order_qt_sql_size_Boolean "select size(kabool) from fn_test"
    order_qt_sql_size_Boolean_notnull "select size(kabool) from fn_test_not_nullable"

    order_qt_sql_size_Char "select size(kachr) from fn_test"
    order_qt_sql_size_Char_notnull "select size(kachr) from fn_test_not_nullable"
    order_qt_sql_size_Varchar "select size(kavchr) from fn_test"
    order_qt_sql_size_Varchar_notnull "select size(kavchr) from fn_test_not_nullable"
    order_qt_sql_size_String "select size(kastr) from fn_test"
    order_qt_sql_size_String_notnull "select size(kastr) from fn_test_not_nullable"

    order_qt_sql_size_DatetimeV2 "select size(kadtmv2) from fn_test"
    order_qt_sql_size_DatetimeV2_notnull "select size(kadtmv2) from fn_test_not_nullable"
    order_qt_sql_size_DateV2 "select size(kadtv2) from fn_test"
    order_qt_sql_size_DateV2_notnull "select size(kadtv2) from fn_test_not_nullable"

    // split_by_string
    order_qt_sql_split_by_string_Char "select split_by_string(kchrs1, ',') from fn_test"
    order_qt_sql_split_by_string_Char_notnull "select split_by_string(kchrs1, ',') from fn_test_not_nullable"
    order_qt_sql_split_by_string_VarChar "select split_by_string(kvchrs1, ',') from fn_test"
    order_qt_sql_split_by_string_VarChar_notnull "select split_by_string(kvchrs1, ',') from fn_test_not_nullable"
    order_qt_sql_split_by_string_String "select split_by_string(kstr, ',') from fn_test"
    order_qt_sql_split_by_string_String_notnull "select split_by_string(kstr, ',') from fn_test_not_nullable"

    // tokenize
    order_qt_sql_tokenize_Char "select tokenize(kchrs1, '') from fn_test"
    order_qt_sql_tokenize_Char_notnull "select tokenize(kchrs1, '') from fn_test_not_nullable"
    order_qt_sql_tokenize_VarChar "select tokenize(kvchrs1, null) from fn_test"
    order_qt_sql_tokenize_VarChar_notnull "select tokenize(kvchrs1, '') from fn_test_not_nullable"
    order_qt_sql_tokenize_String "select tokenize(kstr, '') from fn_test"
    order_qt_sql_tokenize_String_notnull "select tokenize(kstr, null) from fn_test_not_nullable"

    // test array_map
    order_qt_sql_array_map_Double "select array_map(x -> x is not null, kadbl) from fn_test"
    order_qt_sql_array_map_Double_notnull "select array_map(x -> x is not null, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_map_Float "select array_map(x -> x is not null, kafloat) from fn_test"
    order_qt_sql_array_map_Float_notnull "select array_map(x -> x is not null, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_map_LargeInt "select array_map(x -> x is not null, kalint) from fn_test"
    order_qt_sql_array_map_LargeInt_notnull "select array_map(x -> x is not null, kalint) from fn_test_not_nullable"
    order_qt_sql_array_map_BigInt "select array_map(x -> x is not null, kabint) from fn_test"
    order_qt_sql_array_map_BigInt_notnull "select array_map(x -> x is not null, kabint) from fn_test_not_nullable"
    order_qt_sql_array_map_SmallInt "select array_map(x -> x is not null, kasint) from fn_test"
    order_qt_sql_array_map_SmallInt_notnull "select array_map(x -> x is not null, kasint) from fn_test_not_nullable"
    order_qt_sql_array_map_Integer "select array_map(x -> x is not null, kaint) from fn_test"
    order_qt_sql_array_map_Integer_notnull "select array_map(x -> x is not null, kaint) from fn_test_not_nullable"
    order_qt_sql_array_map_TinyInt "select array_map(x -> x is not null, katint) from fn_test"
    order_qt_sql_array_map_TinyInt_notnull "select array_map(x -> x is not null, katint) from fn_test_not_nullable"
    order_qt_sql_array_map_DecimalV3 "select array_map(x -> x is not null, kadcml) from fn_test"
    order_qt_sql_array_map_DecimalV3_notnull "select array_map(x -> x is not null, kadcml) from fn_test_not_nullable"
    order_qt_sql_array_map_lambda_agg "select array_map(x->(x+100), collect_list(ktint)) from fn_test group by id;"

    // test array_exists
    order_qt_sql_array_exists_Double "select array_exists(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_exists_Double_notnull "select array_exists(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_exists_Float "select array_exists(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_exists_Float_notnull "select array_exists(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_exists_LargeInt "select array_exists(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_exists_LargeInt_notnull "select array_exists(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_exists_BigInt "select array_exists(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_exists_BigInt_notnull "select array_exists(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_exists_SmallInt "select array_exists(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_exists_SmallInt_notnull "select array_exists(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_exists_Integer "select array_exists(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_exists_Integer_notnull "select array_exists(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_exists_TinyInt "select array_exists(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_exists_TinyInt_notnull "select array_exists(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_exists_DecimalV3 "select array_exists(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_exists_DecimalV3_notnull "select array_exists(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_first_index
    order_qt_sql_array_first_index_Double "select array_first_index(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_first_index_Double_notnull "select array_first_index(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_first_index_Float "select array_first_index(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_first_index_Float_notnull "select array_first_index(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_first_index_LargeInt "select array_first_index(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_first_index_LargeInt_notnull "select array_first_index(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_first_index_BigInt "select array_first_index(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_first_index_BigInt_notnull "select array_first_index(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_first_index_SmallInt "select array_first_index(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_first_index_SmallInt_notnull "select array_first_index(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_first_index_Integer "select array_first_index(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_first_index_Integer_notnull "select array_first_index(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_first_index_TinyInt "select array_first_index(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_first_index_TinyInt_notnull "select array_first_index(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_first_index_DecimalV3 "select array_first_index(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_first_index_DecimalV3_notnull "select array_first_index(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_count
    order_qt_sql_array_count_Double "select array_count(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_count_Double_notnull "select array_count(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_count_Float "select array_count(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_count_Float_notnull "select array_count(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_count_LargeInt "select array_count(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_count_LargeInt_notnull "select array_count(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_count_BigInt "select array_count(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_count_BigInt_notnull "select array_count(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_count_SmallInt "select array_count(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_count_SmallInt_notnull "select array_count(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_count_Integer "select array_count(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_count_Integer_notnull "select array_count(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_count_TinyInt "select array_count(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_count_TinyInt_notnull "select array_count(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_count_DecimalV3 "select array_count(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_count_DecimalV3_notnull "select array_count(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_map
    order_qt_sql_array_map_Double "select array_map(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_map_Double_notnull "select array_map(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_map_Float "select array_map(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_map_Float_notnull "select array_map(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_map_LargeInt "select array_map(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_map_LargeInt_notnull "select array_map(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_map_BigInt "select array_map(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_map_BigInt_notnull "select array_map(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_map_SmallInt "select array_map(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_map_SmallInt_notnull "select array_map(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_map_Integer "select array_map(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_map_Integer_notnull "select array_map(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_map_TinyInt "select array_map(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_map_TinyInt_notnull "select array_map(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_map_DecimalV3 "select array_map(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_map_DecimalV3_notnull "select array_map(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_filter
    order_qt_sql_array_filter_Double "select array_filter(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_filter_Double_notnull "select array_filter(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_filter_Float "select array_filter(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_filter_Float_notnull "select array_filter(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_filter_LargeInt "select array_filter(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_filter_LargeInt_notnull "select array_filter(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_filter_BigInt "select array_filter(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_filter_BigInt_notnull "select array_filter(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_filter_SmallInt "select array_filter(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_filter_SmallInt_notnull "select array_filter(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_filter_Integer "select array_filter(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_filter_Integer_notnull "select array_filter(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_filter_TinyInt "select array_filter(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_filter_TinyInt_notnull "select array_filter(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_filter_DecimalV3 "select array_filter(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_filter_DecimalV3_notnull "select array_filter(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_sortby
    order_qt_sql_array_sortby_Double "select array_sortby(x -> x + 1, kadbl) from fn_test"
    order_qt_sql_array_sortby_Double_notnull "select array_sortby(x -> x + 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_sortby_Float "select array_sortby(x -> x + 1, kafloat) from fn_test"
    order_qt_sql_array_sortby_Float_notnull "select array_sortby(x -> x + 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_sortby_LargeInt "select array_sortby(x -> x + 1, kalint) from fn_test"
    order_qt_sql_array_sortby_LargeInt_notnull "select array_sortby(x -> x + 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_sortby_BigInt "select array_sortby(x -> x + 1, kabint) from fn_test"
    order_qt_sql_array_sortby_BigInt_notnull "select array_sortby(x -> x + 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_sortby_SmallInt "select array_sortby(x -> x + 1, kasint) from fn_test"
    order_qt_sql_array_sortby_SmallInt_notnull "select array_sortby(x -> x + 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_sortby_Integer "select array_sortby(x -> x + 1, kaint) from fn_test"
    order_qt_sql_array_sortby_Integer_notnull "select array_sortby(x -> x + 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_sortby_TinyInt "select array_sortby(x -> x + 1, katint) from fn_test"
    order_qt_sql_array_sortby_TinyInt_notnull "select array_sortby(x -> x + 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_sortby_DecimalV3 "select array_sortby(x -> x + 1, kadcml) from fn_test"
    order_qt_sql_array_sortby_DecimalV3_notnull "select array_sortby(x -> x + 1, kadcml) from fn_test_not_nullable"
    // test array_last_index
    order_qt_sql_array_last_index_Double "select array_last_index(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_last_index_Double_notnull "select array_last_index(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_last_index_Float "select array_last_index(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_last_index_Float_notnull "select array_last_index(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_last_index_LargeInt "select array_last_index(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_last_index_LargeInt_notnull "select array_last_index(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_last_index_BigInt "select array_last_index(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_last_index_BigInt_notnull "select array_last_index(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_last_index_SmallInt "select array_last_index(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_last_index_SmallInt_notnull "select array_last_index(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_last_index_Integer "select array_last_index(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_last_index_Integer_notnull "select array_last_index(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_last_index_TinyInt "select array_last_index(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_last_index_TinyInt_notnull "select array_last_index(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_last_index_DecimalV3 "select array_last_index(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_last_index_DecimalV3_notnull "select array_last_index(x -> x > 1, kadcml) from fn_test_not_nullable"
    order_qt_sql_array_first_Double "select array_first(x -> x > 1, kadbl) from fn_test"
    // test array_first
    order_qt_sql_array_first_Double_notnull "select array_first(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_first_Float "select array_first(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_first_Float_notnull "select array_first(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_first_LargeInt "select array_first(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_first_LargeInt_notnull "select array_first(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_first_BigInt "select array_first(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_first_BigInt_notnull "select array_first(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_first_SmallInt "select array_first(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_first_SmallInt_notnull "select array_first(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_first_Integer "select array_first(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_first_Integer_notnull "select array_first(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_first_TinyInt "select array_first(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_first_TinyInt_notnull "select array_first(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_first_DecimalV3 "select array_first(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_first_DecimalV3_notnull "select array_first(x -> x > 1, kadcml) from fn_test_not_nullable"
    // test array_last
    order_qt_sql_array_last_Double "select array_last(x -> x > 1, kadbl) from fn_test"
    order_qt_sql_array_last_Double_notnull "select array_last(x -> x > 1, kadbl) from fn_test_not_nullable"
    order_qt_sql_array_last_Float "select array_last(x -> x > 1, kafloat) from fn_test"
    order_qt_sql_array_last_Float_notnull "select array_last(x -> x > 1, kafloat) from fn_test_not_nullable"
    order_qt_sql_array_last_LargeInt "select array_last(x -> x > 1, kalint) from fn_test"
    order_qt_sql_array_last_LargeInt_notnull "select array_last(x -> x > 1, kalint) from fn_test_not_nullable"
    order_qt_sql_array_last_BigInt "select array_last(x -> x > 1, kabint) from fn_test"
    order_qt_sql_array_last_BigInt_notnull "select array_last(x -> x > 1, kabint) from fn_test_not_nullable"
    order_qt_sql_array_last_SmallInt "select array_last(x -> x > 1, kasint) from fn_test"
    order_qt_sql_array_last_SmallInt_notnull "select array_last(x -> x > 1, kasint) from fn_test_not_nullable"
    order_qt_sql_array_last_Integer "select array_last(x -> x > 1, kaint) from fn_test"
    order_qt_sql_array_last_Integer_notnull "select array_last(x -> x > 1, kaint) from fn_test_not_nullable"
    order_qt_sql_array_last_TinyInt "select array_last(x -> x > 1, katint) from fn_test"
    order_qt_sql_array_last_TinyInt_notnull "select array_last(x -> x > 1, katint) from fn_test_not_nullable"
    order_qt_sql_array_last_DecimalV3 "select array_last(x -> x > 1, kadcml) from fn_test"
    order_qt_sql_array_last_DecimalV3_notnull "select array_last(x -> x > 1, kadcml) from fn_test_not_nullable"

    // test array_first_index
    sql "create view v as select array_first_index(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_first_index_Double "select * from v;"
    sql "drop view v"
    // test array_count
    sql "create view v as select array_count(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_count_Double "select * from v;"
    sql "drop view v"
    // test array_first
    sql "create view v as select array_first(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_first_Double "select * from v;"
    sql "drop view v"
    // test array_sortby
    sql "create view v as select array_sortby(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_sortby_Double "select * from v;"
    sql "drop view v"
    // test array_filter
    sql "create view v as select array_filter(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_filter_Double "select * from v;"
    sql "drop view v"
    // test array_exists
    sql "create view v as select array_exists(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_exists_Double "select * from v;"
    sql "drop view v"
    // test array_last_index
    sql "create view v as select array_last_index(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_last_index_Double "select * from v;"
    sql "drop view v"
    // test array_last
    sql "create view v as select array_last(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_last_Double "select * from v;"
    sql "drop view v"
    // test array_map
    sql "create view v as select array_map(x -> x > 1, kadbl) from fn_test;"
    order_qt_sql_view_array_map_Double "select * from v;"
    sql "drop view v"
    test {
        sql "select tokenize('arg1','xxx = yyy,zzz');"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql "select tokenize('arg1','2');"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql "select tokenize(kstr, kstr) from fn_test"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    sql "DROP TABLE IF EXISTS test_array_with_scale_type_table"
    sql """
        CREATE TABLE IF NOT EXISTS `test_array_with_scale_type_table` (
        `uid` int(11) NULL COMMENT "",
        `c_datetimev2` datetimev2(3) NULL COMMENT "",
        `c_decimal` decimal(8,3) NULL COMMENT "",
        `c_decimalv3` decimalv3(8,3) NULL COMMENT "",
        `c_array_datetimev2` ARRAY<datetimev2(3)> NULL COMMENT "",
        `c_array_decimal` ARRAY<decimal(8,3)> NULL COMMENT "",
        `c_array_decimalv3` ARRAY<decimalv3(8,3)> NULL COMMENT ""
        ) ENGINE=OLAP
    DUPLICATE KEY(`uid`)
    DISTRIBUTED BY HASH(`uid`) BUCKETS 1
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "storage_format" = "V2"
    )
    """

    sql """INSERT INTO test_array_with_scale_type_table values
    (1,"2022-12-01 22:23:24.999999",22.6789,33.6789,["2022-12-01 22:23:24.999999","2022-12-01 23:23:24.999999"],[22.6789,33.6789],[22.6789,33.6789]),
    (2,"2022-12-02 22:23:24.999999",23.6789,34.6789,["2022-12-02 22:23:24.999999","2022-12-02 23:23:24.999999"],[23.6789,34.6789],[22.6789,34.6789])
    """

    // array_apply
    qt_array_apply1 """select array_apply(c_array_datetimev2, "=", '2022-12-02 22:23:24.999999') from test_array_with_scale_type_table"""
    qt_array_apply2 """select array_apply(c_array_datetimev2, ">", '2022-12-01 22:23:24.999999') from test_array_with_scale_type_table"""
    qt_array_apply3 """select array_apply(c_array_datetimev2, ">", null) from test_array_with_scale_type_table"""
    qt_array_apply4 """select array_apply(c_array_decimal, "=", 22.679) from test_array_with_scale_type_table"""
    qt_array_apply5 """select array_apply(c_array_decimal, ">=", 22.1) from test_array_with_scale_type_table"""
    qt_array_apply6 """select array_apply(c_array_decimal, ">=", null) from test_array_with_scale_type_table"""

    // array_repeat
    qt_array_repeat1 """select array_repeat("hello", 2)"""
    qt_array_repeat2 """select array_repeat(123, 2)"""
    qt_array_repeat3 """select array_repeat(null, 2)"""
    qt_array_repeat4 """select array_repeat(3, null)"""

    // array_zip
    sql "select array_zip([1], ['1'], [1.0])"

    // array_range with datetime argument, sequence with int and datetime argument
    qt_array_range_datetime1 """select array_range(kdtmv2s1, date_add(kdtmv2s1, interval kint+1 day), interval kint day) from fn_test order by kdtmv2s1;"""
    qt_array_range_datetime2 """select array_range(kdtmv2s1, date_add(kdtmv2s1, interval kint+2 week), interval kint week) from fn_test order by kdtmv2s1;"""
    qt_sequence_int_one_para """select sequence(kint) from fn_test order by kint;"""
    qt_sequence_int_two_para """select sequence(kint, kint+2) from fn_test order by kint;"""
    qt_sequence_int_three_para """select sequence(kint-1, kint+2, 1) from fn_test order by kint;"""
    qt_sequence_datetime_default """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint-3 day)) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_day """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+1 day), interval kint day) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_week """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+2 week), interval kint week) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_month """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+3 month), interval kint month) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_year """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+3 year), interval kint year) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_hour """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint-3 hour), interval kint hour) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_minute """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+1 minute), interval kint minute) from fn_test order by kdtmv2s1;"""
    qt_sequence_datetime_second """select sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint second), interval kint-1 second) from fn_test order by kdtmv2s1;"""

    // with array empty
    qt_array_empty_fe """select array()"""
    // make large error size
    test {
        sql "select array_size(sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+1000 year), interval kint hour)) from fn_test order by kdtmv2s1;"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql "select array_size(sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+10000 month), interval kint hour)) from fn_test order by kdtmv2s1;"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql "select array_size(sequence(kdtmv2s1, date_add(kdtmv2s1, interval kint+1000001 day), interval kint day)) from fn_test order by kdtmv2s1;"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    sql """ set enable_fold_constant_by_be=true; """
    qt_array_empty_fe """select array()"""

    // array_map with string is can be succeed
    qt_sql_array_map """select array_map(x->x!='', split_by_string('amory,is,better,committing', ','))"""

    // array_apply with string should be failed
    test {
       sql """select array_apply(split_by_string("amory,is,better,committing", ","), '!=', '');"""
       exception("errCode = 2")
    }

    // array_min/max with nested array for args
    test {
        sql "select array_min(array(1,2,3),array(4,5,6));"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test {
        sql "select array_max(array(1,2,3),array(4,5,6));"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }

    test {
        sql "select array_min(array(split_by_string('a,b,c',',')));"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    test {
        sql "select array_max(array(split_by_string('a,b,c',',')));"
        check{result, exception, startTime, endTime ->
            assertTrue(exception != null)
            logger.info(exception.message)
        }
    }
    // array_map with string is can be succeed
    qt_sql_array_map """select array_map(x->x!='', split_by_string('amory,is,better,committing', ','))"""

    // array_apply with string should be failed
    test {
       sql """select array_apply(split_by_string("amory,is,better,committing", ","), '!=', '');"""
       exception("errCode = 2")
    }

    // agg for array types add decimal256 cases array_min/array_max/array_product/array_avg/array_sum with decimal256
    sql """ set enable_decimal256=true; """
    order_qt_sql_array_min_decimal256 "select array_min(c) from fn_test_array_with_large_decimal order by id"
    order_qt_sql_array_max_decimal256 "select array_max(c) from fn_test_array_with_large_decimal order by id"
    order_qt_sql_array_product_decimal256 "select array_product(c) from fn_test_array_with_large_decimal order by id"
    order_qt_sql_array_avg_decimal256 "select array_avg(c) from fn_test_array_with_large_decimal order by id"
    order_qt_sql_array_sum_decimal256 "select array_sum(c) from fn_test_array_with_large_decimal order by id"
    // array_overlap for type correctness
    order_qt_sql_array_overlaps_1 """select arrays_overlap(a, b) from fn_test_array_with_large_decimal order by id"""
    order_qt_sql_array_overlaps_2 """select arrays_overlap(b, a) from fn_test_array_with_large_decimal order by id"""
    order_qt_sql_array_overlaps_3 """select arrays_overlap(a, c) from fn_test_array_with_large_decimal order by id"""
    order_qt_sql_array_overlaps_4 """select arrays_overlap(c, a) from fn_test_array_with_large_decimal order by id"""
    order_qt_sql_array_overlaps_5 """select arrays_overlap(b, c) from fn_test_array_with_large_decimal order by id"""
    order_qt_sql_array_overlaps_6 """select arrays_overlap(c, b) from fn_test_array_with_large_decimal order by id"""

    // tests for nereids array functions for number overflow cases
    qt_sql """ SELECT array_position([1,258],257),array_position([2],258);"""
    qt_sql """ select array_apply([258], '>' , 257), array_apply([1,2,3], '>', 258);"""
    qt_sql """ select array_contains([258], 257), array_contains([1,2,3], 258);"""
    // pushfront and pushback
    qt_sql """ select array_pushfront([258], 257), array_pushfront([1,2,3], 258);"""
    qt_sql """ select array_pushback([1,258], 257), array_pushback([1,2,3], 258);"""
    // array_remove
    qt_sql """ select array_remove([1,258], 257), array_remove([1,2,3], 258);"""
    // countequal
    qt_sql """ select countequal([1,258], 257), countequal([1,2,3], 258);"""
    // map_contains_key
    qt_sql """ select map_contains_key(map(1,258), 257), map_contains_key(map(2,1), 258);"""
    // map_contains_value
    qt_sql """ select map_contains_value(map(1,1), 257), map_contains_value(map(1,2), 258);"""

}
