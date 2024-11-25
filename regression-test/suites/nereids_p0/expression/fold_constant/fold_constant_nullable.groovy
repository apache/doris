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

suite("fold_constant_nullable") {
    def db = "fold_constant_string_arithmatic"
    sql "create database if not exists ${db}"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    // Date and Time Functions
    testFoldConst("SELECT Years_Sub(NULL, NULL)")
    testFoldConst("SELECT YearWeek(NULL)")
    testFoldConst("SELECT Year(NULL)")
    testFoldConst("SELECT Weeks_Sub(NULL, NULL)")
    testFoldConst("SELECT Weeks_Diff(NULL, NULL)")
    testFoldConst("SELECT Weeks_Add(NULL, NULL)")
    testFoldConst("SELECT Weekday(NULL)")
    testFoldConst("SELECT Week(NULL)")
    testFoldConst("SELECT To_Days(NULL)")
    testFoldConst("SELECT Timestamp(NULL)")
    testFoldConst("SELECT Seconds_Sub(NULL, NULL)")
    testFoldConst("SELECT Seconds_Add(NULL, NULL)")
    testFoldConst("SELECT Quarter(NULL)")
    testFoldConst("SELECT Minute(NULL)")
    testFoldConst("SELECT Minutes_Sub(NULL, NULL)")
    testFoldConst("SELECT Minutes_Diff(NULL, NULL)")
    testFoldConst("SELECT MilliSeconds_Sub(NULL, NULL)")
    testFoldConst("SELECT MilliSeconds_Add(NULL, NULL)")
    testFoldConst("SELECT Microsecond(NULL)")
    testFoldConst("SELECT MicroSeconds_Sub(NULL, NULL)")
    testFoldConst("SELECT MicroSeconds_Add(NULL, NULL)")
    testFoldConst("SELECT MicroSecond_Timestamp(NULL)")
    testFoldConst("SELECT Hours_Sub(NULL, NULL)")
    testFoldConst("SELECT Hours_Add(NULL, NULL)")
    testFoldConst("SELECT DayOfYear(NULL)")
    testFoldConst("SELECT DayName(NULL)")

    // Numeric Functions
    testFoldConst("SELECT Width_Bucket(NULL, NULL, NULL, NULL)")
    testFoldConst("SELECT Radians(NULL)")
    testFoldConst("SELECT Negative(NULL)")
    testFoldConst("SELECT Murmur_Hash3_64(NULL)")
    testFoldConst("SELECT Fpow(NULL, NULL)")
    testFoldConst("SELECT Field(NULL, NULL)")
    testFoldConst("SELECT Exp(NULL)")
    testFoldConst("SELECT Dround(NULL)")
    testFoldConst("SELECT Dfloor(NULL)")
    testFoldConst("SELECT Dexp(NULL)")
    testFoldConst("SELECT Cos(NULL)")
    testFoldConst("SELECT Abs(NULL)")
    testFoldConst("SELECT Multiply(NULL, NULL)")
    testFoldConst("SELECT 101>null")
    testFoldConst("SELECT 101=null")
    testFoldConst("SELECT 1+null")
    testFoldConst("SELECT 1-null")
    testFoldConst("SELECT Bitmap_Remove(NULL, NULL)")
    testFoldConst("SELECT Bitmap_Has_Any(NULL, NULL)")
    testFoldConst("SELECT Bitmap_And_Not(NULL, NULL)")
    testFoldConst("SELECT Bitmap_And(NULL, NULL)")
    testFoldConst("SELECT Bit_Length(NULL)")
    testFoldConst("SELECT Width_Bucket(NULL, NULL, NULL, NULL)")

    // String Functions
    testFoldConst("SELECT Upper(NULL)")
    testFoldConst("SELECT Unhex(NULL)")
    testFoldConst("SELECT Tokenize(NULL, NULL)")
    testFoldConst("SELECT Space(NULL)")
    testFoldConst("SELECT Sm3sum(NULL)")
    testFoldConst("SELECT Sm3(NULL)")
    testFoldConst("SELECT Sha2(NULL, 256)")
    testFoldConst("SELECT Md5Sum(NULL)")
    testFoldConst("SELECT Md5(NULL)")
    testFoldConst("SELECT Lower(NULL)")
    testFoldConst("SELECT jsonb_exists_path(NULL, 'key')")
    testFoldConst("SELECT json_replace('{\"null\": 1, \"b\": [2, 3]}', '\$', null)")
    testFoldConst("SELECT Initcap(NULL)")
    testFoldConst("SELECT Hex(NULL)")
    testFoldConst("SELECT From_Second(NULL)")
    testFoldConst("SELECT From_Millisecond(NULL)")
    testFoldConst("SELECT EsQuery(NULL, NULL)")
    testFoldConst("SELECT domain_without_www(NULL)")
    testFoldConst("SELECT Concat(NULL, 'test')")
    testFoldConst("SELECT Character_Length(NULL)")

    // Other Functions
    testFoldConst("SELECT Protocol(NULL)")
    testFoldConst("SELECT money_format(NULL)")
    testFoldConst("SELECT Not(NULL)")
    testFoldConst("SELECT 3 in (1, null)")


    // Geographic and Mathematical Functions
    testFoldConst("SELECT St_Y(NULL)")
    testFoldConst("SELECT St_X(NULL)")
    testFoldConst("SELECT St_Distance_Sphere(NULL, NULL, 1,2)")
    testFoldConst("SELECT St_Area_Square_Meters(NULL)")
    testFoldConst("SELECT St_Area_Square_Km(NULL)")
    testFoldConst("SELECT St_Angle_Sphere(0, NULL, 45, 0)")
    testFoldConst("SELECT St_Angle(ST_Point(1, 0),ST_Point(0, 0),NULL)")
    testFoldConst("SELECT Sqrt(NULL)")
    testFoldConst("SELECT Pmod(NULL, NULL)")
    testFoldConst("SELECT Log2(NULL)")
    testFoldConst("SELECT Log10(NULL)")
    testFoldConst("SELECT Ln(NULL)")
    testFoldConst("SELECT Fmod(NULL, NULL)")
    testFoldConst("SELECT Dsqrt(NULL)")
    testFoldConst("SELECT Dlog10(NULL)")
    testFoldConst("SELECT Mod(NULL, NULL)")
    testFoldConst("SELECT Divide(NULL, NULL)")
    testFoldConst("SELECT Bitmap_Min(NULL)")
    testFoldConst("SELECT Bitmap_Max(NULL)")
    testFoldConst("SELECT Asin(NULL)")
    testFoldConst("SELECT Acos(NULL)")

    // String Functions
    testFoldConst("SELECT Sub_Replace(NULL, 'old', 'new')")
    testFoldConst("SELECT Rpad(NULL, 5, 'pad')")
    testFoldConst("SELECT Repeat(NULL, 2)")
    testFoldConst("SELECT regexp_replace_one(NULL, 'pattern', 'replacement')")
    testFoldConst("SELECT regexp_replace(NULL, 'pattern', 'replacement')")
    testFoldConst("SELECT regexp_extract_all(NULL, 'pattern')")
    testFoldConst("SELECT regexp_extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', null)")
    testFoldConst("SELECT parse_url('https://doris.apache.org/', NULL)")
    testFoldConst("SELECT Lpad(NULL, 5, 'pad')")
    testFoldConst("SELECT json_unquote(NULL)")
    testFoldConst("SELECT json_length(NULL)")
    testFoldConst("SELECT Json_Extract('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', null)")
    testFoldConst("SELECT Json_Contains(NULL, 'key')")
    testFoldConst("SELECT Get_Json_String(NULL, 'key')")
    testFoldConst("SELECT from_base64(NULL)")
    testFoldConst("SELECT bitmap_from_string(NULL)")
    testFoldConst("SELECT bitmap_from_string(NULL)")
    testFoldConst("SELECT bitmap_from_array(NULL)")
    testFoldConst("SELECT append_trailing_char_if_absent(NULL, 'char')")
    testFoldConst("SELECT Nullable(NULL)")
    testFoldConst("SELECT NullIf(NULL, 'value')")
    testFoldConst("SELECT Split_Part('hello world', NULL, 1)")

    // Date Functions
    testFoldConst("SELECT to_datev2(NULL)")
    testFoldConst("SELECT To_Date(NULL)")
    testFoldConst("SELECT Str_To_Date('2014-12-21 12:34:56', NULL)")
    testFoldConst("SELECT Second_Ceil(NULL)")
    testFoldConst("SELECT Month_Floor(NULL)")
    testFoldConst("SELECT Month_Ceil(NULL)")
    testFoldConst("SELECT Minute_Floor(NULL)")
    testFoldConst("SELECT Hour_Floor(NULL)")
    testFoldConst("SELECT Hour_Ceil(NULL)")
    testFoldConst("SELECT From_Days(NULL)")
    testFoldConst("SELECT Day_Floor(NULL)")
    testFoldConst("SELECT DateV2(NULL)")
    testFoldConst("SELECT Date_Format(NULL, 'format')")
    testFoldConst("SELECT Date(NULL)")
    testFoldConst("SELECT Convert_Tz(NULL, 'from_tz', 'to_tz')")
    testFoldConst("SELECT st_geomfromtext(NULL)")
    testFoldConst("SELECT St_Geometryfromtext(NULL)")
    testFoldConst("SELECT St_Polyfromtext(NULL)")
    testFoldConst("SELECT St_Point(NULL, 1)")
    testFoldConst("SELECT St_Linefromtext(NULL)")
    testFoldConst("SELECT St_GeomFromWKB(NULL)")
    testFoldConst("SELECT St_Polygon(NULL)")
}
