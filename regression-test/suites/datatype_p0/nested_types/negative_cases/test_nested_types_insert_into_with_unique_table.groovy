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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_nested_types_insert_into_with_unique_table", "p0") {
    sql 'use regression_test_datatype_p0_nested_types'

    // test action for scala to array with scala type
    //  current we support char family to insert nested type
    // mor table test
    test {
        sql "insert into tbl_array_nested_types_mor (c_bool) select c_bool from tbl_scalar_types_dup"
        exception "java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<boolean>"
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<tinyint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<smallint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<int>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<largeint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<float>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<double>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<datev2>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<datetimev2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<datev2>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<datetimev2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mor (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_mor_c """select count() from tbl_array_nested_types_mor;"""

    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_array_nested_types_mor2 (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<array<boolean>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<array<tinyint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<array<smallint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<array<int>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<array<largeint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<array<float>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<array<double>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<array<datev2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<array<datev2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mor2 (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_mor2_c """select count() from tbl_array_nested_types_mor2;"""


    // test action for scala to map with map-scala-scala type
    test {
        sql "insert into tbl_map_types_mor (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=map<boolean,boolean>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=map<tinyint,tinyint>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=map<smallint,smallint>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=map<int,int>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=map<largeint,largeint>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=map<float,float>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=map<double,double>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,datev2>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),datetimev2(0)>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,datev2>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),datetimev2(0)>")
    }

    test {
        sql "insert into tbl_map_types_mor (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_mor (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_mor (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_mor_c """select count() from tbl_map_types_mor;"""

    // test action for scala to array with map-scala-scala type
    test {
        sql "insert into tbl_array_map_types_mor (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<map<boolean,boolean>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<map<tinyint,tinyint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<map<smallint,smallint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<map<int,int>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<map<largeint,largeint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<map<float,float>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<map<double,double>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<map<decimalv3(20,3),decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<map<decimalv3(20,3),decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<map<datev2,datev2>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<map<datetimev2(0),datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<map<datev2,datev2>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<map<datetimev2(0),datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_mor (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_array_map_mor_c """select count() from tbl_array_map_types_mor;"""

    // test action for map with scala array-scala
    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_map_array_types_mor (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=map<boolean,array<boolean>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=map<tinyint,array<tinyint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=map<smallint,array<smallint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=map<int,array<int>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=map<largeint,array<largeint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=map<float,array<float>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=map<double,array<double>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,array<datev2>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,array<datev2>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_mor (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_array_mor_c """select count() from tbl_map_array_types_mor;"""


    // mow table test
    test {
        sql "insert into tbl_array_nested_types_mow (c_bool) select c_bool from tbl_scalar_types_dup"
        exception "java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<boolean>"
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<tinyint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<smallint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<int>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<largeint>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<float>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<double>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<datev2>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<datetimev2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<datev2>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<datetimev2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mow (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_mow_c """select count() from tbl_array_nested_types_mow;"""

    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_array_nested_types_mow2 (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<array<boolean>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<array<tinyint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<array<smallint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<array<int>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<array<largeint>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<array<float>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<array<double>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<array<datev2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<array<datev2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_mow2 (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_mow2_c """select count() from tbl_array_nested_types_mow2;"""


    // test action for scala to map with map-scala-scala type
    test {
        sql "insert into tbl_map_types_mow (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=map<boolean,boolean>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=map<tinyint,tinyint>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=map<smallint,smallint>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=map<int,int>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=map<largeint,largeint>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=map<float,float>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=map<double,double>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),decimalv3(20,3)>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,datev2>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),datetimev2(0)>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,datev2>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),datetimev2(0)>")
    }

    test {
        sql "insert into tbl_map_types_mow (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_mow (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_mow (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_mow_c """select count() from tbl_map_types_mow;"""

    // test action for scala to array with map-scala-scala type
    test {
        sql "insert into tbl_array_map_types_mow (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=array<map<boolean,boolean>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=array<map<tinyint,tinyint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=array<map<smallint,smallint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=array<map<int,int>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=array<map<largeint,largeint>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=array<map<float,float>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=array<map<double,double>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<map<decimalv3(20,3),decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=array<map<decimalv3(20,3),decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<map<datev2,datev2>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<map<datetimev2(0),datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=array<map<datev2,datev2>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=array<map<datetimev2(0),datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_mow (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_array_map_mow_c """select count() from tbl_array_map_types_mow;"""

    // test action for map with scala array-scala
    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_map_array_types_mow (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type boolean to target type=map<boolean,array<boolean>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type tinyint to target type=map<tinyint,array<tinyint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type smallint to target type=map<smallint,array<smallint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type int to target type=map<int,array<int>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type largeint to target type=map<largeint,array<largeint>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type float to target type=map<float,array<float>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type double to target type=map<double,array<double>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type decimalv3(20,3) to target type=map<decimalv3(20,3),array<decimalv3(20,3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,array<datev2>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datev2 to target type=map<datev2,array<datev2>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type datetimev2(0) to target type=map<datetimev2(0),array<datetimev2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_mow (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_array_mow_c """select count() from tbl_map_array_types_mow;"""

}
