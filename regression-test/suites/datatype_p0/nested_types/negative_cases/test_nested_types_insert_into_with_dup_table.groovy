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

suite("test_nested_types_insert_into_with_dup_table", "p0") {
    sql 'use regression_test_datatype_p0_nested_types'

    sql """
        truncate table `tbl_array_nested_types_dup`;
    """

    sql """
        truncate table `tbl_array_nested_types_dup2`;
    """

    sql """
        truncate table `tbl_map_types_dup`;
    """

    sql """
        truncate table `tbl_array_map_types_dup`;
    """

    sql """
        truncate table `tbl_map_array_types_dup`;
    """
    
    // test action for scala to array with scala type
    //  current we support char family to insert nested type
    test {
        sql "insert into tbl_array_nested_types_dup (c_bool) select c_bool from tbl_scalar_types_dup"
        exception "java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<BOOLEAN>"
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<TINYINT>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<SMALLINT>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<INT>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<LARGEINT>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<FLOAT>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<DOUBLE>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<DATEV2>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<DATETIMEV2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<DATEV2>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<DATETIMEV2(0)>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_dup (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_dup_c """select count() from tbl_array_nested_types_dup;"""

    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_array_nested_types_dup2 (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<ARRAY<BOOLEAN>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<ARRAY<TINYINT>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<ARRAY<SMALLINT>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<ARRAY<INT>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<ARRAY<LARGEINT>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<ARRAY<FLOAT>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<ARRAY<DOUBLE>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<ARRAY<DATEV2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<ARRAY<DATEV2>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_nested_types_dup2 (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_dup2_c """select count() from tbl_array_nested_types_dup2;"""


    // test action for scala to map with map-scala-scala type
    test {
        sql "insert into tbl_map_types_dup (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=MAP<BOOLEAN,BOOLEAN>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=MAP<TINYINT,TINYINT>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=MAP<SMALLINT,SMALLINT>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=MAP<INT,INT>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=MAP<LARGEINT,LARGEINT>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=MAP<FLOAT,FLOAT>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=MAP<DOUBLE,DOUBLE>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,DATEV2>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),DATETIMEV2(0)>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,DATEV2>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),DATETIMEV2(0)>")
    }

    test {
        sql "insert into tbl_map_types_dup (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_dup (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_types_dup (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_dup_c """select count() from tbl_map_types_dup;"""

    // test action for scala to array with map-scala-scala type
    test {
        sql "insert into tbl_array_map_types_dup (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=ARRAY<MAP<BOOLEAN,BOOLEAN>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=ARRAY<MAP<TINYINT,TINYINT>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=ARRAY<MAP<SMALLINT,SMALLINT>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=ARRAY<MAP<INT,INT>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=ARRAY<MAP<LARGEINT,LARGEINT>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=ARRAY<MAP<FLOAT,FLOAT>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=ARRAY<MAP<DOUBLE,DOUBLE>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=ARRAY<MAP<DECIMALV3(20, 3),DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<MAP<DATEV2,DATEV2>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<MAP<DATETIMEV2(0),DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=ARRAY<MAP<DATEV2,DATEV2>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=ARRAY<MAP<DATETIMEV2(0),DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_array_map_types_dup (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_array_map_dup_c """select count() from tbl_array_map_types_dup;"""

    // test action for map with scala array-scala
    // test action for scala to array with array-scala type
    test {
        sql "insert into tbl_map_array_types_dup (c_bool) select c_bool from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type BOOLEAN to target type=MAP<BOOLEAN,ARRAY<BOOLEAN>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_tinyint) select c_tinyint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type TINYINT to target type=MAP<TINYINT,ARRAY<TINYINT>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_smallint) select c_smallint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type SMALLINT to target type=MAP<SMALLINT,ARRAY<SMALLINT>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_int) select c_int from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type INT to target type=MAP<INT,ARRAY<INT>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_largeint) select c_largeint from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type LARGEINT to target type=MAP<LARGEINT,ARRAY<LARGEINT>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_float) select c_float from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type FLOAT to target type=MAP<FLOAT,ARRAY<FLOAT>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_double) select c_double from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DOUBLE to target type=MAP<DOUBLE,ARRAY<DOUBLE>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_decimal) select c_decimal from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_decimalv3) select c_decimalv3 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DECIMALV3(20, 3) to target type=MAP<DECIMALV3(20, 3),ARRAY<DECIMALV3(20, 3)>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_date) select c_date from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,ARRAY<DATEV2>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_datetime) select c_datetime from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_datev2) select c_datev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATEV2 to target type=MAP<DATEV2,ARRAY<DATEV2>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_datetimev2) select c_datetimev2 from tbl_scalar_types_dup"
        exception("java.sql.SQLException: errCode = 2, detailMessage = can not cast from origin type DATETIMEV2(0) to target type=MAP<DATETIMEV2(0),ARRAY<DATETIMEV2(0)>>")
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_char) select c_char from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_varchar) select c_varchar from tbl_scalar_types_dup"
        exception null
    }

    test {
        sql "insert into tbl_map_array_types_dup (c_string) select c_string from tbl_scalar_types_dup"
        exception null
    }

    qt_sql_nested_table_map_array_dup_c """select count() from tbl_map_array_types_dup;"""

}
