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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_javaudtf_custom_lib_all_types") {
    def tableName = "test_javaudtf_custom_lib_all_types"

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            int_col int,
            boolean_col boolean,
            tinyint_col tinyint,
            smallint_col smallint,
            bigint_col bigint,
            largeint_col largeint,
            decimal_col decimal(15, 4),
            float_col float,
            double_col double,
            date_col date,
            datetime_col datetime(6),
            string_col string,
            array_col array<string>,
            map_col map<string, string>
            )
            DISTRIBUTED BY HASH(int_col) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i},${i%2},${i},${i}*2,${i}*3,${i}*4,${3.33/i},${(7.77/i).round(3)},${(3.1415/i).round(5)},"2023-10-${i+17}","2023-10-${i+10} 10:1${i}:11.234","row${i}",array(null, "nested${i}"),{"k${i}":null,"k${i+1}":"value${i}"}),
            """)
        }
        sb.append("""
                (${i},${i%2},null,${i}*2,${i}*3,${i}*4,null,${(7.77/i).round(3)},${(3.1415/i).round(5)},null,"2023-10-${i+10} 10:${i}:11.234",null,array(null, "nested${i}"),{"k${i}":null,"k${i+1}":"value${i}"}),
            """)
        sb.append("""
                (null,null,null,null,null,null,null,null,null,null,null,null,null,null)
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        qt_select """select * from ${tableName} order by 1,2,3;"""


        sql """DROP FUNCTION IF EXISTS c_udtf_boolean(boolean, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_boolean(boolean, int) RETURNS array<boolean> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfBoolean",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_tinyint(tinyint, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_tinyint(tinyint, int) RETURNS array<tinyint> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfByte",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_short(smallint, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_short(smallint, int) RETURNS array<smallint> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfShort",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_int(int, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_int(int, int) RETURNS array<int> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_long(bigint, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_long(bigint, int) RETURNS array<bigint> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfLong",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_largeint(largeint, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_largeint(largeint, int) RETURNS array<largeint> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfLargeInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_decimal(decimal(15, 4), int);"""
        sql """CREATE TABLES FUNCTION c_udtf_decimal(decimal(15, 4), int) RETURNS array<decimal(15, 4)> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfDecimal",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_float(float, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_float(float, int) RETURNS array<float> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfFloat",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_double(double, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_double(double, int) RETURNS array<double> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfDouble",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_date(date, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_date(date, int) RETURNS array<date> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfDate",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_datetime(datetime(6), int);"""
        sql """CREATE TABLES FUNCTION c_udtf_datetime(datetime(6), int) RETURNS array<datetime(6)> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfDateTime",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_string(string, string);"""
        sql """CREATE TABLES FUNCTION c_udtf_string(string,string) RETURNS array<string> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfString",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_list(array<string>, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_list(array<string>, int) RETURNS array<string> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfList",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS c_udtf_map(map<string,string>, int);"""
        sql """CREATE TABLES FUNCTION c_udtf_map(map<string,string>, int) RETURNS array<string> PROPERTIES (
            "symbol"="org.apache.doris.udf.UDTFAllTypeTest\$UdtfMap",
            "type"="JAVA_UDF"
        );"""

        qt_select_boolean_col  """select int_col, boolean_col, e1 from ${tableName} lateral view c_udtf_boolean(boolean_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_tinyint_col  """select int_col, tinyint_col, e1 from ${tableName} lateral view c_udtf_tinyint(tinyint_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_smallint_col """select int_col, smallint_col, e1 from ${tableName} lateral view c_udtf_short(smallint_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_int_col      """select int_col, int_col, e1 from ${tableName} lateral view c_udtf_int(int_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_bigint_col   """select int_col, bigint_col, e1 from ${tableName} lateral view c_udtf_long(bigint_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_largeint_col """select int_col, largeint_col,e1 from ${tableName} lateral view c_udtf_largeint(largeint_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_decimal_col  """select int_col, decimal_col,e1 from ${tableName} lateral view c_udtf_decimal(decimal_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_float_col    """select int_col, float_col,e1 from ${tableName} lateral view c_udtf_float(float_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_double_col   """select int_col, double_col,e1 from ${tableName} lateral view c_udtf_double(double_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_date_col     """select int_col, date_col,e1 from ${tableName} lateral view c_udtf_date(date_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_datetime_col """select int_col, datetime_col,e1 from ${tableName} lateral view c_udtf_datetime(datetime_col, int_col) tmp1 as e1 order by int_col,2,3;"""
        qt_select_string_col   """select int_col, string_col,e1 from ${tableName} lateral view c_udtf_string(string_col, "") tmp1 as e1 order by int_col,2,3;"""
        qt_select_array_col    """select int_col, array_col,e1 from ${tableName} lateral view c_udtf_list(array_col, int_col) tmp1 as e1 order by int_col,3;"""
        qt_select_map_col      """select int_col, map_col,e1 from ${tableName} lateral view c_udtf_map(map_col, int_col) tmp1 as e1 order by int_col,3;"""
    } finally {
        try_sql """DROP FUNCTION IF EXISTS c_udtf_boolean (boolean, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_tinyint (tinyint, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_short (smallint, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_int (int, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_long (bigint, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_largeint (largeint, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_decimal (decimal(15, 4), int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_float (float, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_double (double, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_date (date, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_datetime (datetime(6), int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_string (string, string);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_list (array<string>, int);"""
        try_sql """DROP FUNCTION IF EXISTS c_udtf_map (map<string,string>, int);"""
    }
}
