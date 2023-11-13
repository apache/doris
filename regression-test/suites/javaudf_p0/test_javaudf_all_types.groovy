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

suite("test_javaudf_all_types") {
    def tableName = "test_javaudf_all_types"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""

    log.info("Jar path: ${jarPath}".toString())
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
            map_col map<string, int>
            )
            DISTRIBUTED BY HASH(int_col) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i},${i%2},${i},${i}*2,${i}*3,${i}*4,${3.33/i},${7.77/i},${3.1415/i},"2023-10-${i+17}","2023-10-${i+10} 10:1${i}:11.234","row${i}",array(null, "nested${i}"),{"k${i}":null,"k${i+1}":${i}}),
            """)
        }
        sb.append("""
                (${i},${i%2},null,${i}*2,${i}*3,${i}*4,null,${7.77/i},${3.1415/i},null,"2023-10-${i+10} 10:${i}:11.234",null,array(null, "nested${i}"),{"k${i}":null,"k${i+1}":${i}})
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS echo_boolean(boolean);"""
        sql """CREATE FUNCTION echo_boolean(boolean) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoBoolean",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_tinyint(tinyint);"""
        sql """CREATE FUNCTION echo_tinyint(tinyint) RETURNS tinyint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoByte",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_short(smallint);"""
        sql """CREATE FUNCTION echo_short(smallint) RETURNS smallint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoShort",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_int(int);"""
        sql """CREATE FUNCTION echo_int(int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_long(bigint);"""
        sql """CREATE FUNCTION echo_long(bigint) RETURNS bigint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoLong",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_largeint(largeint);"""
        sql """CREATE FUNCTION echo_largeint(largeint) RETURNS largeint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoLargeInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_decimal(decimal(15, 4));"""
        sql """CREATE FUNCTION echo_decimal(decimal(15, 4)) RETURNS decimal(15, 4) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDecimal",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_float(float);"""
        sql """CREATE FUNCTION echo_float(float) RETURNS float PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoFloat",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_double(double);"""
        sql """CREATE FUNCTION echo_double(double) RETURNS double PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDouble",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_date(date);"""
        sql """CREATE FUNCTION echo_date(date) RETURNS date PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDate",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_date2(date);"""
        sql """CREATE FUNCTION echo_date2(date) RETURNS date PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDate2",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_date3(date);"""
        sql """CREATE FUNCTION echo_date3(date) RETURNS date PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDate3",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_datetime(datetime(6));"""
        sql """CREATE FUNCTION echo_datetime(datetime(6)) RETURNS datetime(6) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDateTime",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_datetime2(datetime(6));"""
        sql """CREATE FUNCTION echo_datetime2(datetime(6)) RETURNS datetime(6) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDateTime2",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_datetime3(datetime(6));"""
        sql """CREATE FUNCTION echo_datetime3(datetime(6)) RETURNS datetime(6) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoDateTime3",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_string(string);"""
        sql """CREATE FUNCTION echo_string(string) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoString",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_list(array<string>);"""
        sql """CREATE FUNCTION echo_list(array<string>) RETURNS array<string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoList",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS echo_map(map<string,int>);"""
        sql """CREATE FUNCTION echo_map(map<string,int>) RETURNS map<string,int> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.Echo\$EchoMap",
            "type"="JAVA_UDF"
        );"""

        qt_java_udf_all_types """select
            int_col,
            echo_boolean(boolean_col),
            echo_tinyint(tinyint_col),
            echo_short(smallint_col),
            echo_int(int_col),
            echo_long(bigint_col),
            echo_largeint(largeint_col),
            echo_decimal(decimal_col),
            echo_float(float_col),
            echo_double(double_col),
            echo_date(date_col),
            echo_date2(date_col),
            echo_date3(date_col),
            echo_datetime(datetime_col),
            echo_datetime2(datetime_col),
            echo_datetime3(datetime_col),
            echo_string(string_col),
            echo_list(array_col),
            echo_map(map_col)
            from ${tableName} order by int_col;"""
    } finally {
        try_sql """DROP FUNCTION IF EXISTS echo_boolean(boolean);"""
        try_sql """DROP FUNCTION IF EXISTS echo_tinyint(tinyint);"""
        try_sql """DROP FUNCTION IF EXISTS echo_short(smallint);"""
        try_sql """DROP FUNCTION IF EXISTS echo_int(int);"""
        try_sql """DROP FUNCTION IF EXISTS echo_long(bigint);"""
        try_sql """DROP FUNCTION IF EXISTS echo_largeint(largeint);"""
        try_sql """DROP FUNCTION IF EXISTS echo_decimal(decimal(15, 4));"""
        try_sql """DROP FUNCTION IF EXISTS echo_float(float);"""
        try_sql """DROP FUNCTION IF EXISTS echo_double(double);"""
        try_sql """DROP FUNCTION IF EXISTS echo_date(date);"""
        try_sql """DROP FUNCTION IF EXISTS echo_date2(date);"""
        try_sql """DROP FUNCTION IF EXISTS echo_date3(date);"""
        try_sql """DROP FUNCTION IF EXISTS echo_datetime(datetime(6));"""
        try_sql """DROP FUNCTION IF EXISTS echo_datetime2(datetime(6));"""
        try_sql """DROP FUNCTION IF EXISTS echo_datetime3(datetime(6));"""
        try_sql """DROP FUNCTION IF EXISTS echo_string(string);"""
        try_sql """DROP FUNCTION IF EXISTS echo_list(array<string>);"""
        try_sql """DROP FUNCTION IF EXISTS echo_map(map<string,int>);"""
        try_sql("""DROP TABLE IF EXISTS ${tableName};""")
    }
}
