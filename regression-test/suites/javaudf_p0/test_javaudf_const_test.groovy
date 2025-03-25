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

suite("test_javaudf_const_test") {
    def tableName = "test_javaudf_const_test"
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    // scp_udf_file_to_all_be(jarPath)

    log.info("Jar path: ${jarPath}".toString())
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS test_javaudf_const_test (
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
            struct_col STRUCT<s_id:int, s_name:string>,
            map_col map<string, string>
            )
            DISTRIBUTED BY HASH(int_col) PROPERTIES("replication_num" = "1");
        """
        StringBuilder sb = new StringBuilder()
        int i = 1
        for (; i < 10; i++) {
            sb.append("""
                (${i},${i%2},${i},${i}*2,${i}*3,${i}*4,${3.33/i},${(7.77/i).round(3)},${(3.1415/i).round(5)},"2023-10-${i+17}","2023-10-${i+10} 10:1${i}:11.234","row${i}",array(null, "nested${i}"),struct(${i}, "sa${i}"), {"k${i}":null,"k${i+1}":${i}}),
            """)
            log.info("${sb.toString()}");
        }
        sb.append("""
                (${i},${i%2},null,${i}*2,${i}*3,${i}*4,null,${(7.77/i).round(3)},${(3.1415/i).round(5)},null,"2023-10-${i+10} 10:${i}:11.234",null,array(null, "nested${i}"),struct(${i}, "saaaaaa"),{"k${i}":null,"k${i+1}":${i}})
            """)
        sql """ INSERT INTO ${tableName} VALUES
             ${sb.toString()}
            """
        File path = new File(jarPath)
        if (!path.exists()) {
            throw new IllegalStateException("""${jarPath} doesn't exist! """)
        }

        sql """DROP FUNCTION IF EXISTS const_boolean(int,boolean);"""
        sql """CREATE FUNCTION const_boolean(int,boolean) RETURNS boolean PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstBoolean",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_int(int,int);"""
        sql """CREATE FUNCTION const_int(int,int) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_largeint(int,largeint);"""
        sql """CREATE FUNCTION const_largeint(int,largeint) RETURNS largeint PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstLargeInt",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_decimal(int,decimal(15, 4));"""
        sql """CREATE FUNCTION const_decimal(int,decimal(15, 4)) RETURNS decimal(15, 4) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstDecimal",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_float(int,float);"""
        sql """CREATE FUNCTION const_float(int,float) RETURNS float PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstFloat",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_double(int,double);"""
        sql """CREATE FUNCTION const_double(int,double) RETURNS double PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstDouble",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_date(int,date);"""
        sql """CREATE FUNCTION const_date(int,date) RETURNS date PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstDate",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_datetime(int,datetime(6));"""
        sql """CREATE FUNCTION const_datetime(int,datetime(6)) RETURNS datetime(6) PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstDateTime",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_string(int,string);"""
        sql """CREATE FUNCTION const_string(int,string) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstString",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_array(int,array<string>);"""
        sql """CREATE FUNCTION const_array(int,array<string>) RETURNS array<string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstArray",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_struct(int,STRUCT<s_id:int, s_name:string>);"""
        sql """CREATE FUNCTION const_struct(int,STRUCT<s_id:int, s_name:string>) RETURNS STRUCT<s_id:int, s_name:string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstStruct",
            "type"="JAVA_UDF"
        );"""

        sql """DROP FUNCTION IF EXISTS const_map(int,map<string,string>);"""
        sql """CREATE FUNCTION const_map(int,map<string,string>) RETURNS map<string,string> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.UDFConstTest\$ConstMap",
            "type"="JAVA_UDF"
        );"""

        qt_java_udf_all_types """select
            int_col,
            const_boolean(int_col,true),
            const_int(int_col,2),
            const_largeint(int_col,3),
            const_decimal(int_col,4.4),
            const_float(int_col,5.5),
            const_double(int_col,6.6),
            const_date(int_col,'2020-02-02'),
            const_datetime(int_col,'2022-02-03 10:10:10'),
            const_string(int_col,'asd'),
            const_array(int_col,['a','b']),
            const_struct(int_col,struct(2, 'sa2')),
            const_map(int_col,{"aa":"bb"})
            from ${tableName} order by int_col;"""
    } finally {
        try_sql """DROP FUNCTION IF EXISTS const_boolean(int,boolean);"""
        try_sql """DROP FUNCTION IF EXISTS const_int(int,int);"""
        try_sql """DROP FUNCTION IF EXISTS const_largeint(int,largeint);"""
        try_sql """DROP FUNCTION IF EXISTS const_decimal(int,decimal(15, 4));"""
        try_sql """DROP FUNCTION IF EXISTS const_float(int,float);"""
        try_sql """DROP FUNCTION IF EXISTS const_double(int,double);"""
        try_sql """DROP FUNCTION IF EXISTS const_date(int,date);"""
        try_sql """DROP FUNCTION IF EXISTS const_datetime(int,datetime(6));"""
        try_sql """DROP FUNCTION IF EXISTS const_string(int,string);"""
        try_sql """DROP FUNCTION IF EXISTS const_array(int,array<string>);"""
        try_sql """DROP FUNCTION IF EXISTS const_struct(int,STRUCT<s_id:int, s_name:string>);"""
        try_sql """DROP FUNCTION IF EXISTS const_map(int,map<string,string>);"""
        try_sql("""DROP TABLE IF EXISTS ${tableName};""")
    }
}
