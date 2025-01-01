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

suite("nereids_test_javaudf_ip") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    def jarPath = """${context.file.parent}/../../javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)

    try {
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv4_test1(ipv4);"""
        sql """ CREATE FUNCTION java_udf_ipv4_test1(ipv4) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV4TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv4_test2(string);"""
        sql """ CREATE FUNCTION java_udf_ipv4_test2(string) RETURNS ipv4 PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV4TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv4_test3(array<ipv4>);"""
        sql """ CREATE FUNCTION java_udf_ipv4_test3(array<ipv4>) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV4TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv4_test4();"""
        sql """ CREATE FUNCTION java_udf_ipv4_test4() RETURNS array<ipv4> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV4TypeTest",
            "type"="JAVA_UDF"
        ); """

        sql """DROP TABLE IF EXISTS test_udf_ip;"""
        sql """
                CREATE TABLE test_udf_ip
                (
                    k1 BIGINT ,
                    k4 ipv4 ,
                    k6 ipv6 ,
                    s string 
                )
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES("replication_num" = "1");
        """
        sql """ insert into test_udf_ip values(1,123,34141,"0.0.0.123") , (2,3114,318903,"0.0.0.123") , (3,7832131,192837891738927931231,"2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D"),(4,null,null,"2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D"); """
        qt_select_ipv4_1 """ select k1,k4,java_udf_ipv4_test1(k4) from test_udf_ip order by k1   """
        qt_select_ipv4_2 """ select k1,s,java_udf_ipv4_test2(s) from test_udf_ip where IS_IPV4_STRING(s) order by k1  """
        qt_select_ipv4_3 """ select java_udf_ipv4_test3(array_sort(array_agg(k4))) from test_udf_ip   """
        qt_select_ipv4_4 """ select k1, java_udf_ipv4_test4() from test_udf_ip order by k1   """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv4_test1(ipv4);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv4_test2(string);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv4_test3(array<ipv4>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv4_test4();")
    }

    try {
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv6_test1(ipv6);"""
        sql """ CREATE FUNCTION java_udf_ipv6_test1(ipv6) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV6TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv6_test2(string);"""
        sql """ CREATE FUNCTION java_udf_ipv6_test2(string) RETURNS ipv6 PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV6TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv6_test3(array<ipv6>);"""
        sql """ CREATE FUNCTION java_udf_ipv6_test3(array<ipv6>) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV6TypeTest",
            "type"="JAVA_UDF"
        ); """
        sql """ DROP FUNCTION IF EXISTS java_udf_ipv6_test4();"""
        sql """ CREATE FUNCTION java_udf_ipv6_test4() RETURNS array<ipv6> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.IPV6TypeTest",
            "type"="JAVA_UDF"
        ); """

        sql """DROP TABLE IF EXISTS test_udf_ip;"""
        sql """
                CREATE TABLE test_udf_ip
                (
                    k1 BIGINT ,
                    k4 ipv4 ,
                    k6 ipv6 ,
                    s string 
                )
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES("replication_num" = "1");
        """
        sql """ insert into test_udf_ip values(1,123,34141,"0.0.0.123") , (2,3114,318903,"0.0.0.123") , (3,7832131,192837891738927931231,"2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D"),(4,null,null,"2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D"); """
        qt_select_ipv6_1 """ select k1,k6,java_udf_ipv6_test1(k6) from test_udf_ip order by k1   """
        qt_select_ipv6_2 """ select k1,s,java_udf_ipv6_test2(s) from test_udf_ip where IS_IPV6_STRING(s) order by k1  """
        qt_select_ipv6_3 """ select java_udf_ipv6_test3(array_sort(array_agg(k6))) from test_udf_ip   """
        qt_select_ipv6_4 """ select k1, java_udf_ipv6_test4() from test_udf_ip order by k1   """




        sql """ CREATE AGGREGATE FUNCTION udaf_my_sum_ip(ipv6) RETURNS string PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumIP",
            "always_nullable"="false",
            "type"="JAVA_UDF"
        ); """


        qt_select_ipv6_5  """ select udaf_my_sum_ip(k6) from test_udf_ip; """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv6_test1(ipv4);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv6_test2(string);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv6_test3(array<ipv4>);")
        try_sql("DROP FUNCTION IF EXISTS java_udf_ipv6_test4();")
        try_sql("DROP FUNCTION IF EXISTS udaf_my_sum_ip(ipv6);")
    }
}
