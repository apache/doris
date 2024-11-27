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

suite("test_javaudf_agg_map") {
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    scp_udf_file_to_all_be(jarPath)
    log.info("Jar path: ${jarPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS mapii(Map<Int,Int>);")
        try_sql("DROP FUNCTION IF EXISTS mapid(Map<Int,Double>);")
        try_sql("DROP TABLE IF EXISTS db_agg_map")
        sql """
             CREATE TABLE IF NOT EXISTS db_agg_map(
                        `id` INT NULL COMMENT "",
                        `i` INT NULL COMMENT "",
                        `d` Double NULL COMMENT "",
                        `mii` Map<INT, INT> NULL COMMENT "",
                        `mid` Map<INT, Double> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2");
        """
        sql """ INSERT INTO db_agg_map VALUES(1, 10,1.1,{1:1,10:1,100:1},{1:1.1,11:11.1});   """
        sql """ INSERT INTO db_agg_map VALUES(2, 20,2.2,{2:2,20:2,200:2},{2:2.2,22:22.2});   """

        sql """
          
        CREATE AGGREGATE FUNCTION mapii(Map<Int,Int>) RETURNS BigInt PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumMapInt",
            "type"="JAVA_UDF"
        ); 
        
        """
        
        sql """
          
        CREATE AGGREGATE FUNCTION mapid(Map<Int,Double>) RETURNS Double PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MySumMapIntDou",
            "type"="JAVA_UDF"
        ); 
        
        """


        qt_select_1 """ select mapid(mid) from db_agg_map; """

        qt_select_2 """ select mapii(mii) from db_agg_map; """

    } finally {
        try_sql("DROP FUNCTION IF EXISTS mapii(Map<Int,Int>);")
        try_sql("DROP FUNCTION IF EXISTS mapid(Map<Int,Double>);")
        try_sql("DROP TABLE IF EXISTS db_agg_map")
    }
}
