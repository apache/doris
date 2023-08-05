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

suite("test_javaudf_map") {
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    log.info("Jar path: ${jarPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS udfii(Map<INT, INT>);")
        try_sql("DROP FUNCTION IF EXISTS udfss(Map<String, String>);")
        try_sql("DROP TABLE IF EXISTS map_ii")
        try_sql("DROP TABLE IF EXISTS map_ss")
        sql """
            CREATE TABLE IF NOT EXISTS map_ii (
                        `id` INT(11) NULL COMMENT "",
                        `m` Map<INT, INT> NULL COMMENT ""
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`id`)
                        DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "storage_format" = "V2"
            );
        """
                 sql """  """
        sql """ INSERT INTO map_ii VALUES(1, {1:1,10:1,100:1}); """
        sql """ INSERT INTO map_ii VALUES(2, {2:1,20:1,200:1,2000:1});   """
        sql """ INSERT INTO map_ii VALUES(3, {3:1}); """
        sql """ DROP FUNCTION IF EXISTS udfii(Map<INT, INT>); """
        sql """ CREATE FUNCTION udfii(Map<INT, INT>) RETURNS INT PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MapIntIntTest",
            "type"="JAVA_UDF"
        ); """


        qt_select_1 """ select m,udfii(m) from map_ii order by id; """

        sql """ CREATE TABLE IF NOT EXISTS map_ss (
              `id` INT(11) NULL COMMENT "",
              `m` Map<String, String> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        ); """
        sql """ INSERT INTO map_ss VALUES(1, {"114":"514","1919":"810"});         """
        sql """ INSERT INTO map_ss VALUES(2, {"a":"bc","def":"g","hij":"k"});   """
        sql """ DROP FUNCTION IF EXISTS udfss(Map<String, String>); """

        sql """ CREATE FUNCTION udfss(Map<String, String>) RETURNS STRING PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MapStrStrTest",
            "type"="JAVA_UDF"
        ); """

        qt_select_2 """ select m,udfss(m) from map_ss order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udfii(Map<INT, INT>);")
        try_sql("DROP FUNCTION IF EXISTS udfss(Map<String, String>);")
        try_sql("DROP TABLE IF EXISTS map_ii")
        try_sql("DROP TABLE IF EXISTS map_ss")
    }
}
