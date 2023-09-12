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

suite("test_javaudf_with_decimal") {
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    log.info("Jar path: ${jarPath}".toString())
    try {
        try_sql("drop function IF EXISTS getarrscale(Array<Decimal(15,3)>);")
        try_sql("drop function IF EXISTS getmapscale(Map<Decimal(15,3),Decimal(15,6)>);")
        try_sql("drop function IF EXISTS retscale(int);")
        try_sql("drop table IF EXISTS dbwithDecimal;")
        sql """
             CREATE TABLE IF NOT EXISTS dbwithDecimal (
            `id` INT(11) NULL COMMENT "" ,
            `arr` Array<Decimal(15,3)> NULL COMMENT ""   ,
            `mp` Map<Decimal(15,3),Decimal(15,6)> NULL COMMENT ""
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`id`)
                        DISTRIBUTED BY HASH(`id`) BUCKETS 1
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "storage_format" = "V2"
            );
        """
        sql """ INSERT INTO dbwithDecimal VALUES(1,[1.123,1.123456],{1.123:1.123456789});   """
        sql """ INSERT INTO dbwithDecimal VALUES(2,[2.123,2.123456],{2.123:2.123456789});   """


        sql """
          
        CREATE FUNCTION getarrscale(Array<Decimal(15,3)>) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyArrayDecimal",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        ); 
        
        """
        
        sql """
          
        CREATE FUNCTION getmapscale(Map<Decimal(15,3),Decimal(15,6)>) RETURNS int PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyMapDecimal",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        );
        
        """


        sql """
          
        CREATE FUNCTION retscale(int) RETURNS Map<Decimal(15,10),Decimal(15,10)> PROPERTIES (
            "file"="file://${jarPath}",
            "symbol"="org.apache.doris.udf.MyMapRetDecimal",
            "always_nullable"="true",
            "type"="JAVA_UDF"
        );
        """

        sql """
            set enable_nereids_planner=false;
        """

        qt_select_1 """ select arr,getarrscale(arr) from dbwithDecimal order by id; """

        qt_select_2 """ select mp,getmapscale(mp) from dbwithDecimal order by id ; """

        qt_select_3 """ select id,retscale(id) from dbwithDecimal order by id; """
    } finally {
        try_sql("drop function IF EXISTS getarrscale(Array<Decimal(15,3)>);")
        try_sql("drop function IF EXISTS getmapscale(Map<Decimal(15,3),Decimal(15,6)>);")
        try_sql("drop function IF EXISTS retscale(int);")
        try_sql("drop table IF EXISTS dbwithDecimal;")
    }
}
