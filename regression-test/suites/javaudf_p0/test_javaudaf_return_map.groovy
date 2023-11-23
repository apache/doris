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

suite("test_javaudaf_return_map") {
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    log.info("Jar path: ${jarPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS aggmap(int);")
        try_sql("DROP FUNCTION IF EXISTS aggmap2(int,double);")
        try_sql("DROP FUNCTION IF EXISTS aggmap3(int,double);")
        try_sql("DROP TABLE IF EXISTS aggdb")
        sql """
            CREATE TABLE IF NOT EXISTS aggdb(
              `id` INT NULL COMMENT ""  ,
              `d` Double NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        );
        """


       
        sql """  INSERT INTO aggdb VALUES(1,0.01);   """
        sql """  INSERT INTO aggdb VALUES(2,0.02);   """
        sql """  INSERT INTO aggdb VALUES(3,0.03);   """
        sql """  INSERT INTO aggdb VALUES(4,0.04);   """
        sql """  INSERT INTO aggdb VALUES(5,0.05);   """ 


        sql """
          
            CREATE AGGREGATE FUNCTION aggmap(int) RETURNS Map<int,int> PROPERTIES (
                 "file"="file://${jarPath}",
                 "symbol"="org.apache.doris.udf.MySumReturnMapInt",
                 "type"="JAVA_UDF"
             ); 
        
        """

        sql """
          
            CREATE AGGREGATE FUNCTION aggmap2(int,double) RETURNS Map<int,double> PROPERTIES (
                 "file"="file://${jarPath}",
                 "symbol"="org.apache.doris.udf.MySumReturnMapIntDou",
                 "type"="JAVA_UDF"
             ); 

        
        """


        sql """
          
            CREATE AGGREGATE FUNCTION aggmap3(int,double) RETURNS Map<String,String> PROPERTIES (
                 "file"="file://${jarPath}",
                 "symbol"="org.apache.doris.udf.MyReturnMapString",
                 "type"="JAVA_UDF"
             ); 

        
        """

        qt_select_1 """ select aggmap(id) from aggdb; """

        qt_select_2 """ select aggmap2(id,d) from aggdb; """

        qt_select_3 """ select aggmap(id) from aggdb group by id order by id; """

        qt_select_4 """ select aggmap2(id,d) from aggdb group by id order by id; """

        qt_select_5 """ select aggmap3(id,d) from aggdb; """

        qt_select_6 """ select aggmap3(id,d) from aggdb group by id order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS aggmap(int);")
        try_sql("DROP FUNCTION IF EXISTS aggmap2(int,double);")
        try_sql("DROP FUNCTION IF EXISTS aggmap3(int,double);")
        try_sql("DROP TABLE IF EXISTS aggdb")
    }
}
