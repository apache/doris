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

suite("test_pythonudf_ret_map") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        try_sql("DROP FUNCTION IF EXISTS retii(map<int,int>);")
        try_sql("DROP FUNCTION IF EXISTS retss(map<String,String>);")
        try_sql("DROP FUNCTION IF EXISTS retid(map<int,Double>);")
        try_sql("DROP FUNCTION IF EXISTS retidss(int ,double);")
        try_sql("DROP TABLE IF EXISTS db")
        try_sql("DROP TABLE IF EXISTS dbss")
        sql """
             CREATE TABLE IF NOT EXISTS db(
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
        sql """ INSERT INTO db VALUES(1, 10,1.1,{1:1,10:1,100:1},{1:1.1,11:11.1});   """
        sql """ INSERT INTO db VALUES(2, 20,2.2,{2:2,20:2,200:2},{2:2.2,22:22.2});   """

        sql """
              CREATE TABLE IF NOT EXISTS dbss(
              `id` INT NULL COMMENT "",
   					`m` Map<String, String> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2");
        """

        sql """ INSERT INTO dbss VALUES(1,{"abc":"efg","h":"i"}); """
        sql """ INSERT INTO dbss VALUES(2,{"j":"k"}); """


        sql """
          
        CREATE FUNCTION retii(map<int,int>) RETURNS map<int,int> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="map_ret_int_int_test",
            "type"="PYTHON_UDF"
        ); 
        
        """

        sql """
          
        CREATE FUNCTION retss(map<String,String>) RETURNS map<String,String> PROPERTIES (
                    "file"="file://${pyPath}",
                    "symbol"="map_ret_string_string_test",
                    "always_nullable"="true",
                    "type"="PYTHON_UDF"
        ); 
        
        """


        sql """
          
            CREATE FUNCTION retid(map<int,Double>) RETURNS map<int,Double> PROPERTIES (
                        "file"="file://${pyPath}",
                        "symbol"="map_ret_int_double_test",
                        "always_nullable"="true",
                        "type"="PYTHON_UDF"
            ); 
        
        """

        sql """
          
        CREATE FUNCTION retidss(int ,double) RETURNS map<String,String> PROPERTIES (
                    "file"="file://${pyPath}",
                    "symbol"="map_int_double_ret_string_string_test",
                    "always_nullable"="true",
                    "type"="PYTHON_UDF"
        ); 
        
        """

        qt_select_1 """ select mid , retid(mid) from db order by id; """

        qt_select_2 """ select mii , retii(mii) from db order by id; """

        qt_select_3 """ select i,d,retidss(i,d) from db order by id; """

        qt_select_4 """ select m,retss(m) from dbss order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS retii(map<int,int>);")
        try_sql("DROP FUNCTION IF EXISTS retss(map<String,String>);")
        try_sql("DROP FUNCTION IF EXISTS retid(map<int,Double>);")
        try_sql("DROP FUNCTION IF EXISTS retidss(int ,double);")
        try_sql("DROP TABLE IF EXISTS db")
        try_sql("DROP TABLE IF EXISTS dbss")
    }
}
