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

suite("test_pythonudf_map") {
    def pyPath = """${context.file.parent}/udf_scripts/pyudf.zip"""
    scp_udf_file_to_all_be(pyPath)
    def runtime_version = "3.8.10"
    log.info("Python Zip path: ${pyPath}".toString())
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
            "file"="file://${pyPath}",
            "symbol"="map_int_int_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
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
            "file"="file://${pyPath}",
            "symbol"="map_string_string_test.evaluate",
            "type"="PYTHON_UDF",
            "always_nullable" = "true",
            "runtime_version" = "${runtime_version}"
        ); """

        qt_select_2 """ select m,udfss(m) from map_ss order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udfii(Map<INT, INT>);")
        try_sql("DROP FUNCTION IF EXISTS udfss(Map<String, String>);")
        try_sql("DROP TABLE IF EXISTS map_ii")
        try_sql("DROP TABLE IF EXISTS map_ss")
    }
}
