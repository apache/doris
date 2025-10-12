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

suite("test_pythonudtf_map") {
    def pyPath = """${context.file.parent}/udtf_scripts/pyudtf.zip"""
    scp_udf_file_to_all_be(pyPath)
    log.info("Python Zip path: ${pyPath}".toString())
    try {
        try_sql("DROP TABLE IF EXISTS map_ss")
        sql """ CREATE TABLE IF NOT EXISTS map_ss (
              `id` INT(11) NULL COMMENT "",
              `m` Map<STRING, INT> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
        ); """

        sql """ INSERT INTO map_ss VALUES(1, {"114":514,"1919":810});         """
        sql """ INSERT INTO map_ss VALUES(2, {"a":11,"def":22,"hij":33});   """

        sql """ DROP FUNCTION IF EXISTS udfss(Map<String, INT>); """

        sql """ CREATE TABLES FUNCTION udfss(Map<String, INT>) RETURNS ARRAY<MAP<STRING,INT>> PROPERTIES (
            "file"="file://${pyPath}",
            "symbol"="map_test",
            "type"="PYTHON_UDF"
        ); """

        qt_select_1 """ select id, e1 from map_ss lateral view udfss(m) temp as e1 order by id; """
    } finally {
        try_sql("DROP FUNCTION IF EXISTS udfss(Map<String, INT>);")
        try_sql("DROP TABLE IF EXISTS map_ii")
        try_sql("DROP TABLE IF EXISTS map_ss")
    }
}
