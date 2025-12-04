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

suite("test_insert_statistic", "p0") {
    def dbName = "test_insert_statistic_db"
    def insert_tbl = "test_insert_statistic_tbl"
    def label = "test_insert_statistic_label"
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""
    sql """use ${dbName}"""

    // insert into value
    sql """ DROP TABLE IF EXISTS ${insert_tbl}_1"""
    sql """
     CREATE TABLE ${insert_tbl}_1 (
       `k1` char(5) NULL,
       `k2` int(11) NULL,
       `k3` tinyint(4) NULL,
       `k4` int(11) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """
    sql """ 
    INSERT INTO ${insert_tbl}_1 values(1, 1, 1, 1)
    """
    sql """ 
    INSERT INTO ${insert_tbl}_1 values(1, 1, 1, 1)
    """
    def result = sql "SHOW LOAD FROM ${dbName}"
    log.info("result size: " + result.size())
    assertEquals(result.size(), 0)

    // group commit
    sql """ set group_commit = sync_mode; """
    sql """ 
    INSERT INTO ${insert_tbl}_1 values(1, 1, 1, 1)
    """
    sql """ set group_commit = off_mode; """
    result = sql "SHOW LOAD FROM ${dbName}"
    log.info("result size: " + result.size())
    assertEquals(result.size(), 0)

    // insert into select
    sql """ DROP TABLE IF EXISTS ${insert_tbl}_2"""
    sql """
     CREATE TABLE ${insert_tbl}_2 (
       `k1` char(5) NULL,
       `k2` int(11) NULL,
       `k3` tinyint(4) NULL,
       `k4` int(11) NULL
     ) ENGINE=OLAP
     DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)
     COMMENT 'OLAP'
     DISTRIBUTED BY HASH(`k1`) BUCKETS 5
     PROPERTIES (
       "replication_num"="1"
     );
    """
    sql """ 
    INSERT INTO ${insert_tbl}_2 select * from ${insert_tbl}_1
    """
    result = sql "SHOW LOAD FROM ${dbName}"
    logger.info("JobDetails: " + result[0][14])
    def json = parseJson(result[0][14])
    assertEquals(json.ScannedRows, 3)
    assertEquals(json.FileNumber, 0)
    assertEquals(json.FileSize, 0)
    assertTrue(json.LoadBytes > 0)

    // insert into s3 tvf
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    sql """ DROP TABLE IF EXISTS ${insert_tbl}_3 """
    sql """
        CREATE TABLE IF NOT EXISTS ${insert_tbl}_3 (
            `k1` int NULL,
            `k2` varchar(50) NULL,
            `v1` varchar(50)  NULL,
            `v2` varchar(50)  NULL,
            `v3` varchar(50)  NULL,
            `v4` varchar(50)  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ insert into ${insert_tbl}_3 select * from S3 (
                        "uri" = "http://${bucket}.${s3_endpoint}/regression/load/data/empty_field_as_null.csv",
                        "ACCESS_KEY"= "${ak}",
                        "SECRET_KEY" = "${sk}",
                        "format" = "csv",
                        "empty_field_as_null" = "true",
                        "column_separator" = ",",
                        "region" = "${region}"
                        );
                    """
    result = sql "SHOW LOAD FROM ${dbName}"
    logger.info("JobDetails: " + result[1][14])
    json = parseJson(result[1][14])
    assertEquals(json.ScannedRows, 2)
    assertEquals(json.FileNumber, 1)
    assertEquals(json.FileSize, 86)
    assertEquals(result.size(), 2)
    assertTrue(json.LoadBytes > 0)
}