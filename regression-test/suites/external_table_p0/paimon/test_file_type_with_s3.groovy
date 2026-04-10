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

suite("test_file_type_with_s3", "p0,external") {
    logger.info("start test_file_type_with_s3 test")
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    // Use standard AWS S3 endpoint (same as test_paimon_s3.groovy)
    String aws_ak = context.config.otherConfigs.get("AWSAK")
    String aws_sk = context.config.otherConfigs.get("AWSSK")
    String aws_endpoint = "s3.ap-east-1.amazonaws.com"
    String aws_region = "ap-east-1"
    String s3_warehouse = "s3://selectdb-qa-datalake-test-hk/paimon_warehouse"

    // Create fileset table on AWS S3 Paimon warehouse to discover data files
    sql """ drop table if exists test_file; """
    sql """
        CREATE EXTERNAL TABLE test_file (
                `file` FILE NULL
            )
            ENGINE = fileset
            PROPERTIES (
                "s3.region" = "${aws_region}",
                "s3.endpoint" = "${aws_endpoint}",
                "s3.access_key" = "${aws_ak}",
                "s3.secret_key" = "${aws_sk}",
                "location" = "${s3_warehouse}/aws_db.db/hive_test_table/bucket-0/*.parquet"
        );
    """

    // Verify fileset table returns data files
    def fileset_result = sql """select * from test_file"""
    assertTrue(fileset_result.size() > 0, "fileset table should list at least one parquet file")
    logger.info("fileset returned ${fileset_result.size()} files")

    // Extract a real file URI from the fileset result for to_file() testing
    def fileJson = new groovy.json.JsonSlurper().parseText(fileset_result[0][0].toString())
    String data_file_uri = fileJson.uri
    logger.info("discovered data file URI: ${data_file_uri}")

    // Test successful to_file() with the discovered S3 object
    def to_file_result = sql """
        select to_file("${data_file_uri}", "${aws_region}", "${aws_endpoint}", "${aws_ak}", "${aws_sk}")
    """
    assertTrue(to_file_result.size() == 1, "to_file() should return one row")
    def toFileJson = new groovy.json.JsonSlurper().parseText(to_file_result[0][0].toString())
    assertEquals(data_file_uri, toFileJson.uri)
    assertTrue(toFileJson.size > 0, "file size should be positive")

    // Error: invalid URI format (not a valid S3 path)
    test {
        sql """select to_file("s3", "${aws_region}", "${aws_endpoint}", "${aws_ak}", "${aws_sk}");"""
        exception "INVALID_ARGUMENT"
    }

    // Error: wrong credentials
    test {
        sql """
            select to_file("${data_file_uri}", "${aws_region}", "${aws_endpoint}", "${aws_ak}", "wrong_secret_key");
        """
        exception "INVALID_ARGUMENT"
    }

    // Error: INSERT into fileset table (not an OLAP table)
    test {
        sql """
            insert into test_file values(
                to_file("${data_file_uri}", "${aws_region}", "${aws_endpoint}", "${aws_ak}", "${aws_sk}")
            );
        """
        exception "not an OLAP table"
    }

    // Error: ORDER BY on FILE column
    test {
        sql """select file from test_file order by file;"""
        exception "and don't support filter, group by or order by"
    }

    // Error: GROUP BY on FILE column
    test {
        sql """select file from test_file group by file;"""
        exception "and don't support filter, group by or order by"
    }

    // Error: WHERE condition on FILE column
    test {
        sql """
            select file from test_file where file = to_file("${data_file_uri}",
                "${aws_region}", "${aws_endpoint}", "${aws_ak}", "${aws_sk}");
        """
        exception "FILE type does not support filter condition"
    }

    // Error: CREATE OLAP table with FILE column
    test {
        sql """
            create table test_file_2 (
                `file` FILE NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`file`)
            DISTRIBUTED BY HASH(`file`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
        """
        exception "only allowed in fileset engine"
    }

}
