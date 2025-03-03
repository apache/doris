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


suite("test_show_load_warnings","p0") {
    def tableName = "test_show_load_warnings_tb"
    def dbName = "test_show_load_warnings_db"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Provider = getS3Provider()

    try {
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        // create table and insert data
        sql """ drop table if exists ${dbName}.${tableName} force"""
        sql """
            CREATE TABLE ${dbName}.${tableName}(
                user_id            BIGINT       NOT NULL COMMENT "user id",
                name               VARCHAR(20)           COMMENT "name",
                age                INT                   COMMENT "age"
            )
            DUPLICATE KEY(user_id)
            DISTRIBUTED BY HASH(user_id) BUCKETS 10
            PROPERTIES
            (
            'replication_num' = '1'
            )
            """

        streamLoad {
            db "${dbName}"
            table "${tableName}"
            set 'column_separator', ','
            set 'columns', 'user_id,name,age'
            set 'strict_mode','true'
            file 'test_show_load_warnings.csv'
            time 3000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(result.contains("ErrorURL"))
                checkNereidsExecute("""show load warnings on '${json.ErrorURL}' """)

            }
        }


        sql """ DROP TABLE IF EXISTS ${dbName}.${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
                `k1` int(20) NULL,
                `k2` bigint(20) NULL,
                `v1` tinyint(4)  NULL,
                `v2` string  NULL,
                `v3` date NOT NULL,
                `v4` datetime NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 3
            PROPERTIES ("replication_allocation" = "tag.location.default: 1"); 
        """
        String label = "test_brokerload_failed"
        String path = "s3://${s3BucketName}/regression/load/data/etl_failure/etl-failure.csv"
        String format = "CSV"
        String ak = getS3AK()
        String sk = getS3SK()    
        sql """ use ${dbName} """   

        sql """
            LOAD LABEL ${label} (
                    DATA INFILE("$path")
                    INTO TABLE ${tableName}
                    FORMAT AS ${format}
                )
                WITH S3 (
                    "AWS_ACCESS_KEY" = "${ak}",
                    "AWS_SECRET_KEY" = "${sk}",
                    "AWS_ENDPOINT" = "${s3Endpoint}",
                    "AWS_REGION" = "${s3Region}",
                    "provider" = "${s3Provider}"
                )
        """

        checkNereidsExecute("""show load warnings from ${dbName} where label = '${label}' """)

        String[][] result = sql """ show load from ${dbName} where label = '${label}' """
        log.info("show load result: " + result[0])
        def job_id = result[0][0].toLong()
        checkNereidsExecute("""show load warnings from ${dbName} where load_job_id = ${job_id} """)


    }catch (Exception e) {
        // Log any exceptions that occur during testing
        log.error("Failed to execute SHOW LOAD WARNINGS command", e)
        throw e
    }finally {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ DROP DATABASE IF EXISTS ${dbName} """
    }


}
