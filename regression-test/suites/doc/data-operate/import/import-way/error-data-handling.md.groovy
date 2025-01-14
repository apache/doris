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

suite("test_error_data_handling", "p0") {
    def tableName = "test_error_data_handling"
    // partial update strict mode
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(10),
            age INT,
            city VARCHAR(10),
            balance DECIMAL(9, 0),
            last_access_time DATETIME
        ) 
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO ${tableName} VALUES (1, 'kevin', 18, 'shenzhen', 400, '2023-07-01 12:00:00')
    """

    streamLoad {
        table "${tableName}"
        file "test_data_partial.csv"
        set 'column_separator', ','
        set 'columns', 'id,balance,last_access_time'
        set 'partial_columns', 'true'
        set 'strict_mode', 'true'
        time 10000
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
            assertEquals(3, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }

    streamLoad {
        table "${tableName}"
        file "test_data_partial.csv"
        set 'column_separator', ','
        set 'columns', 'id,balance,last_access_time'
        set 'partial_columns', 'true'
        set 'strict_mode', 'false'
        time 10000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    qt_select "SELECT * FROM ${tableName} ORDER BY id"
    

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id TINYINT NULL,
            name VARCHAR(10) NULL,
            age DECIMAL(1,0) NULL,
            score INT NULL
    ) 
    DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1")
    """

    streamLoad {
        table "${tableName}"
        file "error_data_handling.csv"
        set 'column_separator', ','
        set 'strict_mode', 'false'

        time 10000 
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    streamLoad {
        table "${tableName}"
        file "error_data_handling.csv"
        set 'column_separator', ','
        set 'strict_mode', 'true'

        time 10000 
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
            assertEquals(3, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
        }
    }

    streamLoad {
        table "${tableName}"
        file "error_data_handling.csv"
        set 'column_separator', ','
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.8'

        time 10000 
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
        }
    }

    qt_select "SELECT * FROM ${tableName} ORDER BY id"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id TINYINT NULL,
            name VARCHAR(10) NULL,
            age DECIMAL(1,0) NULL,
            score INT NULL
        ) 
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    String ak = getS3AK()
    String sk = getS3SK()
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def label = "test_error_data_handling" + UUID.randomUUID().toString().replace("-", "_") 
    sql """
        LOAD LABEL ${label} (
            DATA INFILE("s3://${s3BucketName}/regression/load/data/error_data_handling.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
            "strict_mode" = "false"
        )
    """
    def max_try_milli_secs = 1200000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="${label}" order by createtime desc limit 1; """

        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED ${label}: $result")
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "load failed: $result")
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "load Timeout: $label")
        }
    }

    label = "test_error_data_handling" + UUID.randomUUID().toString().replace("-", "_") 
    sql """
        LOAD LABEL ${label} (
            DATA INFILE("s3://${s3BucketName}/regression/load/data/error_data_handling.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
            "strict_mode" = "true"
        )
    """
    max_try_milli_secs = 1200000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="${label}" order by createtime desc limit 1; """

        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED ${label}: $result")
            assertTrue(false, "load should be failed but was success: $result")
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            logger.info("Load FINISHED ${label}: $result")
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "load Timeout: $label")
        }
    }

    label = "test_error_data_handling" + UUID.randomUUID().toString().replace("-", "_") 
    sql """
        LOAD LABEL ${label} (
            DATA INFILE("s3://${s3BucketName}/regression/load/data/error_data_handling.csv")
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "provider" = "${getS3Provider()}"
        )
        properties(
            "max_filter_ratio" = "0.8",
            "strict_mode" = "true"
        )
    """
    max_try_milli_secs = 1200000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="${label}" order by createtime desc limit 1; """

        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED ${label}: $result")
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "load failed: $result")
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "load Timeout: $label")
        }
    }
    qt_select "SELECT * FROM ${tableName} ORDER BY id"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id TINYINT NULL,
            name VARCHAR(10) NULL,
            age DECIMAL(1,0) NULL,
            score INT NULL
        ) 
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        LOAD DATA LOCAL
        INFILE '${context.dataPath}/error_data_handling.csv'
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ","
    """
    test {
        sql """
            LOAD DATA LOCAL
            INFILE '${context.dataPath}/error_data_handling.csv'
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY ","
            PROPERTIES (
                "strict_mode" = "true"
            );
        """
        exception "too many filtered rows"
    }

    sql """
        LOAD DATA LOCAL
        INFILE '${context.dataPath}/error_data_handling.csv'
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ","
        PROPERTIES (
            "max_filter_ratio" = "0.8",
            "strict_mode" = "true"
        );
    """
    qt_select "SELECT * FROM ${tableName} ORDER BY id"

}