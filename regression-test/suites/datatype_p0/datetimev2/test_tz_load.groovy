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

suite("test_tz_load", "nonConcurrent") {
    def table1 = "global_timezone_test"
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    
    sql "drop table if exists ${table1}"

    sql "SET GLOBAL time_zone = 'Asia/Shanghai'"
    
    sql """
    CREATE TABLE IF NOT EXISTS ${table1} (
      `id` int NULL,
      `dt_datetime` datetime NULL,
      `dt_date` date NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 3
    PROPERTIES (
        "replication_num" = "1" 
    )
    """

    // case1 stream load set time_zone = UTC
    //       stream load set timezone = UTC
    //       broker load set timezone = UTC
    // insert into does not support setting timezone
    // same as case3
    /*
    1	2024-04-11T08:00:13	2024-04-11
    1	2024-04-11T08:00:13	2024-04-11
    1	2024-04-11T08:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    */
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        set 'timezone', 'UTC'
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        set 'time_zone', 'UTC'
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    def label1 = "s3_load_default_" + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        LOAD LABEL ${label1} (
            DATA INFILE("s3://${s3BucketName}/load/test_global_timezone_streamload.csv")
            INTO TABLE ${table1}
            COLUMNS TERMINATED BY ","
            FORMAT AS "csv"
            (id, dt_datetime, dt_date)
        )
        WITH S3 (
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}"
        )
        PROPERTIES (
            "timezone" = "UTC"
        );
        """
    def max_try_time1 = 60000
    while (max_try_time1 > 0) {
        def result = sql "select * from ${table1}"
        if (result.size() == 6) {
            break;
        }
        Thread.sleep(1000)
        max_try_time1 -= 1000
        if (max_try_time1 <= 0) {
            throw new Exception("Load job timeout")
        }
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
    sql "truncate table ${table1}"

    // case2 stream load not set timezone
    //       broker load not set timezone
    //       insert into not set timezone
    /*
    1	2024-04-11T16:00:13	2024-04-11
    2	2024-04-11T06:00:13	2024-04-11
    */
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    
    def label2 = "s3_load_no_timezone_" + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        LOAD LABEL ${label2} (
            DATA INFILE("s3://${s3BucketName}/load/test_global_timezone_streamload.csv")
            INTO TABLE ${table1}
            COLUMNS TERMINATED BY ","
            FORMAT AS "csv"
            (id, dt_datetime, dt_date)
        )
        WITH S3 (
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}"
        );
        """

    sql """
        INSERT INTO ${table1}(id, dt_datetime, dt_date)
        SELECT 
            CAST(split_part(c1, ',', 1) AS INT) AS id,
            CAST(split_part(c1, ',', 2) AS DATETIME) AS dt_datetime,
            CAST(split_part(c1, ',', 3) AS DATE) AS dt_date
        FROM S3 (
            "uri" = "s3://${s3BucketName}/load/test_global_timezone_streamload.csv",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}",
            "format" = "csv"
        );
        """
    def max_try_time2 = 60000
    while (max_try_time2 > 0) {
        def result = sql "select * from ${table1}"
        if (result.size() == 6) {
            break;
        }
        Thread.sleep(1000)
        max_try_time2 -= 1000
        if (max_try_time2 <= 0) {
            throw new Exception("Load job timeout")
        }
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
    sql "truncate table ${table1}"

    // case3 not set timezone but default is UTC
    // same as case1
    /*
    1	2024-04-11T08:00:13	2024-04-11
    2	2024-04-10T22:00:13	2024-04-11
    */
    sql "SET GLOBAL time_zone = 'UTC'"
    streamLoad {
        table "${table1}"
        set 'column_separator', ','
        file "test_global_timezone_streamload2.csv"
        time 20000
    }
    
    def label3 = "s3_load_utc_" + UUID.randomUUID().toString().replaceAll("-", "")
    sql """
        LOAD LABEL ${label3} (
            DATA INFILE("s3://${s3BucketName}/load/test_global_timezone_streamload.csv")
            INTO TABLE ${table1}
            COLUMNS TERMINATED BY ","
            FORMAT AS "csv"
            (id, dt_datetime, dt_date)
        )
        WITH S3 (
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}"
        )
        PROPERTIES (
            "timezone" = "UTC"
        );
        """
    sql """
        INSERT INTO ${table1}(id, dt_datetime, dt_date)
        SELECT 
            CAST(split_part(c1, ',', 1) AS INT) AS id,
            CAST(split_part(c1, ',', 2) AS DATETIME) AS dt_datetime,
            CAST(split_part(c1, ',', 3) AS DATE) AS dt_date
        FROM S3 (
            "uri" = "s3://${s3BucketName}/load/test_global_timezone_streamload.csv",
            "s3.access_key" = "${ak}",
            "s3.secret_key" = "${sk}",
            "s3.endpoint" = "${s3Endpoint}",
            "s3.region" = "${s3Region}",
            "format" = "csv"
        );
        """
    def max_try_time3 = 60000
    while (max_try_time3 > 0) {
        def result = sql "select * from ${table1}"
        if (result.size() == 6) {
            break;
        }
        Thread.sleep(1000)
        max_try_time3 -= 1000
        if (max_try_time3 <= 0) {
            throw new Exception("Load job timeout")
        }
    }
    sql "sync"
    qt_global_offset "select * from ${table1} order by id"
}