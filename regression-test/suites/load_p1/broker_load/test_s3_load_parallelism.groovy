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

suite("test_s3_load_parallelism", "p1") {

    // Load file from S3 with load_parallelism specified
    def s3load_paral_wait = {tbl, fmt, path, paral ->
        String ak = getS3AK()
        String sk = getS3SK()
        String s3BucketName = getS3BucketName()
        String s3Endpoint = getS3Endpoint()
        String s3Region = getS3Region()
        def load_label = "part_" + UUID.randomUUID().toString().replace("-", "0")
        sql """
            LOAD LABEL ${load_label} (
                DATA INFILE("s3://${s3BucketName}/${path}")
                INTO TABLE ${tbl}
                COLUMNS TERMINATED BY ","
                FORMAT AS "${fmt}"
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}"
            )
            PROPERTIES(
                "load_parallelism" = "${paral}"
            );
        """
        // Waiting for job finished or cancelled
        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$load_label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + load_label)
                break;
            }
            if (result[0][2].equals("CANCELLED")) {
                assertTrue(false, "load failed: $result")
                break;
            }
            Thread.sleep(6000)
            max_try_milli_secs -= 6000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $load_label")
            }
        }
    }

    String enabled = context.config.otherConfigs.get("enableBrokerLoad")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def tableName = "paral_load"
        try {
            sql """drop table if exists ${tableName} force;"""
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT NOT NULL,
                    clientip VARCHAR(32),
                    request VARCHAR(256),
                    status INT,
                    size INT
                )
                ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 3
                PROPERTIES
                (
                    "replication_num" = "1"
                );
            """
            // Parallelly load csv from S3
            s3load_paral_wait.call(tableName, "CSV", "regression/load/data/test_load_parallelism.csv", 3)
            qt_paral_load_csv """ select count(1) from ${tableName}; """

            //Parallelly load json from S3
            sql """truncate table ${tableName};"""
            s3load_paral_wait.call(tableName, "JSON", "regression/load/data/test_load_parallelism.json", 3)
            qt_paral_load_json """ select count(1) from ${tableName}; """
        } finally {
            sql """drop table if exists ${tableName} force;"""
        }
    }
}

