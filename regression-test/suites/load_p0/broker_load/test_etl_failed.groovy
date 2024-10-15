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

suite("test_etl_failed", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def tableName = "test_etl_failed"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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
    String label = "test_etl_failed"
    String path = "s3://${s3BucketName}/regression/load/data/etl_failure/etl-failure.csv"
    String format = "CSV"
    String ak = getS3AK()
    String sk = getS3SK()
    sql """
        LOAD LABEL ${label} (
                DATA INFILE("$path")
                INTO TABLE ${tableName}
                FORMAT AS ${format}
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "provider" = "${getS3Provider()}"
            )
            PROPERTIES(
                "use_new_load_scan_node" = "true",
                "max_filter_ratio" = "0.1"
            );
    """

    def max_try_milli_secs = 600000
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$label" order by createtime desc limit 1; """
        logger.info("Load result: " + result[0])
        if (result[0][2].equals("FINISHED")) {
            logger.info("Load FINISHED " + label)
            assertTrue(1 == 2, "etl should be failed")
            break;
        }
        if (result[0][2].equals("CANCELLED") && result[0][13].contains("error_log")) {
            break;
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if(max_try_milli_secs <= 0) {
            assertTrue(1 == 2, "load Timeout: $label")
        }
    }
    String[][] result = sql """ show load warnings where label="$label" """
    assertTrue(result[0].size() > 1, "warning show be not null")
}

