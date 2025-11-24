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

suite("test_tvf_empty_field_as_null", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def tableName = "test_tvf_empty_field_as_null"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
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

    qt_select """ select * from S3 (
                        "uri" = "http://${bucket}.${s3_endpoint}/regression/load/data/empty_field_as_null.csv",
                        "ACCESS_KEY"= "${ak}",
                        "SECRET_KEY" = "${sk}",
                        "format" = "csv",
                        "empty_field_as_null" = "true",
                        "column_separator" = ",",
                        "region" = "${region}"
                        );
                    """
}