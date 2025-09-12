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

suite("test_tvf_error_data", "p0") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    String path = "load_p0/tvf"

    def tableName = "test_tvf_error_data"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
        CREATE TABLE IF NOT EXISTS test_tvf_error_data (
            `k1` int NULL,
            `k2` datetime NULL,
            `v1` varchar(50)  NULL,
            `v2` varchar(50)  NULL,
            `v3` varchar(50)  NULL,
            `v4` varchar(50)  NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`,`k2`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    // date with quotatio 
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_date_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """
 
    // string with quotatio init
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """

    // string with double quotatio init
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_varchar_double_quotatio.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """

    // string with symbol init
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_symbol.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """

    // string with enclose init
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_enclose.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """

    // string with separator init
    qt_sql """       
              INSERT INTO ${tableName}
                     SELECT * FROM S3 (
                            "uri" = "http://${bucket}.${s3_endpoint}/${path}/tvf_error_data_with_separator.csv",
                            "ACCESS_KEY"= "${ak}",
                            "SECRET_KEY" = "${sk}",
                            "format" = "csv",
                            "region" = "${region}"
                            );
       """
}
