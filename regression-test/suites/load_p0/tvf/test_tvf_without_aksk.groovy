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

suite("test_tvf_without_aksk", "load_p0") {
    def tableName = "tbl_without_aksk"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
             user_id            BIGINT       NOT NULL COMMENT "用户 ID",
             name               VARCHAR(20)           COMMENT "用户姓名",
             age                INT                   COMMENT "用户年龄"
         ) DUPLICATE KEY(user_id)
         DISTRIBUTED BY HASH(user_id) BUCKETS 1
         PROPERTIES (
             "replication_num" = "1"
         )
    """

    def label = UUID.randomUUID().toString().replace("-", "0")

    sql """
            INSERT INTO ${tableName}
                SELECT * FROM S3
                (
                    "uri" = "s3://${s3BucketName}/regression/load/data/example_0.csv",
                    "s3.endpoint" = "${getS3Endpoint()}",
                    "column_separator" = ",",
                    "format" = "csv"
                );
     """
    qt_sql """ SELECT * FROM ${tableName} order by user_id """

    sql """
            INSERT INTO ${tableName}
                SELECT * FROM S3
                (
                    "uri" = "s3://${s3BucketName}/regression/load/data/example_*.csv",
                    "s3.endpoint" = "${getS3Endpoint()}",
                    "column_separator" = ",",
                    "format" = "csv"
                );
     """
    qt_sql """ SELECT * FROM ${tableName} order by user_id """
}
