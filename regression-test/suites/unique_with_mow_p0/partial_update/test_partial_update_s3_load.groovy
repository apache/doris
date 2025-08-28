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

suite("test_partial_update_s3_load", "p0") {

    def tableName = "test_partial_update_s3_load"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "replication_num" = "1"); """

    sql "insert into ${tableName} values(1,1,1,1,1);"
    sql "insert into ${tableName} values(2,2,2,2,2);"
    sql "insert into ${tableName} values(3,3,3,3,3);"
    sql "sync;"
    qt_sql "select * from ${tableName} order by k1;"


    def label = "test_pu" + UUID.randomUUID().toString().replace("-", "_")
    logger.info("test_primary_key_partial_update, label: $label")
    // 1,99
    // 4,88
    sql """
    LOAD LABEL $label (
        DATA INFILE("s3://${getS3BucketName()}/regression/unqiue_with_mow_p0/partial_update/row_s3_1.csv")
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ","
        (k1,c4)
    ) WITH S3 (
        "AWS_ACCESS_KEY" = "${getS3AK()}",
        "AWS_SECRET_KEY" = "${getS3SK()}",
        "AWS_ENDPOINT" = "${getS3Endpoint()}",
        "AWS_REGION" = "${getS3Region()}",
        "provider" = "${getS3Provider()}"
    );
    """
    waitForBrokerLoadDone(label)
    qt_sql "select * from ${tableName} order by k1;"


    label = "test_pu" + UUID.randomUUID().toString().replace("-", "_")
    logger.info("test_primary_key_partial_update, label: $label")
    // 3,333
    // 5,555
    sql """
    LOAD LABEL $label (
        DATA INFILE("s3://${getS3BucketName()}/regression/unqiue_with_mow_p0/partial_update/row_s3_2.csv")
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ","
        (k1,c1)
    ) WITH S3 (
        "AWS_ACCESS_KEY" = "${getS3AK()}",
        "AWS_SECRET_KEY" = "${getS3SK()}",
        "AWS_ENDPOINT" = "${getS3Endpoint()}",
        "AWS_REGION" = "${getS3Region()}",
        "provider" = "${getS3Provider()}"
    ) properties(
        "partial_columns" = "false"
    );
    """
    waitForBrokerLoadDone(label)
    qt_sql "select * from ${tableName} order by k1;"


    label = "test_pu" + UUID.randomUUID().toString().replace("-", "_")
    logger.info("test_primary_key_partial_update, label: $label")
    // 1,123,876
    // 2,345,678
    sql """
    LOAD LABEL $label (
        DATA INFILE("s3://${getS3BucketName()}/regression/unqiue_with_mow_p0/partial_update/pu_s3.csv")
        INTO TABLE ${tableName}
        COLUMNS TERMINATED BY ","
        (k1,c2,c3)
    )
    WITH S3 (
        "AWS_ACCESS_KEY" = "${getS3AK()}",
        "AWS_SECRET_KEY" = "${getS3SK()}",
        "AWS_ENDPOINT" = "${getS3Endpoint()}",
        "AWS_REGION" = "${getS3Region()}",
        "provider" = "${getS3Provider()}"
    ) properties(
        "partial_columns" = "true"
    );
    """
    waitForBrokerLoadDone(label)
    qt_sql "select * from ${tableName} order by k1;"
}