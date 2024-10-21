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

import org.junit.Assert

suite("test_mtmv_outfile","mtmv") {

    String dbName = context.config.getDbNameByFile(context.file)
    String suiteName = "test_mtmv_outfile"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""

    sql """
        CREATE TABLE ${tableName}
        (
            k2 INT,
            k3 varchar(32)
        )
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        );
        """

    sql """
        insert into ${tableName} values (1,1),(1,2);
        """

    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        BUILD DEFERRED REFRESH AUTO ON MANUAL
        DISTRIBUTED BY hash(k2) BUCKETS 2
        PROPERTIES (
        'replication_num' = '1'
        )
        AS
        SELECT * from ${tableName};
        """
    sql """
        REFRESH MATERIALIZED VIEW ${mvName} AUTO;
    """

    def jobName = getJobName(dbName, mvName)
    waitingMTMVTaskFinished(jobName)

    // use to outfile to s3
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def outfile_to_S3_directly = {
        // select ... into outfile ...
        def s3_outfile_path = "${bucket}/outfile/csv/test-mtmv-outfile"
        def uri = "s3://${s3_outfile_path}/exp_"

        def res = sql """
            SELECT * FROM ${mvName} t 
            INTO OUTFILE "${uri}"
            FORMAT AS csv
            PROPERTIES (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}"
            );
        """
        logger.info("outfile to s3 success path: " + res[0][3]);
    }

    outfile_to_S3_directly()

    sql """drop table if exists `${tableName}`"""
    sql """drop materialized view if exists ${mvName};"""
}
