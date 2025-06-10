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

import com.google.common.base.Strings;

suite("test_export_with_role") {
    if (Strings.isNullOrEmpty(context.config.awsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }


    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def label = "label_" + randomStr
    def tableName = "test_export_with_role"

    def endpoint = context.config.awsEndpoint
    def region = context.config.awsRegion
    def bucket = context.config.awsBucket
    def roleArn = context.config.awsRoleArn
    def externalId = context.config.awsExternalId
    
    def prefix = context.config.awsPrefix

    sql """
        DROP TABLE IF EXISTS ${tableName} FORCE;
        """

    sql """
        CREATE TABLE ${tableName}
        (
            siteid INT DEFAULT '10',
            citycode SMALLINT NOT NULL,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
        """

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (2, 2, "yyy", 2),
            (3, 3, "zzz", 3)
        """

    sql """
        EXPORT TABLE ${tableName} TO "s3://${bucket}/${prefix}/aws_iam_role_p0/test_export_with_role"
        PROPERTIES(
            "label" = "${label}",
            "format" = "csv",
            "column_separator"=",",
            "delete_existing_files"= "true"
        )
        WITH s3 (
            "s3.endpoint" = "${endpoint}",
            "s3.region" = "${region}",
            "s3.role_arn" = "${roleArn}",
            "s3.external_id"="${externalId}",
            "provider" = "AWS"
        );
    """

    def maxTryMs = 600000
    def outfileUrl = ""
    while (maxTryMs > 0) {
        String[][] result = sql """ show export where label = "${label}" """
        logger.info("result: ${result}")
        if (result[0][2].equals("FINISHED")) {
            def json = parseJson(result[0][11])
            logger.info("json: ${json}")
            assert json instanceof List
            assertEquals("1", json.fileNumber[0][0])
            log.info("outfileUrl: ${json.url[0][0]}")
            outfileUrl = json.url[0][0]
            break;
        }
        if (result[0][2].equals("CANCELLED")) {
            assertTrue(false, "Export ${label} cancelled: ${result}")
            break
        }
        Thread.sleep(5000)
        maxTryMs -= 5000
        if (maxTryMs <= 0) {
            assertTrue(false, "Export ${label} timeout")
        }
    }

    def result = sql """
        select count(*) from s3("uri" = "s3://${bucket}/${outfileUrl.substring(5 + bucket.length())}0.csv",
                    "s3.endpoint" = "${endpoint}",
                    "s3.region" = "${region}",
                    "s3.role_arn"= "${roleArn}",
                    "s3.external_id" = "${externalId}",
                    "format" = "csv",
                    "column_separator" = ",",
                    "use_path_style" = "false");
        """
    log.info("result: ${result}")
    assertEquals(3, result[0][0])
}