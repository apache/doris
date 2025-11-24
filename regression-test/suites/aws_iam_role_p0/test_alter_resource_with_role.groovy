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
import groovy.json.JsonSlurper

suite("test_alter_resource_with_role") {
    if (Strings.isNullOrEmpty(context.config.awsRoleArn)) {
        logger.info("skip ${name} case, because awsRoleArn is null or empty")
        return
    }


    if (isCloudMode()) {
        logger.info("skip ${name} case, because it is cloud mode")
        return
    }

    def tableName = "test_alter_resource_with_role"
    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def resourceName = "alter_resource_${randomStr}"
    def policyName = "alter_policy_${randomStr}"

    def awsEndpoint = context.config.awsEndpoint
    def region = context.config.awsRegion
    def bucket = context.config.awsBucket
    def roleArn = context.config.awsRoleArn
    def externalId = context.config.awsExternalId
    def prefix = context.config.awsPrefix

    def awsAccessKey = context.config.awsAccessKey
    def awsSecretKey = context.config.awsSecretKey

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resourceName}"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "${awsEndpoint}",
            "AWS_REGION" = "${region}",
            "AWS_BUCKET" = "${bucket}",
            "AWS_ROOT_PATH" = "${prefix}/aws_iam_role_p0/test_alter_resource_with_role/${randomStr}",
            "AWS_ACCESS_KEY" = "error_ak",
            "AWS_SECRET_KEY" = "error_sk",
            "s3_validity_check" = "false"
        );
    """

    sql """
        CREATE STORAGE POLICY IF NOT EXISTS ${policyName}
        PROPERTIES(
            "storage_resource" = "${resourceName}",
            "cooldown_ttl" = "1"
        )
    """

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
            "replication_num" = "1",
            "storage_policy" = "${policyName}"
        )
        """

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (2, 2, "yyy", 2),
            (3, 3, "zzz", 3)
        """

    def result = sql """ SHOW RESOURCES WHERE NAME = "${resourceName}"; """
    log.info("result:${result}")
    assertTrue(!result.toString().contains(roleArn))
    assertTrue(!result.toString().contains(externalId));
    assertTrue(result.toString().contains("error_ak"));

    // data_sizes is one arrayList<Long>, t is tablet
    def fetchDataSize = {List<Long> data_sizes, Map<String, Object> t ->
        def tabletId = t.TabletId
        def meta_url = t.MetaUrl
        def clos = {  respCode, body ->
            logger.info("test ttl expired resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
            logger.info("test fetchBeHttp, resp body: ${out}")
            def obj = new JsonSlurper().parseText(out)
            data_sizes[0] = obj.local_data_size
            data_sizes[1] = obj.remote_data_size
        }
        meta_url = meta_url.replace("header", "data_size")

        def i = meta_url.indexOf("/api")
        def endPoint = meta_url.substring(0, i)
        def metaUri = meta_url.substring(i)
        logger.info("test fetchBeHttp, endpoint:${endPoint}, metaUri:${metaUri}")
        i = endPoint.lastIndexOf('/')
        endPoint = endPoint.substring(i + 1)

        httpTest {
            endpoint {endPoint}
            uri metaUri
            op "get"
            check clos
        }
    }

    List<Long> sizes = [-1, -1]
    def tablets = sql_return_maparray """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty:${tablets}")
    fetchDataSize(sizes, tablets[0])
    def retry = 12
    while (sizes[1] == 0 && retry-- > 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql_return_maparray """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    assertTrue(sizes[1] == 0, "remote size expected is zero, but got ${sizes[1]}")
    assertTrue(tablets.size() > 0)

    // alter resource to arn role
    sql """ truncate table ${tableName} """
    sql """ALTER RESOURCE "${resourceName}"
            PROPERTIES(
                "AWS_ROLE_ARN" = "${roleArn}",
                "AWS_EXTERNAL_ID" = "${externalId}"
            );"""

    result = sql """ SHOW RESOURCES WHERE NAME = "${resourceName}"; """
    log.info("result:${result}")
    assertTrue(result.toString().contains(roleArn))
    assertTrue(result.toString().contains(externalId));
    assertTrue(!result.toString().contains("error_ak"));

    // report_tablet_interval_seconds = 60 sleep wait for report policy
    sleep(120 * 1000)

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (4, 4, "yyy", 5),
            (5, 5, "zzz", 5),
            (6, 6, "xxx", 6)
        """

    sizes = [-1, -1]
    tablets = sql_return_maparray """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty:${tablets}")
    fetchDataSize(sizes, tablets[0])
    retry = 100
    while (sizes[1] == 0 && retry-- > 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql_return_maparray """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    assertTrue(sizes[1] != 0, "remote size is still zero, maybe some error occurred")
    assertTrue(tablets.size() > 0)
    log.info( "test remote size not zero")

    // alter resource to correct ak/sk
    sql """ truncate table ${tableName} """
    sql """ALTER RESOURCE "${resourceName}"
            PROPERTIES(
                "AWS_ACCESS_KEY" = "${awsAccessKey}",
                "AWS_SECRET_KEY" = "${awsSecretKey}"
            );"""

    result = sql """ SHOW RESOURCES WHERE NAME = "${resourceName}"; """
    log.info("result:${result}")
    assertTrue(!result.toString().contains(roleArn))
    assertTrue(!result.toString().contains(externalId));
    assertTrue(result.toString().contains(awsAccessKey));

    // report_tablet_interval_seconds = 60 sleep wait for report policy
    sleep(120 * 1000)

    sql """insert into ${tableName}(siteid, citycode, username, pv) values (1, 1, "xxx", 1),
            (4, 4, "yyy", 5),
            (5, 5, "zzz", 5),
            (6, 6, "xxx", 6),
            (7, 7, "aaa", 8)
        """

    sizes = [-1, -1]
    tablets = sql_return_maparray """
    SHOW TABLETS FROM ${tableName}
    """
    log.info( "test tablets not empty:${tablets}")
    fetchDataSize(sizes, tablets[0])
    retry = 100
    while (sizes[1] == 0 && retry-- > 0) {
        log.info( "test remote size is zero, sleep 10s")
        sleep(10000)
        tablets = sql_return_maparray """
        SHOW TABLETS FROM ${tableName}
        """
        fetchDataSize(sizes, tablets[0])
    }
    assertTrue(sizes[1] != 0, "remote size is still zero, maybe some error occurred")
    assertTrue(tablets.size() > 0)
    log.info( "test remote size not zero")
}