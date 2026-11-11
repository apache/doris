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

import groovy.json.JsonSlurper
import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_resource_iam") {
    if (isCloudMode()) {
        logger.info("skip ${name} case, because cloud mode not support")
        return
    }
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def randomStr = UUID.randomUUID().toString().replace("-", "")
    def testObjects = []
    config.authCases.eachWithIndex { authCase, index ->
        def tableName = "test_resource_iam_${index}"
        def resourceName = "resource_${authCase.name}_${randomStr}"
        def policyName = "policy_${authCase.name}_${randomStr}"

        logger.info("run ${name} with ${authCase.name}")
        sql """
            CREATE RESOURCE IF NOT EXISTS "${resourceName}"
            PROPERTIES(
                "type" = "s3",
                ${authCase.storageSqlProperties},
                "s3.bucket" = "${config.bucket}",
                "s3.root.path" = "${config.prefix}/test_resource_iam/${authCase.name}/${randomStr}",
                "s3.connection.maximum" = "50",
                "s3.connection.request.timeout" = "3000",
                "s3.connection.timeout" = "1000",
                "s3_validity_check" = "true"
            );
        """

        sql """
            CREATE STORAGE POLICY IF NOT EXISTS ${policyName}
            PROPERTIES(
                "storage_resource" = "${resourceName}",
                "cooldown_ttl" = "1"
            )
        """

        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
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
        testObjects.add([name: authCase.name, tableName: tableName])
    }

    // data_sizes is one arrayList<Long>, t is tablet
    def fetchDataSize = {List<Long> data_sizes, Map<String, Object> t ->
        def tabletId = t.TabletId
        def meta_url = t.MetaUrl
        def clos = {  respCode, body ->
            logger.info("test ttl expired resp Code {}", "${respCode}".toString())
            assertEquals("${respCode}".toString(), "200")
            String out = "${body}".toString()
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

    sleep(60000)

    testObjects.each { testObject ->
        List<Long> sizes = [-1, -1]
        def tablets = sql_return_maparray "SHOW TABLETS FROM ${testObject.tableName}"
        log.info("test tablets not empty for ${testObject.name}: ${tablets}")
        fetchDataSize(sizes, tablets[0])
        def retry = 100
        while (sizes[1] == 0 && retry-- > 0) {
            log.info("test remote size is zero for ${testObject.name}, sleep 10s")
            sleep(10000)
            tablets = sql_return_maparray "SHOW TABLETS FROM ${testObject.tableName}"
            fetchDataSize(sizes, tablets[0])
        }
        assertTrue(sizes[1] != 0,
                "remote size is still zero for ${testObject.name}, maybe some error occurred")
        assertTrue(tablets.size() > 0)
        log.info("test remote size not zero for ${testObject.name}")
    }
}
