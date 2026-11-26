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

import com.google.common.base.Strings
import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_export_iam") {
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def cases = config.authCases.collect { authCase ->
        return [
                name: authCase.name,
                properties: """
                    ${authCase.storageSqlProperties}
                """
        ]
    }

    def tableName = "test_export_iam"
    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    sql """
        CREATE TABLE ${tableName} (
            siteid INT DEFAULT '10',
            citycode SMALLINT NOT NULL,
            username VARCHAR(32) DEFAULT '',
            pv BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(siteid, citycode, username)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """INSERT INTO ${tableName}(siteid, citycode, username, pv) VALUES
            (1, 1, "xxx", 1), (2, 2, "yyy", 2), (3, 3, "zzz", 3)"""

    cases.each { testCase ->
        def randomStr = UUID.randomUUID().toString().replace("-", "")
        def label = "iam_export_${testCase.name}_${randomStr}"
        // Keep cleanup inside this run's UUID directory.
        def exportPrefix = "${config.prefix}/test_export_iam/${testCase.name}/${randomStr}/"

        logger.info("run ${name} with ${testCase.name}")
        sql """
            EXPORT TABLE ${tableName} TO "${config.scheme}://${config.bucket}/${exportPrefix}"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator" = ",",
                "delete_existing_files" = "true"
            )
            WITH S3 (
                ${testCase.properties}
            )
        """

        def maxTryMs = 600000
        def exportedUrl = ""
        while (maxTryMs > 0) {
            String[][] exportResult = sql "SHOW EXPORT WHERE LABEL = '${label}'"
            if (exportResult[0][2] == "FINISHED") {
                def json = parseJson(exportResult[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0], testCase.name)
                exportedUrl = json.url[0][0]
                break
            }
            if (exportResult[0][2] == "CANCELLED") {
                assertTrue(false, "Export ${label} cancelled for ${testCase.name}: ${exportResult}")
            }
            Thread.sleep(5000)
            maxTryMs -= 5000
        }
        assertTrue(maxTryMs > 0, "Export ${label} timeout for ${testCase.name}")
        assertFalse(Strings.isNullOrEmpty(exportedUrl), "Export returned no URL for ${testCase.name}")

        def s3Prefix = "s3://${config.bucket}/"
        assertTrue(exportedUrl.startsWith(s3Prefix), "Unexpected export URL: ${exportedUrl}")
        def exportedFile = "${config.scheme}://${config.bucket}/" +
                "${exportedUrl.substring(s3Prefix.length())}0.csv"
        def result = sql """
            SELECT COUNT(*) FROM s3(
                "uri" = "${exportedFile}",
                ${testCase.properties},
                "format" = "csv",
                "column_separator" = ",",
                "use_path_style" = "false"
            )
        """
        assertEquals(3, result[0][0], testCase.name)
    }
}
