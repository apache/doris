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

import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardCopyOption

suite('test_ingestion_load_label_exists', 'p0') {

    def tableName = 'test_ingestion_load_label_exists'

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                c_int int(11) NULL,
                c_char char(15) NULL,
                c_varchar varchar(100) NULL,
                c_bool boolean NULL,
                c_tinyint tinyint(4) NULL,
                c_smallint smallint(6) NULL,
                c_bigint bigint(20) NULL,
                c_largeint largeint(40) NULL,
                c_float float NULL,
                c_double double NULL,
                c_decimal decimal(6, 3) NULL,
                c_decimalv3 decimal(6, 3) NULL,
                c_date date NULL,
                c_datev2 date NULL,
                c_datetime datetime NULL,
                c_datetimev2 datetime NULL,
                INDEX idx_c_varchar(c_varchar) USING INVERTED,
                INDEX idx_c_bigint(c_bigint) USING INVERTED,
                INDEX idx_c_datetimev2(c_datetimev2) USING INVERTED
            )
            DUPLICATE KEY(c_int)
            DISTRIBUTED BY HASH(c_int) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            )
            """

        def label = "test_ingestion_load_label_exists"

        sql "TRUNCATE TABLE ${tableName}"

        sql "CLEAN LABEL FROM ${context.dbName}"

        long loadId = -1

        String reqBody =
                """{
                    "label": "${label}",
                    "tableToPartition": {
                        "${tableName}": []
                    },
                    "properties": {}
                }"""

        httpTest {
            endpoint context.config.feHttpAddress
            uri "/api/ingestion_load/internal/${context.dbName}/_create"
            op "post"
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            body reqBody
            check { code, resBody ->
                assert code == 200
                def resBodyJson = parseJson(resBody)
                assert resBodyJson instanceof Map
                assert resBodyJson.code == 0
                def data = resBodyJson.data
                loadId = data.loadId
            }
        }

        logger.info("loadId:" + loadId)

        try {
            httpTest {
                endpoint context.config.feHttpAddress
                uri "/api/ingestion_load/internal/${context.dbName}/_create"
                op "post"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body reqBody
                check { code, resBody ->
                    assert code == 200
                    def resBodyJson = parseJson(resBody)
                    assert resBodyJson instanceof Map
                    assert resBodyJson.code == 1
                    assert (resBodyJson.data =~ /has already been used/).find()
                }
            }
        } finally {
            String updateStatusReqBody =
                    """{
                    "loadId": ${loadId},
                    "statusInfo": {
                        "status": "FAILED",
                        "msg": "load client cancelled.",
                        "appId": ""
                    }
                }"""

            httpTest {
                endpoint context.config.feHttpAddress
                uri "/api/ingestion_load/internal/${context.dbName}/_update"
                op "post"
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
                body updateStatusReqBody
            }
        }


}
