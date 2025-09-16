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

suite('test_ingestion_load_drop_table', 'p0,external') {

    def testIngestLoadJob = { testTable, loadLabel, dataFile, alterAction ->

        sql "TRUNCATE TABLE ${testTable}"

        sql "CLEAN LABEL FROM ${context.dbName}"

        long loadId = -1
        long tableId = -1
        long partitionId = -1
        long indexId = -1
        long bucketId = 0
        long schemaHash = -1

        String reqBody =
                """{
                    "label": "${loadLabel}",
                    "tableToPartition": {
                        "${testTable}": []
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
                def tableMeta = data.tableMeta
                tableId = tableMeta["${testTable}"].id
                def index = tableMeta["${testTable}"].indexes[0]
                indexId = index.indexId
                schemaHash = index.schemaHash
                partitionId = tableMeta["${testTable}"].partitionInfo.partitions[0].partitionId
            }
        }

        String resultFileName = "V1.${loadLabel}.${tableId}.${partitionId}.${indexId}.${bucketId}.${schemaHash}.parquet"
        logger.info("resultFileName: " + resultFileName)

        Files.copy(Paths.get(dataFile),
                Paths.get(context.config.dataPath + "/load_p0/ingestion_load/${resultFileName}"), StandardCopyOption.REPLACE_EXISTING)

        String etlResultFilePath = uploadToHdfs "/load_p0/ingestion_load/${resultFileName}"

        String dppResult = '{\\"isSuccess\\":true,\\"failedReason\\":\\"\\",\\"scannedRows\\":10,\\"fileNumber\\":1,' +
                '\\"fileSize\\":2441,\\"normalRows\\":10,\\"abnormalRows\\":0,\\"unselectRows\\":0,' +
                '\\"partialAbnormalRows\\":\\"[]\\",\\"scannedBytes\\":0}'

        String updateStatusReqBody =
                """{
                "loadId": ${loadId},
                "statusInfo": {
                    "status": "SUCCESS",
                    "msg": "",
                    "appId": "",
                    "dppResult": "${dppResult}",
                    "filePathToSize": "{\\"${etlResultFilePath}\\": 5745}",
                    "hadoopProperties": "{\\"fs.defaultFS\\":\\"${getHdfsFs()}\\",\\"hadoop.username\\":\\"${getHdfsUser()}\\",\\"hadoop.password\\":\\"${getHdfsPasswd()}\\"}"
                }
            }"""

        httpTest {
            endpoint context.config.feHttpAddress
            uri "/api/ingestion_load/internal/${context.dbName}/_update"
            op "post"
            basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            body updateStatusReqBody
            check { code, resBody ->
                {
                    assert code == 200
                    def resBodyJson = parseJson(resBody)
                    assert resBodyJson instanceof Map
                    assert resBodyJson.code == 0
                }
            }
        }

        alterAction.call()

        def max_try_milli_secs = 120000
        while (max_try_milli_secs) {
            def result = sql "show load where label = '${loadLabel}'"
            if (result.size() == 0) {
                break
            } else {
                sleep(5000) // wait 1 second every time
                max_try_milli_secs -= 5000
                if (max_try_milli_secs <= 0) {
                    assertEquals(1, 2)
                }
            }
        }

    }

    if (enableHdfs()) {

        tableName = 'tbl_test_spark_load_drop_table'

        try {

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
                    c_datetimev2 datetime NULL
                )
                DUPLICATE KEY(c_int)
                DISTRIBUTED BY HASH(c_int) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
                """

            label = "test_ingestion_load_drop_table"

            testIngestLoadJob.call(tableName, label, context.config.dataPath + '/load_p0/ingestion_load/data.parquet', {
                sql "DROP TABLE ${tableName}"
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
                    c_datetimev2 datetime NULL
                )
                DUPLICATE KEY(c_int)
                DISTRIBUTED BY HASH(c_int) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
                """
            })

        } finally {
        }

    }

}