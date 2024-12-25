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

suite('test_ingestion_load', 'p0') {

    def testIngestLoadJob = { testTable, loadLabel, String dataFile ->

        sql "TRUNCATE TABLE ${testTable}"

        sql "CLEAN LABEL FROM ${context.dbName}"

        Integer loadId = -1
        Integer tableId = -1
        Integer partitionId = -1
        Integer indexId = -1
        Integer bucketId = 0
        Integer schemaHash = -1

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
            uri "/api/ingestion_load/${context.dbName}/_create"
            op "post"
            user context.config.feHttpUser
            password context.config.feHttpPassword
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
                    "filePathToSize": "{\\"${etlResultFilePath}\\": 81758}",
                    "hadoopProperties": "{\\"fs.defaultFS\\":\\"${getHdfsFs()}\\",\\"hadoop.username\\":\\"${getHdfsUser()}\\",\\"hadoop.password\\":\\"${getHdfsPasswd()}\\"}"
                }
            }"""

        httpTest {
            endpoint context.config.feHttpAddress
            uri "/api/ingestion_load/${context.dbName}/_update"
            op "post"
            user context.config.feHttpUser
            password context.config.feHttpPassword
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

        max_try_milli_secs = 60000
        while (max_try_milli_secs) {
            result = sql "show load where label = '${loadLabel}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                qt_select "select * from ${testTable} order by c_int"
                break
            } else {
                sleep(5000) // wait 1 second every time
                max_try_milli_secs -= 5000
                if (max_try_milli_secs < 0) {
                    assertEquals(1, 2)
                }
            }
        }

    }

    if (enableHdfs()) {

        tableName = 'tbl_test_spark_load'

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
            DISTRIBUTED BY HASH(c_int) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            )
            """

        def label = "test_ingestion_load"

        testIngestLoadJob.call(tableName, label, context.config.dataPath + '/load_p0/ingestion_load/data.parquet')

    }

}
