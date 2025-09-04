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

suite('test_ingestion_load_multi_table', 'p0') {

    def testIngestLoadJob = { loadLabel, testTable1, testTable2, dataFile1, dataFile2 ->

        sql "TRUNCATE TABLE ${testTable1}"
        sql "TRUNCATE TABLE ${testTable2}"

        sql "CLEAN LABEL FROM ${context.dbName}"

        Integer loadId = -1
        Integer tableId = -1
        Integer partitionId = -1
        Integer indexId = -1
        Integer bucketId = 0
        Integer schemaHash = -1

        String resultFileName1 = ""
        String resultFileName2 = ""

        String reqBody =
                """{
                    "label": "${loadLabel}",
                    "tableToPartition": {
                        "${testTable1}": [],
                        "${testTable2}": []
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
                // table1
                tableId = tableMeta["${testTable1}"].id
                def index1 = tableMeta["${testTable1}"].indexes[0]
                indexId = index1.indexId
                schemaHash = index1.schemaHash
                partitionId = tableMeta["${testTable1}"].partitionInfo.partitions[0].partitionId
                resultFileName1 = "V1.${loadLabel}.${tableId}.${partitionId}.${indexId}.${bucketId}.${schemaHash}.parquet"
                // table2
                tableId = tableMeta["${testTable2}"].id
                def index2 = tableMeta["${testTable2}"].indexes[0]
                indexId = index2.indexId
                schemaHash = index2.schemaHash
                partitionId = tableMeta["${testTable2}"].partitionInfo.partitions[0].partitionId
                resultFileName2 = "V1.${loadLabel}.${tableId}.${partitionId}.${indexId}.${bucketId}.${schemaHash}.parquet"
            }
        }

        logger.info("resultFileName1: " + resultFileName1)
        logger.info("resultFileName2: " + resultFileName2)

        Files.copy(Paths.get(dataFile1),
                Paths.get(context.config.dataPath + "/load_p0/ingestion_load/${resultFileName1}"), StandardCopyOption.REPLACE_EXISTING)
        Files.copy(Paths.get(dataFile2),
                Paths.get(context.config.dataPath + "/load_p0/ingestion_load/${resultFileName2}"), StandardCopyOption.REPLACE_EXISTING)

        String etlResultFilePath1 = uploadToHdfs "/load_p0/ingestion_load/${resultFileName1}"
        String etlResultFilePath2 = uploadToHdfs "/load_p0/ingestion_load/${resultFileName2}"

        String dppResult = '{\\"isSuccess\\":true,\\"failedReason\\":\\"\\",\\"scannedRows\\":10,\\"fileNumber\\":2,' +
                '\\"fileSize\\":163516,\\"normalRows\\":10,\\"abnormalRows\\":0,\\"unselectRows\\":0,' +
                '\\"partialAbnormalRows\\":\\"[]\\",\\"scannedBytes\\":0}'


        String updateStatusReqBody =
                """{
                "loadId": ${loadId},
                "statusInfo": {
                    "status": "SUCCESS",
                    "msg": "",
                    "appId": "",
                    "dppResult": "${dppResult}",
                    "filePathToSize": "{\\"${etlResultFilePath1}\\": 5745, \\"${etlResultFilePath2}\\": 5745}",
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

        max_try_milli_secs = 60000
        while (max_try_milli_secs) {
            result = sql "show load where label = '${loadLabel}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                qt_select "select * from ${testTable1} order by c_int"
                qt_select "select * from ${testTable2} order by c_int"
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

        tableName1 = 'tbl_test_spark_load_multi_1'

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName1} (
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

        tableName2 = 'tbl_test_spark_load_multi_2'

        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
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

        def label = "test_ingestion_load_multi_table"

        testIngestLoadJob.call(label, tableName1, tableName2, context.config.dataPath + '/load_p0/ingestion_load/data.parquet', context.config.dataPath + '/load_p0/ingestion_load/data.parquet')

    }

}