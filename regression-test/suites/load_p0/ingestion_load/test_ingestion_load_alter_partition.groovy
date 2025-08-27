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

suite('test_ingestion_load_alter_partition', 'p0,external') {

    def testIngestLoadJob = { testTable, loadLabel, dataFiles, alterAction ->

        sql "TRUNCATE TABLE ${testTable}"

        sql "CLEAN LABEL ${loadLabel} FROM ${context.dbName}"

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

        def resultFileNames = []    

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
                def partitions = tableMeta["${testTable}"].partitionInfo.partitions
                for(partition in partitions) {
                    logger.info("partitionId: " + partition.partitionId)
                    resultFileNames.add("V1.${loadLabel}.${tableId}.${partition.partitionId}.${indexId}.${bucketId}.${schemaHash}.parquet")
                }
            }
        }

        def etlResultFilePaths = []
        for(int i=0; i < dataFiles.size(); i++) {
            Files.copy(Paths.get(dataFiles[i]),
                Paths.get(context.config.dataPath + "/load_p0/ingestion_load/${resultFileNames[i]}"), StandardCopyOption.REPLACE_EXISTING)
            String etlResultFilePath = uploadToHdfs "/load_p0/ingestion_load/${resultFileNames[i]}"
            logger.info("etlResultFilePath: " + etlResultFilePath)
            etlResultFilePaths.add(etlResultFilePath)
        }

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
                    "filePathToSize": "{\\"${etlResultFilePaths.get(0)}\\":851,\\"${etlResultFilePaths.get(1)}\\":781,\\"${etlResultFilePaths.get(2)}\\":781,\\"${etlResultFilePaths.get(3)}\\":839}",
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
            result = sql "show load where label = '${loadLabel}'"
            if (result[0][2] == "FINISHED") {
                sql "sync"
                qt_select "select c1, count(*) from ${testTable} group by c1 order by c1"
                break
            } else if (result[0][2] == "CANCELLED") {
                msg = result[0][7]
                logger.info("err msg: " + msg)
                assertTrue((result[0][7] =~ /partition does not exist/).find())
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

        def tableName1 = 'tbl_test_spark_load_alter_partition_1'
        def tableName2 = 'tbl_test_spark_load_alter_partition_2'
        def tableName3 = 'tbl_test_spark_load_alter_partition_3'

        try {

            sql """
                CREATE TABLE IF NOT EXISTS ${tableName1} (
                    c0 int not null,
                    c1 date,
                    c2 varchar(64)
                )
                DUPLICATE KEY(c0)
                PARTITION BY RANGE(c1) (
                    FROM ("2024-09-01") TO ("2024-09-05") INTERVAL 1 DAY
                )
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
                """

            def label = UUID.randomUUID().toString().replaceAll("-", "")

            testIngestLoadJob.call(tableName1, label, [context.config.dataPath + '/load_p0/ingestion_load/data2-0.parquet', context.config.dataPath + '/load_p0/ingestion_load/data2-1.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-2.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-3.parquet'], {
                sql "alter table ${tableName1} drop partition p_20240901"
            })

            sql """
                CREATE TABLE IF NOT EXISTS ${tableName2} (
                    c0 int not null,
                    c1 date,
                    c2 varchar(64)
                )
                DUPLICATE KEY(c0)
                PARTITION BY RANGE(c1) (
                    FROM ("2024-09-01") TO ("2024-09-05") INTERVAL 1 DAY
                )
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
                """

            label = UUID.randomUUID().toString().replaceAll("-", "")

            testIngestLoadJob.call(tableName2, label, [context.config.dataPath + '/load_p0/ingestion_load/data2-0.parquet', context.config.dataPath + '/load_p0/ingestion_load/data2-1.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-2.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-3.parquet'], {
                sql "alter table ${tableName2} add partition p_20240905 VALUES [('2024-09-05'), ('2024-09-06'))"
            })

            sql """
                CREATE TABLE IF NOT EXISTS ${tableName3} (
                    c0 int not null,
                    c1 date,
                    c2 varchar(64)
                )
                DUPLICATE KEY(c0)
                PARTITION BY RANGE(c1) (
                    FROM ("2024-09-01") TO ("2024-09-05") INTERVAL 1 DAY
                )
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES (
                "replication_num" = "1"
                )
                """

            label = UUID.randomUUID().toString().replaceAll("-", "")

            testIngestLoadJob.call(tableName3, label, [context.config.dataPath + '/load_p0/ingestion_load/data2-0.parquet', context.config.dataPath + '/load_p0/ingestion_load/data2-1.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-2.parquet',context.config.dataPath + '/load_p0/ingestion_load/data2-3.parquet'], {
                sql "alter table ${tableName3} add temporary partition tp_20240901 VALUES [('2024-09-01'), ('2024-09-02'))"
                sql "alter table ${tableName3} replace partition(p_20240901) with temporary partition(tp_20240901)"
            })

        } finally {
//            sql "DROP TABLE ${tableName1}"
//            sql "DROP TABLE ${tableName2}"
//            sql "DROP TABLE ${tableName3}"
        }

    }

}
