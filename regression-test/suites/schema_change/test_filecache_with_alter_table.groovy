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

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.doris.regression.suite.ClusterOptions
import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.OutputUtils

@groovy.transform.Immutable
class RowsetInfo {
    int startVersion
    int endVersion
    String id
    String originalString
}

suite("test_filecache_with_alter_table", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    options.beConfigs.add('enable_flush_file_cache_async=false')
    options.beConfigs.add('file_cache_enter_disk_resource_limit_mode_percent=99')
    options.beConfigs.add('enable_evict_file_cache_in_advance=false')
    options.beConfigs.add('file_cache_path=[{"path":"/opt/apache-doris/be/storage/file_cache","total_size":83886080,"query_limit":83886080}]')

    def baseTestTable = "test_filecache_with_alter_table"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    def csvPathPrefix = "/tmp/temp_csv_data"
    def loadBatchNum = 20

    def generateCsvData = {
        def rowsPerFile = 32768
        def columnsPerRow = 4
        def headers = 'col1,col2,col3,col4'

        def dir = new File(csvPathPrefix)
        if (!dir.exists()) {
            dir.mkdirs()
        } else {
            dir.eachFile { it.delete() }
        }

        long currentNumber = 1L
        (1..loadBatchNum).each { fileIndex ->
            def fileName = String.format("${csvPathPrefix}/data_%02d.csv", fileIndex)
            def csvFile = new File(fileName)

            csvFile.withWriter('UTF-8') { writer ->
                writer.writeLine(headers)
                (1..rowsPerFile).each { rowIndex ->
                    def row = (1..columnsPerRow).collect { currentNumber++ }
                    writer.writeLine(row.join(','))
                }
            }
        }
        logger.info("Successfully generated ${loadBatchNum} CSV files in ${csvPathPrefix}")
    }

    def getTabletStatus = { tablet ->
        String tabletId = tablet.TabletId
        String backendId = tablet.BackendId
        def beHost = backendId_to_backendIP[backendId]
        def beHttpPort = backendId_to_backendHttpPort[backendId]
        
        String command = "curl -s -X GET http://${beHost}:${beHttpPort}/api/compaction/show?tablet_id=${tabletId}"

        logger.info("Executing: ${command}")
        def process = command.execute()
        def exitCode = process.waitFor()
        def output = process.getText()
        
        logger.info("Get tablet status response: code=${exitCode}, out=${output}")
        assertEquals(0, exitCode, "Failed to get tablet status.")
        
        return parseJson(output.trim())
    }

    def waitForAlterJobToFinish = { tableName, timeoutMillis ->
        def pollInterval = 1000
        def timeElapsed = 0
        while (timeElapsed <= timeoutMillis) {
            def alterResult = sql_return_maparray """SHOW ALTER TABLE COLUMN WHERE TableName = "${tableName}" ORDER BY CreateTime DESC LIMIT 1;"""
            logger.info("Checking ALTER status for table '${tableName}': ${alterResult}")
            if (alterResult && alterResult[0].State == "FINISHED") {
                sleep(3000)
                logger.info("ALTER job on table '${tableName}' finished. Details: ${alterResult[0]}")
                return
            }
            sleep(pollInterval)
            timeElapsed += pollInterval
        }
        fail("Wait for ALTER job on table '${tableName}' to finish timed out after ${timeoutMillis}ms.")
    }

    def runSchemaChangeCacheTest = { String testTable, double inputCacheRatio, boolean expectOutputCached ->
        logger.info("==================================================================================")
        logger.info("Running Test Case on Table '${testTable}': Input Cache Ratio = ${inputCacheRatio * 100}%, Expect Output Cached = ${expectOutputCached}")
        logger.info("==================================================================================")

        sql """ DROP TABLE IF EXISTS ${testTable} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                col1 bigint,
                col2 bigint,
                col3 bigint,
                col4 bigint
            )
            UNIQUE KEY(col1)
            DISTRIBUTED BY HASH(col1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """
        
        (1..loadBatchNum).each { fileIndex ->
            def fileName = String.format("${csvPathPrefix}/data_%02d.csv", fileIndex)
            streamLoad {
                logger.info("Stream loading file index ${fileIndex} into table ${testTable}")
                set "column_separator", ","
                table testTable
                file fileName
                time 3000
                check { res, exception, startTime, endTime ->
                    if (exception != null) throw exception
                    def json = parseJson(res)
                    assertEquals("success", json.Status.toLowerCase())
                }
            }
        }
        sql """ SELECT COUNT(col1) from ${testTable} """

        def tablets = sql_return_maparray "show tablets from ${testTable};"
        assertEquals(1, tablets.size(), "Expected to find exactly one tablet.")
        def tablet = tablets[0]
        def beHost = backendId_to_backendIP[tablet.BackendId]
        def beHttpPort = backendId_to_backendHttpPort[tablet.BackendId]

        def tabletStatus = getTabletStatus(tablet)
        List<RowsetInfo> originalRowsetInfos = tabletStatus["rowsets"].collect { rowsetStr ->
            def parts = rowsetStr.split(" ")
            def versionParts = parts[0].replace('[', '').replace(']', '').split("-")
            new RowsetInfo(
                startVersion: versionParts[0].toInteger(),
                endVersion: versionParts[1].toInteger(),
                id: parts[4],
                originalString: rowsetStr
            )
        }.findAll { it.startVersion != 0 }.sort { it.startVersion }

        int numToClear = Math.round(originalRowsetInfos.size() * (1 - inputCacheRatio)).toInteger()
        logger.info("Total data rowsets: ${originalRowsetInfos.size()}. Clearing cache for ${numToClear} rowsets to achieve ~${inputCacheRatio * 100}% hit ratio.")

        originalRowsetInfos.take(numToClear).each { rowset ->
            Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=clear&sync=true&value=${rowset.id}_0.dat", true)
        }

        def cachedInputRowsets = originalRowsetInfos.findAll { rowset ->
            def data = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=list_cache&value=${rowset.id}_0.dat", true)
            data.any { item -> !item.endsWith("_idx") && !item.endsWith("_disposable") }
        }
        
        def actualCachedRatio = cachedInputRowsets.size() / (double)originalRowsetInfos.size()
        logger.info("Verification: Cached input rowsets: ${cachedInputRowsets.size()}. Actual cache ratio: ${actualCachedRatio * 100}%")
        assertTrue(Math.abs(inputCacheRatio - actualCachedRatio) < 0.01, "Actual cache ratio does not match expected ratio.")

        logger.info("Triggering ALTER TABLE on ${testTable}")
        sql """ALTER TABLE ${testTable} MODIFY COLUMN col2 VARCHAR(255)"""
        waitForAlterJobToFinish(testTable, 60000)

        tablets = sql_return_maparray "show tablets from ${testTable};"
        tablet = tablets[0]
        tabletStatus = getTabletStatus(tablet)

        def newRowsetInfos = tabletStatus["rowsets"].collect { rowsetStr ->
            def parts = rowsetStr.split(" ")
            def version_pair = parts[0].replace('[', '').replace(']', '').split('-')
            new RowsetInfo(
                startVersion: version_pair[0].toInteger(),
                endVersion: version_pair[1].toInteger(),
                id: parts[4],
                originalString: rowsetStr
            )
        }.findAll { it.startVersion != 0 }.sort { it.startVersion }

        def cachedOutputRowsets = newRowsetInfos.findAll { rowset ->
            def data = Http.GET("http://${beHost}:${beHttpPort}/api/file_cache?op=list_cache&value=${rowset.id}_0.dat", true)
            data.any { item -> !item.endsWith("_idx") && !item.endsWith("_disposable") }
        }

        logger.info("After ALTER, found ${cachedOutputRowsets.size()} cached output rowsets out of ${newRowsetInfos.size()}.")

        if (expectOutputCached) {
            assertTrue(cachedOutputRowsets.size() > 0, "Expected output rowsets to be cached, but none were found.")
        } else {
            assertEquals(0, cachedOutputRowsets.size(), "Expected output rowsets NOT to be cached, but some were found.")
        }
        logger.info("Test Case Passed: Input Ratio ${inputCacheRatio * 100}%, Output Cached Check: ${expectOutputCached}")
        
        sql """ DROP TABLE IF EXISTS ${testTable} force;"""
    }
    
    docker(options) {
        getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort);
        
        sql """ set global enable_auto_analyze = false;"""
        
        generateCsvData()

        runSchemaChangeCacheTest("${baseTestTable}_0", 0.0, false)
        runSchemaChangeCacheTest("${baseTestTable}_65", 0.65, false)
        runSchemaChangeCacheTest("${baseTestTable}_75", 0.75, true)
        runSchemaChangeCacheTest("${baseTestTable}_100", 1.0, true)
    }
}