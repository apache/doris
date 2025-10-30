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

suite("test_filecache_with_alter_table", "docker") = {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    options.beConfigs.add('enable_flush_file_cache_async=false')
    options.beConfigs.add('file_cache_enter_disk_resource_limit_mode_percent=99')
    options.beConfigs.add('enable_evict_file_cache_in_advance=false')
    options.beConfigs.add('file_cache_path=[{"path":"/opt/apache-doris/be/storage/file_cache","total_size":83886080,"query_limit":83886080}]')

    def testTable = "test_filecache_with_alter_table"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]
    
    def getTabletStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
    }

    docker(options) {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def url = "jdbc:mysql://${fes[0].Host}:${fes[0].QueryPort}/"
        logger.info("url: " + url)

        sql """ set global enable_auto_analyze = false;"""
        def result = sql 'SELECT DATABASE()'

        sql """ DROP TABLE IF EXISTS ${testTable} force;"""
        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                col1 bigint,
                col2 bigint,
                col3 bigint,
                col4 bigint,
            )
            PAIMARY KEY(col1)
            DISTRIBUTED BY HASH(col1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort);

        def csv_path_prefix = "/tmp/temp_csv_data"
        def load_batch_num = 20
        def rowsPerFile = 32768
        def columnsPerRow = 4
        def headers = 'col1,col2,col3,col4'

        def dir = new File(csv_path_prefix)
        if (!dir.exists()) {
            dir.mkdirs()
        }

        long currentNumber = 1L
        (1..load_batch_num).each { fileIndex ->
            def fileName = String.format("${csv_path_prefix}/data_%02d.csv", fileIndex)
            def csvFile = new File(fileName)

            csvFile.withWriter('UTF-8') { writer ->
                writer.writeLine(headers)
                (1..rowsPerFile).each { rowIndex ->
                    def row = []
                    (1..columnsPerRow).each { columnIndex ->
                        row.add(currentNumber)
                        currentNumber++
                    }
                    writer.writeLine(row.join(','))
                }
            }
        }

        (1..load_batch_num).each { fileIndex ->
            def fileName = String.format("${csv_path_prefix}/data_%02d.csv", fileIndex)
            streamLoad {
                table testTable
                set 'column_separator', '|'
                set 'compress_type', 'GZ'

                file fileName

                time 3000 // limit inflight 3s
                check { res, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${res}".toString())
                    def json = parseJson(res)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
 
        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())

        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")
        
        Set<String> rowsets = new HashSet<>();
        tablet_status = getTabletStatus(tablet)
        rowsets.addAll(tablet_status["rowsets"])
        assertEquals(load_batch_num, rowsets.size())

        def case1_cached_data_ratio = 0.65
        for (int i = 0; i < rowsets.size() * (1 - case1_cached_data_ratio); i++) {
            def rowsetStr = rowsets[i]
            def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
            def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
            def rowset_id = rowsetStr.split(" ")[4]
            if (start_version == 0) {
                continue
            }

            result = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=clear&sync=true&value=${rowset_id}_0.dat", true)
        }

        
    }
}
