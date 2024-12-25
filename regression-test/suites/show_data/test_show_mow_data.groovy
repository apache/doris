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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/tpcds
// and modified by Doris.
import org.codehaus.groovy.runtime.IOGroovyMethods

 // loading one data 10 times, expect data size not rising
suite("test_mow_show_data_in_cloud","p2") {
    //cloud-mode
    if (!isCloudMode()) {
        logger.info("not cloud mode, not run")
        return
    }

    def repeate_stream_load_same_data = { String tableName, int loadTimes ->
        for (int i = 0; i < loadTimes; i++) {
            streamLoad {
                table tableName
                set 'column_separator', '|'
                set 'compress_type', 'GZ'
                file """${getS3Url()}/regression/tpch/sf1/lineitem.csv.split00.gz"""
                time 10000 // limit inflight 10s
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                    assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    def get_tablets_from_table = { String table ->
        def res = sql_return_maparray """show tablets from  ${table}"""
        return res 
    }

    def show_tablet_compaction = { HashMap tablet -> 
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET ")
        sb.append(tablet["CompactionStatus"])
        String command = sb.toString()
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        return parseJson(out.trim())
    }

    def trigger_tablet_compaction = { HashMap tablet, String compact_type ->
        //support trigger base/cumulative/full compaction
        def tabletStatusBeforeCompaction = show_tablet_compaction(tablet)
        
        String tabletInBe = tablet
        String showCompactionStatus = tablet["CompactionStatus"]
        String triggerCompactionUrl = showCompactionStatus.split("show")[0] + "run?tablet_id=" + tablet["TabletId"] + "&compact_type=" + compact_type
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X POST ")
        sb.append(triggerCompactionUrl)
        String command = sb.toString()
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        out = process.getText()
        def outJson = parseJson(out)
        logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
        // if code = 0 means compaction happend, need to check
        // other condition may indicate no suitable compaction condition
        if ( code == 0 && outJson.status.toLowerCase() == "success" ){
            def compactionStatus = "RUNNING"
            def tabletStatusAfterCompaction = null
            long startTime = System.currentTimeMillis()
            long timeoutTimestamp = startTime + 5 * 60 * 1000 // 5 min
            do {
                tabletStatusAfterCompaction = show_tablet_compaction(tablet)
                logger.info("tabletStatusAfterCompaction class: " + tabletStatusAfterCompaction.class)
                logger.info("hhhhhh: " + tabletStatusAfterCompaction.toString())
                if (tabletStatusAfterCompaction.rowsets.size() < tabletStatusBeforeCompaction.rowsets.size()){
                    compactionStatus = 'FINISHED'
                }
                Thread.sleep(60 * 1000)
            } while (timeoutTimestamp > System.currentTimeMillis() && (status != 'FINISHED'))

            if (status != "FINISHED") {
                logger.info("compaction not Finish or failed")
                return false
            }
        }
    }

    def trigger_compaction = { List<List<Object>> tablets ->
        for(def tablet: tablets) {
            trigger_tablet_compaction(tablet, "cumulative")
            trigger_tablet_compaction(tablet, "base")
            trigger_tablet_compaction(tablet, "full")
        }
    }

    def caculate_table_data_size_in_backend_storage = { List<List<Object>> tablets ->
        storageType = context.config.otherConfigs.get("storageProvider")
        Double storageSize = 0

        List<String> tabletIds = []
        for(def tablet: tablets) {
            tabletIds.add(tablet["TabletId"])
        }

        if (storageType.toLowerCase() == "oss") {
            //cbs means cluster backend storage
            ak = context.config.otherConfigs.get("cbsS3Ak")
            sk = context.config.otherConfigs.get("cbsS3Sk")
            endpoint = context.config.otherConfigs.get("cbsS3Endpoint")
            bucketName =  context.config.otherConfigs.get("cbsS3Bucket")
            storagePrefix = context.config.otherConfigs.get("cbsS3Prefix")

            client = initOssClient(ak, sk, endpoint)
            for(String tabletId: tabletIds) {
                storageSize += calculateFolderLength(client, bucketName, storagePrefix + "/data/" + tabletId)
            }
            shutDownOssClient(client)
        }

        if (storageType.toLowerCase() == "hdfs") {
            fsName = context.config.otherConfigs.get("cbsFsName")
            isKerberosFs = context.config.otherConfigs.get("cbsFsKerberos")
            fsUser = context.config.otherConfigs.get("cbsFsUser")
            storagePrefix = context.config.otherConfigs.get("cbsFsPrefix")
        }

        return storageSize
    }

    def translate_different_unit_to_MB = { String size, String unitField ->
        Double sizeKb = 0.0
        if (unitField == "KB") {
            sizeKb = Double.parseDouble(size) / 1024
        } else if (unitField == "MB") {
            sizeKb = Double.parseDouble(size)
        } else if (unitField == "GB") {
            sizeKb = Double.parseDouble(size) * 1024 * 1024
        } else if (unitField == "TB") {
            sizeKb = Double.parseDouble(size) * 1024 * 1024 * 1024
        }
        return sizeKb
    }

    def show_table_data_size_through_mysql = { String table ->
        def mysqlShowDataSize = 0L
        def res = sql_return_maparray " show data from ${table}"
        def tableSizeInfo = res[0]
        def fields = tableSizeInfo["Size"].split(" ")
        if (fields.length == 2 ){
            def sizeField = fields[0]
            def unitField = fields[1]
            mysqlShowDataSize = translate_different_unit_to_MB(sizeField, unitField)
        }
        return mysqlShowDataSize
    }

    def caculate_table_data_size_through_api = { List<List<Object>> tablets ->
        Double apiCaculateSize = 0 
        for (HashMap tablet in tablets) {
            def tabletStatus = show_tablet_compaction(tablet)
            
            for(String rowset: tabletStatus.rowsets){
                def fields = rowset.split(" ")
                if (fields.length == 7) {
                    def sizeField = fields[-2]  // the last field（size）
                    def unitField = fields[-1]   // The second to last field（unit）
                    // 转换成 KB
                    apiCaculateSize += translate_different_unit_to_MB(sizeField, unitField )
                }
            }
        }

        return apiCaculateSize
    }

    def main = {
        tableName="lineitem_mow"
        sql "DROP TABLE IF EXISTS ${tableName};"
        sql new File("""${context.file.parent}/ddl/${tableName}.sql""").text
        sql new File("""${context.file.parent}/ddl/lineitem_delete.sql""").text.replaceAll("\\\$\\{table\\}", tableName)
        List<String> tablets = get_tablets_from_table(tableName)
        def loadTimes = [1, 10]
        Map<String, List> sizeRecords = ["apiSize":[], "mysqlSize":[], "cbsSize":[]]
        for (int i in loadTimes){
            // stream load 1 time, record each size
            repeate_stream_load_same_data(tableName, i)
            def rows = sql_return_maparray "select count(*) as count from ${tableName};"
            logger.info("table ${tableName} has ${rows[0]["count"]} rows")
            // 加一下触发compaction的机制
            trigger_compaction(tablets)

            // 然后 sleep 5min， 等fe汇报完
            sleep(300 * 1000)

            sizeRecords["apiSize"].add(caculate_table_data_size_through_api(tablets))
            sizeRecords["cbsSize"].add(caculate_table_data_size_in_backend_storage(tablets))
            sizeRecords["mysqlSize"].add(show_table_data_size_through_mysql(tableName))
            sleep(300 * 1000)
            logger.info("after ${i} times stream load, mysqlSize is: ${sizeRecords["mysqlSize"][-1]}, apiSize is: ${sizeRecords["apiSize"][-1]}, storageSize is: ${sizeRecords["cbsSize"][-1]}")
            
        }

        // expect mysqlSize == apiSize == storageSize
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["apiSize"][0])
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["cbsSize"][0])
        // expect load 1 times ==  load 10 times
        assertEquals(sizeRecords["mysqlSize"][0], sizeRecords["mysqlSize"][1])
        assertEquals(sizeRecords["apiSize"][0], sizeRecords["apiSize"][1])
        assertEquals(sizeRecords["cbsSize"][0], sizeRecords["cbsSize"][1])
    }

    main()
}
