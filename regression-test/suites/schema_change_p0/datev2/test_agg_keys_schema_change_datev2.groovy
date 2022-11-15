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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_agg_keys_schema_change_datev2") {
    def tbName = "test_agg_keys_schema_change_datev2"
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    String[][] backends = sql """ show backends; """
    assertTrue(backends.size() > 0)
    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    for (String[] backend in backends) {
        backendId_to_backendIP.put(backend[0], backend[2])
        backendId_to_backendHttpPort.put(backend[0], backend[5])
    }

    backend_id = backendId_to_backendIP.keySet()[0]
    StringBuilder showConfigCommand = new StringBuilder();
    showConfigCommand.append("curl -X GET http://")
    showConfigCommand.append(backendId_to_backendIP.get(backend_id))
    showConfigCommand.append(":")
    showConfigCommand.append(backendId_to_backendHttpPort.get(backend_id))
    showConfigCommand.append("/api/show_config")
    logger.info(showConfigCommand.toString())
    def process = showConfigCommand.toString().execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    String out = process.getText()
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    def do_compact = { tableName ->
        String[][] tablets = sql """ show tablets from ${tableName}; """
        for (String[] tablet in tablets) {
            String tablet_id = tablet[0]
            backend_id = tablet[2]
            logger.info("run compaction:" + tablet_id)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://")
            sb.append(backendId_to_backendIP.get(backend_id))
            sb.append(":")
            sb.append(backendId_to_backendHttpPort.get(backend_id))
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=cumulative")

            String command = sb.toString()
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(100)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X GET http://")
                sb.append(backendId_to_backendIP.get(backend_id))
                sb.append(":")
                sb.append(backendId_to_backendHttpPort.get(backend_id))
                sb.append("/api/compaction/run_status?tablet_id=")
                sb.append(tablet_id)

                String command = sb.toString()
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }

    sql """ DROP TABLE IF EXISTS ${tbName} FORCE"""
    // Create table and disable light weight schema change
    sql """
           CREATE TABLE IF NOT EXISTS ${tbName}
           (
               `datek1` date DEFAULT '2022-01-01',
               `datek2` datetime DEFAULT '2022-01-01 11:11:11',
               `datev1` date MAX DEFAULT '2022-01-01',
               `datev2` datetime MAX DEFAULT '2022-01-01 11:11:11'
           )
           AGGREGATE  KEY(`datek1`,`datek2`)
           DISTRIBUTED BY HASH(`datek1`) BUCKETS 1
           PROPERTIES("replication_num" = "1", "light_schema_change" = "false");
        """
    // datev2
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""

    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""

    sql """ alter table ${tbName} add column `datev3` datev2 DEFAULT '2022-01-01' """
    int max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01';"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    // datetimev2(0)
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""

    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter  table ${tbName} add column `datev3` datetimev2 DEFAULT '2022-01-01 11:11:11' """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11';"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    // datetimev2(3)
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""

    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev1`, `datev2`) values('2022-01-05', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-06', '2022-01-06 11:11:11');"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter  table ${tbName} add column `datev3` datetimev2(3) DEFAULT '2022-01-01 11:11:11.111' """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11';"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02 11:11:11.222', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev3`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-02 11:11:11.222', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev3`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-02 11:11:11.222', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev3`, `datev1`, `datev2`) values('2022-01-05', '2022-01-02 11:11:11.222', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev3`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-02 11:11:11.222', '2022-01-06', '2022-01-06 11:11:11');"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11.111';"""
    sql """sync"""
    qt_sql """select /*+ SET_VAR(enable_vectorized_engine=true) */ * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """
    max_try_time = 1000
    while(max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """ DROP TABLE  ${tbName} force"""
}
