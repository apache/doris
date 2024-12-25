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

suite("test_schema_change_varchar_to_datev2") {
    def tbName = "test_schema_change_varchar_to_datev2"
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
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
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(100)
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
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
                `k1` int(11) NOT NULL,
                `k2` varchar(4096) NOT NULL,
                `k3` VARCHAR(4096) NOT NULL,
                `v1` float SUM NOT NULL,
                `v2` decimal(20, 7) SUM NOT NULL
            )
            AGGREGATE KEY(`k1`,`k2`,`k3`)
            DISTRIBUTED BY HASH(`k1`,`k2`) BUCKETS 1
            PROPERTIES("replication_num" = "1", "light_schema_change" = "false");
        """
    // insert
    sql """ insert into ${tbName} values(1,"1","20200101",1,1), (2,"2","2020/01/02",2,2), (3,"3","2020-01-03",3,3), (4,"4","200104",4,4), (5,"5","20/01/05",5,5), (6,"6","20-01-06",6,6)"""
    // select
    qt_sql_1 """select * from ${tbName} ORDER BY `k1`;"""

    sql """ alter table ${tbName} modify column `k3` date; """
    int max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(100)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql_2 """select * from ${tbName} ORDER BY `k1`;"""
    trigger_and_wait_compaction(tbName, "cumulative")
    sql """sync"""
    qt_sql_3 """select * from ${tbName} ORDER BY `k1`;"""
    sql """delete from ${tbName} where `k3` = '2020-01-02';"""
    sql """sync"""
    qt_sql_4 """select * from ${tbName} ORDER BY `k1`;"""

    sql """ DROP TABLE  ${tbName} force"""
}
