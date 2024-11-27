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
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_agg_keys_schema_change_datev2") {
    def tbName = "test_agg_keys_schema_change_datev2"
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
            Awaitility.await().untilAsserted(() -> {
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                return compactionStatus.run_status;
            });
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
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""

    sql """ alter table ${tbName} add column `datev3` datev2 DEFAULT '2022-01-01' """
    int max_try_secs = 300
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });

    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01';"""
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });

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
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter  table ${tbName} add column `datev3` datetimev2 DEFAULT '2022-01-01 11:11:11' """
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11';"""
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });

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
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter  table ${tbName} add column `datev3` datetimev2(3) DEFAULT '2022-01-01 11:11:11.111' """
 
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });

    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    do_compact(tbName)
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11';"""
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ insert into ${tbName} values('2022-01-02', '2022-01-02 11:11:11', '2022-01-02 11:11:11.222', '2022-01-02', '2022-01-02 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev3`, `datev1`) values('2022-01-03', '2022-01-03 11:11:11', '2022-01-02 11:11:11.222', '2022-01-03');"""
    sql """ insert into ${tbName} (`datek1`, `datek2`, `datev3`, `datev2`) values('2022-01-04', '2022-01-04 11:11:11', '2022-01-02 11:11:11.222', '2022-01-04 11:11:11');"""
    sql """ insert into ${tbName} (`datek1`, `datev3`, `datev1`, `datev2`) values('2022-01-05', '2022-01-02 11:11:11.222', '2022-01-05', '2022-01-05 11:11:11');"""
    sql """ insert into ${tbName} (`datek2`, `datev3`, `datev1`, `datev2`) values('2022-01-06 11:11:11', '2022-01-02 11:11:11.222', '2022-01-06', '2022-01-06 11:11:11');"""
    sql """delete from ${tbName} where `datev3` = '2022-01-01 11:11:11.111';"""
    sql """sync"""
    qt_sql """select * from ${tbName} ORDER BY `datek1`;"""
    sql """ alter table ${tbName} drop column `datev3` """

    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });

    sql """ DROP TABLE  ${tbName} force"""
}
