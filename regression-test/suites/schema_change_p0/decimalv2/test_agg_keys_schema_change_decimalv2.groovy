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

suite("test_agg_keys_schema_change_decimalv2", "nonConcurrent") {
    def config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'disable_decimalv2'; """
    String old_value1 = config_row[0][1]

    config_row = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_decimal_conversion'; """
    String old_value2 = config_row[0][1]

    sql """
        admin set frontend config("enable_decimal_conversion" = "false");
    """
    sql """
        admin set frontend config("disable_decimalv2" = "false");
    """

    def tbName = "test_agg_keys_schema_change_decimalv2"
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         logger.info(jobStateResult.toString());
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
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id )
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
               `decimalv2k1` DECIMALV2(27,9),
               `decimalv2k2` DECIMALV2(21,3),
               `decimalv2k3` DECIMALV2(21,3),
               `decimalv2v1` DECIMALV2(27,9) SUM,
               `decimalv2v2` DECIMALV2(21,3) SUM
           )
           AGGREGATE  KEY(`decimalv2k1`,`decimalv2k2`, decimalv2k3)
           DISTRIBUTED BY HASH(`decimalv2k1`) BUCKETS 1
           PROPERTIES("replication_num" = "1", "light_schema_change" = "false");
        """
    sql """ insert into ${tbName} values
        (111111111111111111.111111111,999999999999999999.994,999999999999999999.999,999999999999999999.999999994,999999999999999999.995);
    """
    qt_sql1 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    sql """ alter table ${tbName} add column `decimalv2v3` decimalv2(27,9) """
    int max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """sync"""
    qt_sql2 """select * from ${tbName} ORDER BY 1,2,3,4;"""
    trigger_and_wait_compaction(tbName, "cumulative")
    sql """sync"""
    qt_sql3 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    sql """ alter table ${tbName} drop column `decimalv2v3` """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql4 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV2(21,3) -> decimalv3 OK
    sql """ alter table ${tbName} modify column decimalv2k2 DECIMALV3(21,3) key """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql5 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV2(21,3) -> decimalv3 OK
    sql """ alter table ${tbName} modify column decimalv2k3 DECIMALV3(38,10) key """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql5_2 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV2(27,9) -> decimalv3, round scale part, not overflow
    sql """ alter table ${tbName} modify column decimalv2v1 DECIMALV3(26,8) sum """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql6 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV2(21,3) -> decimalv3,  round scale part, overflow
    sql """ alter table ${tbName} modify column decimalv2v2 DECIMALV3(20,2) sum """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "CANCELLED") {
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql7 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV2(21,3) -> decimalv3,  narrow integral, overflow
    sql """ alter table ${tbName} modify column decimalv2v2 DECIMALV3(20,3) sum """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "CANCELLED") {
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql8 """select * from ${tbName} ORDER BY 1,2,3,4;"""


    // DECIMALV3(21,3) -> decimalv2 OK
    sql """ alter table ${tbName} modify column decimalv2k2 DECIMALV2(21,3) key """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql9 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV3(26,8) -> decimalv2
    sql """ alter table ${tbName} modify column decimalv2v1 DECIMALV2(25,7) sum """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql9_2 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // DECIMALV3(26,8) -> decimalv2, narrow integer
    sql """ alter table ${tbName} modify column decimalv2v1 DECIMALV2(25,8) sum """
    max_try_time = 1000
    while (max_try_time--){
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            sleep(3000)
            break
        } else {
            sleep(1000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }

    sql """sync"""
    qt_sql9_3 """select * from ${tbName} ORDER BY 1,2,3,4;"""

    // restore disable_decimalv2 to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("disable_decimalv2" = "${old_value1}"); """

    // restore enable_decimal_conversion to old_value
    sql """ ADMIN SET FRONTEND CONFIG ("enable_decimal_conversion" = "${old_value2}"); """
}
