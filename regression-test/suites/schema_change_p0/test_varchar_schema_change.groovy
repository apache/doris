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

suite ("test_varchar_schema_change") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def tableName = "varchar_schema_change_regression_test"

    try {

        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    `c0` LARGEINT NOT NULL,
                    `c1` DATE NOT NULL,
                    `c2` VARCHAR(20),
                    `c3` VARCHAR(5) DEFAULT '0'
                ) DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES ( "replication_num" = "1", "light_schema_change" = "true" )
            """

        sql """ insert into ${tableName} values
                (22,'2017-10-01',2147483647,13)
            """
        sql """ insert into ${tableName} values
                (55,'2019-06-01',21474836470,100)
            """

        test {
            sql """ alter table ${tableName} modify column c2 varchar(10)
                """
            exception "Cannot shorten string length"
        }

        // test {//为什么第一次改没发生Nothing is changed错误？查看branch-1.2-lts代码
        //     sql """ alter table ${tableName} modify column c2 varchar(20)
        //             """
        //     exception "Nothing is changed"
        // }

        sql """ alter table ${tableName} modify column c2 varchar(30) """

        int max_try_time = 1200
        while (max_try_time--){
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }

        String[][] res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"varchar(30)")

        qt_sc " select * from ${tableName} order by 1,2; "

        // test { //没捕获到异常
        //     sql """ insert into ${tableName} values(92,'2017-12-01',483647,'sdafdsaf') """
        //     exception "Insert has filtered data in strict mode"
        // }

        sql """ insert into ${tableName} values(22,'2017-12-01',483647,'sdafd') """
        sql """ insert into ${tableName} values(55,'2019-11-21',21474,'123aa') """

        sql """ alter table ${tableName} modify column c2 INT """
        max_try_time = 1200
        while (max_try_time--){
            String result = getJobState(tableName)
            if (result == "CANCELLED" || result == "FINISHED") {
                assertEquals(result, "CANCELLED")
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }

        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"varchar(30)")

        qt_sc " select * from ${tableName} where c2 like '%1%' order by 1,2; "

        sql """ insert into ${tableName} values(22,'2011-12-01','12f2','fdsaf') """
        sql """ insert into ${tableName} values(55,'2009-11-21','12d1d113','123aa') """

        // compaction
        String[][] tablets = sql """ show tablets from ${tableName}; """
        for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                backend_id = tablet[2]
                logger.info("run compaction:" + tablet_id)
                def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        }

        // wait for all compactions done
        for (String[] tablet in tablets) {
                boolean running = true
                do {
                    Thread.sleep(100)
                    String tablet_id = tablet[0]
                    backend_id = tablet[2]
                    def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running)
        }

        qt_sc " select * from ${tableName} order by 1,2; "
        qt_sc " select min(c2),max(c2) from ${tableName} order by 1,2; "
        qt_sc " select min(c2),max(c2) from ${tableName} group by c0 order by 1,2; "

        sleep(5000)
        sql """ alter table ${tableName}
        modify column c2 varchar(40),
        modify column c3 varchar(6) DEFAULT '0'
        """
        max_try_time = 1200
        while (max_try_time--){
            String result = getJobState(tableName)
            if (result == "FINISHED") {
                break
            } else {
                sleep(100)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }

        res = sql """ desc ${tableName} """
        logger.info(res[2][1])
        assertEquals(res[2][1].toLowerCase(),"varchar(40)")

        qt_sc " select * from ${tableName} order by 1,2; "

        // test{
        //     sql """ alter table t0 modify column c1 varchar(20) NOT NULL """
        //     exception "Can not change DATE to VARCHAR"
        // }

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
