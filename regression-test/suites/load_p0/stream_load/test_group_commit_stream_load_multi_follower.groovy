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

import org.apache.doris.regression.suite.ClusterOptions
import org.codehaus.groovy.runtime.IOGroovyMethods

suite('test_group_commit_stream_load_multi_follower', 'docker') {
    def databaseName = context.config.getDbNameByFile(context.file)
    def tableName = "tbl"

    def groupCommitStreamLoad = { fe ->
        def feIp = fe.getHttpAddress()[0]
        def fePort = fe.getHttpAddress()[1]
        def command = """ curl -sS --location-trusted -u root:
                -H group_commit:async_mode
                -H column_separator:,
                -H columns:id,name
                -T ${context.config.dataPath}/load_p0/stream_load/test_stream_load1.csv
                http://${feIp}:${fePort}/api/${databaseName}/${tableName}/_stream_load """
        log.info("group commit command: ${command}")

        def process = command.execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
        def out = process.getText()
        logger.info("load through fe {}:{} master={} code={}, out={}, err={}",
                feIp, fePort, fe.isMaster, code, out, err)
        assertEquals(code, 0)

        def json = parseJson(out)
        assertEquals("success", json.Status.toLowerCase())
        assertTrue(json.GroupCommit)
        assertTrue(json.Label.startsWith("group_commit_"))
        assertEquals(2, json.NumberTotalRows)
        assertEquals(2, json.NumberLoadedRows)
        assertEquals(0, json.NumberFilteredRows)
        assertEquals(0, json.NumberUnselectedRows)
        assertTrue(json.ErrorURL == null || json.ErrorURL.isEmpty())
    }

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(1000)
            def rowCount = sql "select count(*) from ${databaseName}.${tableName}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def options = new ClusterOptions()
    options.feNum = 3
    options.beNum = 1
    options.cloudMode = true
    options.useFollowersMode = true
    options.beConfigs.add('enable_java_support=false')
    options.feConfigs.add('enable_forward_group_commit_stream_load=true')
    options.feConfigs.add('cloud_cluster_check_interval_second=1')
    options.feConfigs.add('heartbeat_interval_second=1')
    docker(options) {
        awaitUntil(60) {
            def ret = sql_return_maparray """SHOW FRONTENDS"""
            ret.size() == 3
                    && ret.count { it.Role.contains("FOLLOWER") } == 3
                    && ret.count { it.IsMaster == "true" } == 1
                    && ret.count { it.Alive == "true" } == 3
        }

        def frontendRows = sql_return_maparray """SHOW FRONTENDS"""
        logger.info("show frontends result {}", frontendRows)
        frontendRows.each { row ->
            assertTrue(row.Role.contains("FOLLOWER"))
            assertEquals("true", row.Alive)
        }

        sql """ CREATE DATABASE IF NOT EXISTS ${databaseName} """
        sql """ DROP TABLE IF EXISTS ${databaseName}.${tableName} """
        sql """
             CREATE TABLE IF NOT EXISTS ${databaseName}.${tableName} (
                 `id` int(11) NOT NULL,
                 `name` varchar(1100) NULL,
                 `score` int(11) NULL default "-1"
             ) ENGINE=OLAP
             DUPLICATE KEY(`id`, `name`)
             DISTRIBUTED BY HASH(`id`) BUCKETS 1
             PROPERTIES (
                 "replication_num" = "1",
                 "group_commit_interval_ms" = "200"
             );
        """

        def expectedRowCount = 0
        def frontends = cluster.getAllFrontends(true).sort { it.index }
        for (int i = 0; i < 2; i++) {
            frontends.each { fe ->
                groupCommitStreamLoad(fe)
                expectedRowCount += 2
            }
        }
        getRowCount(expectedRowCount)

        def rowCount = sql "select count(*) from ${databaseName}.${tableName}"
        logger.info("rowCount: " + rowCount)
        assertEquals(expectedRowCount, rowCount[0][0])    
    }
}
