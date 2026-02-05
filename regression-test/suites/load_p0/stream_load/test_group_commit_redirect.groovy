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

suite('test_group_commit_redirect', 'docker') {
    def databaseName = context.config.getDbNameByFile(context.file)
    def tableName = "tbl"
    def getRedirectLocation = { feIp, fePort, redirectPolicy, mode ->
        def command = """ curl -v --max-redirs 0 --location-trusted -u root:  
                -H redirect-policy:$redirectPolicy  
                -H group_commit:$mode
                 -T ${context.config.dataPath}/load_p0/stream_load/test_stream_load1.csv 
                http://${feIp}:${fePort}/api/${databaseName}/${tableName}/_stream_load """
        log.info("redirect command: ${command}")

        def code = -1
        def location = ""
        try {
            def process = command.execute()
            code = process.waitFor()
            // Parse Location from stderr since curl -v outputs headers to stderr
            def errorOutput = process.err.text
            def locationLine = errorOutput.readLines().find { it.trim().startsWith('< Location: ') }
            if (locationLine) {
                location = locationLine.trim().substring('< Location: '.length())
            }
            log.info("curl output: ${process.text}")
            log.info("curl error: ${errorOutput}")
        } catch (Exception e) {
            log.info("exception: ${e}".toString())
        }
        return location
    }

    def groupCommitRedicetSteamLoad = { beIp, bePort, targetBe, targetBePort  ->
        def command = """ curl -v --location-trusted -u root:  
                -H group_commit:async_mode
                -H column_separator:,
                -H columns:id,name
                 -T ${context.config.dataPath}/load_p0/stream_load/test_stream_load1.csv 
                http://${beIp}:${bePort}/api/${databaseName}/${tableName}/_stream_load_forward?forward_to=${targetBe}:${targetBePort} """
        log.info("redirect command: ${command}")

        def process = command.execute()
        def code = process.waitFor()
        def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
        def out = process.getText()
        logger.info("code=" + code + ", out=" + out + ", err=" + err)
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
            sleep(2000)
            def rowCount = sql "select count(*) from ${tableName}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    // cloud mode
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.cloudMode = true
    options.beConfigs.add('enable_java_support=false')
    options.feConfigs.add('enable_group_commit_streamload_be_forward=true')
    docker(options) {
        // get fe and be info
        def feIp = cluster.getMasterFe().getHttpAddress()[0]
        def fePort = cluster.getMasterFe().getHttpAddress()[1]
        
        def be1Ip = cluster.getBackends().get(0).getHttpAddress()[0]
        def be1HttpPort = cluster.getBackends().get(0).getHttpAddress()[1]
        def be1HeartbeatPort = cluster.getBackends().get(0).getHeartbeatPort()
        
        def be2Ip = cluster.getBackends().get(1).getHttpAddress()[0]
        def be2HttpPort = cluster.getBackends().get(1).getHttpAddress()[1]
        def be2HeartbeatPort = cluster.getBackends().get(1).getHeartbeatPort()
        
        def be3Ip = cluster.getBackends().get(2).getHttpAddress()[0]
        def be3HttpPort = cluster.getBackends().get(2).getHttpAddress()[1]
        def be3HeartbeatPort = cluster.getBackends().get(2).getHeartbeatPort()
        
        def msIp = cluster.getMetaservices().get(0).getHttpAddress()[0]
        def msPort = cluster.getMetaservices().get(0).getHttpAddress()[1]
        
        sql """ CREATE DATABASE IF NOT EXISTS ${databaseName} """
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
        // test be forward stream load
        groupCommitRedicetSteamLoad(be1Ip, be1HttpPort, be1Ip, be1HttpPort)
        groupCommitRedicetSteamLoad(be2Ip, be2HttpPort, be1Ip, be1HttpPort)
        groupCommitRedicetSteamLoad(be2Ip, be2HttpPort, be1Ip, be1HttpPort)

        getRowCount(6)
        qt_sql " SELECT * FROM ${tableName} order by id, name, score asc; "

        // test fe redirect policy
        log.info("Initial cluster setup - 3 BEs")
        log.info("BE1: ${be1Ip}:${be1HeartbeatPort}, BE2: ${be2Ip}:${be2HeartbeatPort}, BE3: ${be3Ip}:${be3HeartbeatPort}")
        sql """show backends"""
        
        log.info("Dropping all BE")
        sql """ALTER SYSTEM DROPP BACKEND '${be1Ip}:${be1HeartbeatPort}'"""
        sql """ALTER SYSTEM DROPP BACKEND '${be2Ip}:${be2HeartbeatPort}'"""
        sql """ALTER SYSTEM DROPP BACKEND '${be3Ip}:${be3HeartbeatPort}'"""
        log.info("after dropping all BE: ${sql """show backends""" }")
        
        log.info("Adding BE1 BE2 BE3 back with different custom endpoints")
        sql """ALTER SYSTEM ADD BACKEND '${be1Ip}:${be1HeartbeatPort}','${be2Ip}:${be2HeartbeatPort}','${be3Ip}:${be3HeartbeatPort}' properties('tag.public_endpoint' = '12.20.20.20:8030', 'tag.private_endpoint' = '11.20.20.19:8040', "tag.compute_group_name" = "compute_cluster")"""

        sleep(30000) 
        
        log.info("Final backends configuration: ${sql """show backends""" }")
        
        // Test redirect locations - should use one of the available BEs
        def location = getRedirectLocation(feIp, fePort, "public", "async_mode")
        log.info("public location: ${location}")
        // redirect url: http://endpoint:port/api/db/table/_stream_load_forward?param=value&forward_to=target_be:port
        assertTrue(location.contains("12.20.20.20:8030/api/$databaseName/$tableName/_stream_load_forward?") && (location.contains("forward_to=${be1Ip}:${be1HttpPort}") || location.contains("forward_to=${be2Ip}:${be2HttpPort}") || location.contains("forward_to=${be3Ip}:${be3HttpPort}")))

        location = getRedirectLocation(feIp, fePort, "private", "sync_mode")
        log.info("private location: ${location}")
        assertTrue(location.contains("11.20.20.19:8040/api/$databaseName/$tableName/_stream_load_forward?") && (location.contains("forward_to=${be1Ip}:${be1HttpPort}") || location.contains("forward_to=${be2Ip}:${be2HttpPort}") || location.contains("forward_to=${be3Ip}:${be3HttpPort}")))

        location = getRedirectLocation(feIp, fePort, "", "async_mode")
        log.info("default location: ${location}")
        assertTrue(location.contains("11.20.20.19:8040/api/$databaseName/$tableName/_stream_load_forward?") && (location.contains("forward_to=${be1Ip}:${be1HttpPort}") || location.contains("forward_to=${be2Ip}:${be2HttpPort}") || location.contains("forward_to=${be3Ip}:${be3HttpPort}")))

        location = getRedirectLocation(feIp, fePort, "public", "off_mode")
        log.info("public location: ${location}")
        assertTrue(location.contains("12.20.20.20:8030/api/$databaseName/$tableName/_stream_load"))

        location = getRedirectLocation(feIp, fePort, "private", "off_mode")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.20.20.19:8040/api/$databaseName/$tableName/_stream_load"))

        location = getRedirectLocation(feIp, fePort, "", "off_mode")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.20.20.19:8040/api/$databaseName/$tableName/_stream_load"))

    }

}
