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
import org.apache.doris.regression.util.NodeType

suite('test_stream_load_endpoint', 'docker') {
    def getRedirectLocation = { feIp, fePort, redirectPolicy ->
        def command = """ curl -v --max-redirs 0 --location-trusted -u root:  
                -H redirect-policy:$redirectPolicy  
                 -T ${context.config.dataPath}/load_p0/stream_load/test_stream_load1.csv 
                http://${feIp}:${fePort}/api/test/tbl/_stream_load """
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
    // local mode
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.cloudMode = false
    options.beConfigs.add('enable_java_support=false')
    options.feConfigs.add('enable_print_request_before_execution=true')
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
        
        log.info("Initial cluster setup - 3 BEs")
        log.info("BE1: ${be1Ip}:${be1HeartbeatPort}, BE2: ${be2Ip}:${be2HeartbeatPort}, BE3: ${be3Ip}:${be3HeartbeatPort}")
        sql """show backends"""
        
        // Drop BE1 and BE2
        log.info("Dropping BE1 and BE2")
        sql """ALTER SYSTEM DROPP BACKEND '${be1Ip}:${be1HeartbeatPort}'"""
        sql """ALTER SYSTEM DROPP BACKEND '${be2Ip}:${be2HeartbeatPort}'"""
        log.info("Backends after dropping BE1 and BE2: ${sql """show backends""" }")
        
        // Add BE1 back with custom endpoints
        log.info("Adding BE1 back with custom public and private endpoints")
        sql """ALTER SYSTEM ADD BACKEND '${be1Ip}:${be1HeartbeatPort}' properties('tag.public_endpoint' = '11.10.10.10:8010', 'tag.private_endpoint' = '10.10.10.9:8020')"""
        
        // Add BE2 back with different custom endpoints
        log.info("Adding BE2 back with different custom endpoints")
        sql """ALTER SYSTEM ADD BACKEND '${be2Ip}:${be2HeartbeatPort}' properties('tag.public_endpoint' = '12.20.20.20:8030', 'tag.private_endpoint' = '11.20.20.19:8040')"""
        
        // Modify BE3's endpoints
        log.info("Modifying BE3's endpoints")
        sql """ALTER SYSTEM MODIFY BACKEND '${be3Ip}:${be3HeartbeatPort}' set ('tag.public_endpoint' = '13.30.30.30:8050', 'tag.private_endpoint' = '12.30.30.29:8060', 'tag.location' = 'default')"""
        
        log.info("Final backends configuration: ${sql """show backends""" }")
        
        // Test redirect locations - should use one of the available BEs
        def location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.10.10.10:8010") || location.contains("12.20.20.20:8030") || location.contains("13.30.30.30:8050"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020") || location.contains("11.20.20.19:8040") || location.contains("12.30.30.29:8060"))

        location = getRedirectLocation(feIp, fePort, "direct")
        log.info("direct location: ${location}")
        assertTrue(location.contains("${be1Ip}:${be1HttpPort}") || location.contains("${be2Ip}:${be2HttpPort}") || location.contains("${be3Ip}:${be3HttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020") || location.contains("11.20.20.19:8040") || location.contains("12.30.30.29:8060"))

        // Test specific BE endpoint modifications
        log.info("Testing endpoint configuration for specific BEs")
        
        sql """ALTER SYSTEM MODIFY BACKEND '${be1Ip}:${be1HeartbeatPort}','${be2Ip}:${be2HeartbeatPort}','${be3Ip}:${be3HeartbeatPort}' set ('tag.location' = 'default')"""
        
        location = getRedirectLocation(feIp, fePort, "")
        log.info("final private location test: ${location}")
        assertTrue(location.contains("${be1Ip}:${be1HttpPort}") || location.contains("${be2Ip}:${be2HttpPort}") || location.contains("${be3Ip}:${be3HttpPort}"))

        sql """ALTER SYSTEM MODIFY BACKEND '${be1Ip}:${be1HeartbeatPort}','${be2Ip}:${be2HeartbeatPort}','${be3Ip}:${be3HeartbeatPort}' set ('tag.location' = 'default',  'tag.private_endpoint' = '12.30.30.29:8060')"""
        location = getRedirectLocation(feIp, fePort, "")
        log.info("final private location test: ${location}")
        assertTrue(location.contains("12.30.30.29:8060"))
    }

    // cloud mode
    def options2 = new ClusterOptions()
    options2.feNum = 1
    options2.beNum = 3
    options2.cloudMode = true
    options2.beConfigs.add('enable_java_support=false')
    options2.feConfigs.add('enable_print_request_before_execution=true')
    docker(options2) {
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
        
        log.info("Initial cluster setup - 3 BEs")
        log.info("BE1: ${be1Ip}:${be1HeartbeatPort}, BE2: ${be2Ip}:${be2HeartbeatPort}, BE3: ${be3Ip}:${be3HeartbeatPort}")
        sql """show backends"""
        
        // Drop BE1 and BE2
        log.info("Dropping BE1 and BE2")
        sql """ALTER SYSTEM DROPP BACKEND '${be1Ip}:${be1HeartbeatPort}'"""
        sql """ALTER SYSTEM DROPP BACKEND '${be2Ip}:${be2HeartbeatPort}'"""
        log.info("Backends after dropping BE1 and BE2: ${sql """show backends""" }")
        
        // Add BE1 back with custom endpoints
        log.info("Adding BE1 back with custom public and private endpoints")
        sql """ALTER SYSTEM ADD BACKEND '${be1Ip}:${be1HeartbeatPort}' properties('tag.public_endpoint' = '11.10.10.10:8010', 'tag.private_endpoint' = '10.10.10.9:8020',"tag.compute_group_name" = "default_compute_group")"""
        
        // Drop BE3
        sql """ALTER SYSTEM DROPP BACKEND '${be3Ip}:${be3HeartbeatPort}'"""

        // Add BE2 BE3 back with different custom endpoints
        log.info("Adding BE2 BE3 back with different custom endpoints")
        sql """ALTER SYSTEM ADD BACKEND '${be2Ip}:${be2HeartbeatPort}','${be3Ip}:${be3HeartbeatPort}' properties('tag.public_endpoint' = '12.20.20.20:8030', 'tag.private_endpoint' = '11.20.20.19:8040', "tag.compute_group_name" = "default_compute_group")"""

        sleep(30000) 
        
        log.info("Final backends configuration: ${sql """show backends""" }")
        
        // Test redirect locations - should use one of the available BEs
        def location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.10.10.10:8010") || location.contains("12.20.20.20:8030"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020") || location.contains("11.20.20.19:8040"))

        location = getRedirectLocation(feIp, fePort, "direct")
        log.info("direct location: ${location}")
        assertTrue(location.contains("${be1Ip}:${be1HttpPort}") || location.contains("${be2Ip}:${be2HttpPort}") || location.contains("${be3Ip}:${be3HttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020") || location.contains("11.20.20.19:8040"))
        
    }

}
