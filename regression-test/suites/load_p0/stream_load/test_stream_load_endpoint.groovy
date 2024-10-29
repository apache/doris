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
    options.beNum = 1
    options.cloudMode = false
    docker(options) {
        // get fe ip
        def feIp = cluster.getMasterFe().getHttpAddress()[0]
        def fePort = cluster.getMasterFe().getHttpAddress()[1]
        def beIp = cluster.getBackends().get(0).getHttpAddress()[0]
        def beHttpPort = cluster.getBackends().get(0).getHttpAddress()[1]
        def beHeartbeatPort = cluster.getBackends().get(0).getHeartbeatPort()
        
        // set public endpoint and private endpoint in fe config
        sql """ADMIN SET FRONTEND CONFIG ('be_public_endpoint' = '1.2.3.4:8011')"""
        sql """ADMIN SET FRONTEND CONFIG ('be_private_endpoint' = '1.2.3.5:8012')"""
        def location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("1.2.3.4:8011"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        location = getRedirectLocation(feIp, fePort, "random-be")
        log.info("random-be location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        sql """ADMIN SET FRONTEND CONFIG ('be_private_endpoint' = '')"""
        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))


        // set public endpoint and private endpoint to backend
        sql """show backends"""
        sql """ALTER SYSTEM DROPP BACKEND '${beIp}:${beHeartbeatPort}'"""
        sql """ALTER SYSTEM ADD BACKEND '${beIp}:${beHeartbeatPort}' properties('tag.public_endpoint' = '11.10.10.10:8010', 'tag.private_endpoint' = '10.10.10.9:8020')"""
        sleep(10000)
        location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.10.10.10:8010"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020"))

        location = getRedirectLocation(feIp, fePort, "random-be")
        log.info("random-be location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020"))

        sql """ALTER SYSTEM MODIFY BACKEND '${beIp}:${beHeartbeatPort}' set ('tag.location' = 'default', 'tag.public_endpoint' = '11.10.10.10:8010')"""
        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))
    }

    // cloud mode
    def options2 = new ClusterOptions()
    options2.feNum = 1
    options2.beNum = 1
    options2.cloudMode = true
    docker(options2) {
        // get fe ip
        def feIp = cluster.getMasterFe().getHttpAddress()[0]
        def fePort = cluster.getMasterFe().getHttpAddress()[1]
        def beIp = cluster.getBackends().get(0).getHttpAddress()[0]
        def beHttpPort = cluster.getBackends().get(0).getHttpAddress()[1]
        def beHeartbeatPort = cluster.getBackends().get(0).getHeartbeatPort()
        
        // set public endpoint and private endpoint in fe config
        sql """ADMIN SET FRONTEND CONFIG ('be_public_endpoint' = '1.2.3.4:8011')"""
        sql """ADMIN SET FRONTEND CONFIG ('be_private_endpoint' = '1.2.3.5:8012')"""
        def location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("1.2.3.4:8011"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        location = getRedirectLocation(feIp, fePort, "random-be")
        log.info("random-be location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        sql """ADMIN SET FRONTEND CONFIG ('be_private_endpoint' = '')"""
        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        def clusters = sql """show clusters"""
        logger.info("show clusters : {}", clusters)
        def backends = sql """show backends"""
        logger.info("show backends : {}", backends)
        def computeGroupName = clusters[0][0]
        // set public endpoint and private endpoint to compute group
        sql """ALTER SYSTEM ADD BACKEND '' properties('tag.compute_group_name' = '${computeGroupName}',
              'tag.compute_group_public_endpoint' = '1.2.3.4:8011', 'tag.compute_group_private_endpoint' = '1.2.3.5:8012')"""
        sleep(30000)
        clusters = sql """show clusters"""
        logger.info("show clusters : {}", clusters)
        backends = sql """show backends"""
        logger.info("show backends : {}", backends)

        location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("1.2.3.4:8011"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        location = getRedirectLocation(feIp, fePort, "random-be")
        log.info("random-be location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("1.2.3.5:8012"))

        // set public endpoint and private endpoint to backend
        sql """ALTER SYSTEM DROPP BACKEND '${beIp}:${beHeartbeatPort}'"""
        sql """ALTER SYSTEM ADD BACKEND '${beIp}:${beHeartbeatPort}' properties('tag.compute_group_name' = '${computeGroupName}',
              'tag.compute_group_public_endpoint' = '11.10.10.10:8010', 'tag.compute_group_private_endpoint' = '10.10.10.9:8020')"""
        sleep(30000)

        backends = sql """show backends"""
        logger.info("show backends : {}", backends)
        location = getRedirectLocation(feIp, fePort, "public")
        log.info("public location: ${location}")
        assertTrue(location.contains("11.10.10.10:8010"))

        location = getRedirectLocation(feIp, fePort, "private")
        log.info("private location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020"))

        location = getRedirectLocation(feIp, fePort, "random-be")
        log.info("random-be location: ${location}")
        assertTrue(location.contains("${beIp}:${beHttpPort}"))

        location = getRedirectLocation(feIp, fePort, "")
        log.info("default location: ${location}")
        assertTrue(location.contains("10.10.10.9:8020"))
    }
}
