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

suite("test_cloud_edit_log_roll_checkpoint", "docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 1
    options.cloudMode = true
    options.feConfigs += [
        "cloud_edit_log_roll_interval_second=30"
    ]

    docker(options) {
        def config = sql_return_maparray """
            ADMIN SHOW FRONTEND CONFIG LIKE 'cloud_edit_log_roll_interval_second'
        """
        assertEquals(1, config.size())
        assertEquals("30", config[0].Value)

        def getCheckpointVersion = {
            def masterFe = cluster.getMasterFe()
            def response = parseJson(new URL(
                    "http://${masterFe.host}:${masterFe.httpPort}/api/show_meta_info?action=SHOW_HA").text)
            assertEquals(0, response.code)
            return response.data.last_checkpoint_version as long
        }

        long initialCheckpointVersion = getCheckpointVersion()
        logger.info("Initial checkpoint version: {}", initialCheckpointVersion)

        long previousCheckpointVersion = initialCheckpointVersion
        int checkpointCount = 0
        awaitUntil(180, 5) {
            long currentCheckpointVersion = getCheckpointVersion()
            logger.info("Current checkpoint version: {}", currentCheckpointVersion)
            if (currentCheckpointVersion > previousCheckpointVersion) {
                checkpointCount++
                previousCheckpointVersion = currentCheckpointVersion
                logger.info("Observed checkpoint image advancement {}, version: {}",
                        checkpointCount, currentCheckpointVersion)
            }
            return checkpointCount >= 2
        }
    }
}
