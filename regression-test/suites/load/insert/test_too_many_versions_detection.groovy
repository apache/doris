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
import java.sql.SQLException

suite("too_many_versions_detection", "nonConcurrent") {
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def get_be_param = { paramName ->
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            logger.info("get config: code=" + code + ", out=" + out + ", err=" + err)
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            paramValue = resultList[2]
            break // only need to get from one BE
        }
        return paramValue
    }

    // Read and save original config value
    def originalMaxTabletVersionNum = get_be_param("max_tablet_version_num")
    logger.info("Original max_tablet_version_num: ${originalMaxTabletVersionNum}")
    
    try {
        set_be_config("max_tablet_version_num", "200")

        sql """ DROP TABLE IF EXISTS t """

        sql """
            create table t(a int)
            DUPLICATE KEY(a)
            DISTRIBUTED BY HASH(a)
            BUCKETS 10 PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
        """

        for (int i = 1; i <= 200; i++) {
            sql """ INSERT INTO t VALUES (${i}) """
        }

        try {
            sql """ INSERT INTO t VALUES (201) """
            assertTrue(false, "Expected TOO_MANY_VERSION error but none occurred")
        } catch (SQLException e) {
            logger.info("Exception caught: ${e.getMessage()}")
            def expectedError = "failed to init rowset builder. version count: 201, exceed limit: 200, tablet:"
            assertTrue(e.getMessage().contains(expectedError),
                "Expected TOO_MANY_VERSION error with message containing '${expectedError}', but got: ${e.getMessage()}")
        }

        sql """ DROP TABLE IF EXISTS t """
    } finally {
        // Restore original config
        set_be_config("max_tablet_version_num", originalMaxTabletVersionNum)
    }
}