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
    sql """ DROP TABLE IF EXISTS t """
    sql """
        CREATE TABLE t (
            id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    def custoBeConfig = [
        "max_tablet_version_num": 1
    ]

    setBeConfigTemporary(custoBeConfig) {
        def beIds = sql """ SHOW BACKENDS """
        beIds.each { be ->
            def beId = be[0]
            def beHost = be[1]
            def beHttpPort = be[4]
            def response = "curl --max-time 10 -X GET http://${beHost}:${beHttpPort}/api/show_config?conf_item=max_tablet_version_num".execute().text
            logger.info("BE ${beId} config response: ${response}")
            assertTrue(response.contains("max_tablet_version_num") && response.contains("1"), 
                "Failed to set max_tablet_version_num to 1 on BE ${beId}")
        }

        try {
            sql """ INSERT INTO t VALUES (1) """
            sql """ INSERT INTO t VALUES (2) """
            assertTrue(false, "Expected TOO_MANY_VERSION error but none occurred")
        } catch (SQLException e) {
            logger.info("Exception caught: ${e.getMessage()}")
            def expectedError = "failed to init rowset builder. version count: 2, exceed limit: 1, tablet:"
            assertTrue(e.getMessage().contains(expectedError),
                "Expected TOO_MANY_VERSION error with message containing '${expectedError}', but got: ${e.getMessage()}")
        }
    }
}