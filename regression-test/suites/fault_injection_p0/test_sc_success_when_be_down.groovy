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

suite("test_sc_success_when_be_down", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.beNum = 3
    options.feNum = 2
    options.enableDebugPoints()
    options.feConfigs += ["agent_task_health_check_intervals_ms=5000"]

    docker(options) {
        GetDebugPoint().clearDebugPointsForAllBEs()

        def tblName = "test_sc_success_when_be_down"
        sql """ DROP TABLE IF EXISTS ${tblName} """
        sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v0` int NOT NULL,
                    `v1` int NOT NULL
                ) 
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(`k`) BUCKETS 24
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 3"
                )
        """
        sql """ INSERT INTO ${tblName} SELECT number, number, number from numbers("number" = "1024") """

        GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob._do_process_alter_tablet.sleep")
        sql """ ALTER TABLE ${tblName} MODIFY COLUMN v0 VARCHAR(100) """
        sleep(3000)
        cluster.stopBackends(1)
        waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tblName}' ORDER BY createtime DESC LIMIT 1 """
        time 600
}
    }
}
