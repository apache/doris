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

suite("test_compaction_score_action") {
    def tableName = "test_compaction_score_action";

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            id INT NOT NULL,
            name STRING NOT NULL
        ) DUPLICATE KEY (`id`)
          PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    for (i in 0..<30) {
        sql """ INSERT INTO ${tableName} VALUES(1, "Vedal") """
        sql """ INSERT INTO ${tableName} VALUES(2, "Neuro") """
        sql """ INSERT INTO ${tableName} VALUES(3, "Evil") """
    }

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    for (int i=0;i<backendId_to_backendIP.size();i++){
        def beHttpAddress =backendId_to_backendIP.entrySet()[i].getValue()+":"+backendId_to_backendHttpPort.entrySet()[i].getValue()
        if (isCloudMode()) {
            def (code, text, err) = curl("GET",beHttpAddress+ "/api/compaction_score?top_n=1&sync_meta=true")
            def score_str = parseJson(text).get(0).get("compaction_score")
            def score = Integer.parseInt(score_str)
            assertTrue(score >= 90)
        } else {
            def (code, text, err) = curl("GET",beHttpAddress+"/api/compaction_score?top_n=1")
            def score_str = parseJson(text).get(0).get("compaction_score")
            def score = Integer.parseInt(score_str)
            assertTrue(score >= 90)
        }
    }
}
