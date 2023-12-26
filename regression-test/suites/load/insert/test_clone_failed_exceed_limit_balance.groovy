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

suite('test_clone_failed_exceed_limit_balance') {
    def options = new ClusterOptions()
    options.beNum = 4
    options.feConfigs.add('disable_balance=true')
    options.feConfigs.add('publish_version_timeout_second=86400')
    options.feConfigs.add('clone_tablet_in_window_failed_limit_number=5')
    options.beConfigs.add('report_task_interval_seconds=1')
    options.enableDebugPoints()
    docker(options) {
        def tbl = 'test_clone_failed_exceed_limit_balance_tbl'

        sql """ DROP TABLE IF EXISTS ${tbl} """

        sql """
         CREATE TABLE ${tbl} (
           `k1` int(11) NULL,
           `k2` int(11) NULL
         )
         DUPLICATE KEY(`k1`, `k2`)
         COMMENT 'OLAP'
         DISTRIBUTED BY HASH(`k1`) BUCKETS 1
         PROPERTIES (
           "replication_num"="3"
         );
         """

        sql """INSERT INTO ${tbl} (k1, k2) VALUES (1, 10)"""

        def result = sql_return_maparray """SHOW TABLETS FROM ${tbl}"""
        logger.info("result1 {}", result)

        def tabletId = null
        def backendId = null
        def version = null
        def lstSuccVersion = null
        for (def res : result) {
            tabletId = res.TabletId
            backendId = res.BackendId
            version = res.Version
            lstSuccVersion = res.LstSuccessVersion
            break
        }
        // check result version
        assertEquals(3, result.size())
        assertEquals(result[0].Version, result[1].Version)
        assertEquals(result[0].Version, result[2].Version)
        assertEquals(result[0].LstSuccessVersion, result[1].LstSuccessVersion)
        assertEquals(result[0].LstSuccessVersion, result[2].LstSuccessVersion)

        def be1 = cluster.getBeByBackendId(backendId.toLong())
        be1.enableDebugPoint('EngineCloneTask.finish.random', ['percent': 1])
        be1.enableDebugPoint('EnginePublishVersionTask.finish.random', ['percent': 1])


        sql """INSERT INTO ${tbl} (k1, k2) VALUES (2, 20)"""
        // check result version, be1 less
        result = sql_return_maparray """SHOW TABLETS FROM ${tbl}"""
        logger.info("result2 {}", result) 
        assertEquals(3, result.size())

        // find be1's result
        def lstFailedVersion = null
        for (def res : result) {
          if (res.BackendId == backendId) {
            version = res.Version
            lstSuccVersion = res.LstSuccessVersion
            lstFailedVersion = res.LstFailedVersion
            backendId = res.BackendId
            break;
          }
        }

        def otherVersion = null
        for (def res : result) {
          if (res.BackendId != backendId) {
            assertEquals(version.toLong() + 1, res.Version.toLong())
            assertEquals(lstSuccVersion.toLong() + 1, res.LstSuccessVersion.toLong())
            assertEquals(-1, res.LstFailedVersion.toLong())
            otherVersion = res.Version
          }
        }
        assertEquals(lstFailedVersion.toLong(), otherVersion.toLong())

        // check be1's replica clone to other
        def finalRes = null
        for (int i = 0; i < 500; i++) {
          result = sql_return_maparray """SHOW TABLETS FROM ${tbl}"""
          logger.info("result final retry {} : {}", i, result) 
          boolean succ = true
          for (def res : result) {
            if (res.BackendId == backendId){
              succ = false
            }
          }
          if (succ) {
            finalRes = result
            break;
          }
          sleep(1000)
        }

        // check clone succ
        assertEquals(3, finalRes.size())
        assertEquals(finalRes[0].Version, finalRes[1].Version)
        assertEquals(finalRes[0].Version, finalRes[2].Version)
        assertEquals(finalRes[0].LstSuccessVersion, finalRes[1].LstSuccessVersion)
        assertEquals(finalRes[0].LstSuccessVersion, finalRes[2].LstSuccessVersion)

        qt_sql """select * from ${tbl}"""

        be1.clearDebugPoints()
    }
}