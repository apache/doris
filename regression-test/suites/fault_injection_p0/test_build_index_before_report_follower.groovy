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

suite("test_create_rollup_before_report_follower", "docker") {
    if (isCloudMode()) {
        return 
    }
    def tbl = 'test_create_rollup_before_report_follower'

    def options = new ClusterOptions()
    options.setFeNum(3)
    options.setBeNum(3)
    options.enableDebugPoints()

    docker(options) {
        try {
            sql """ DROP TABLE IF EXISTS ${tbl} """
            sql """
                CREATE TABLE ${tbl} (
                `k1` int(11) NULL,
                `k2` int(11) NULL,
                `k3` int(11) NULL,
                `k4` int(11) NULL
                )
                DUPLICATE KEY(`k1`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`k1`) BUCKETS 1
                PROPERTIES ("replication_num" = "1")
                """
            def masterFe = cluster.getMasterFe()
            String masterFeHost = masterFe.host
            int masterFeHttpPort = masterFe.httpPort
            log.info("masterFeHost: ${masterFeHost}, masterFeHttpPort: ${masterFeHttpPort}")
            GetDebugPoint().enableDebugPointForAllFEs("FE.CREATE_ROLLUP_WAIT_LOG", [value: 10000])

            sql " ALTER TABLE ${tbl} ADD ROLLUP r1(k4) "

            sleep(10000)
            GetDebugPoint().enableDebugPointForAllFEs("FE.UPDATE_CATALOG_AFTER_VISIBLE", [value: 10000])

            sql "INSERT INTO ${tbl} VALUES (1, 1, 1, 1)"

            GetDebugPoint().disableDebugPointForAllFEs("FE.CREATE_ROLLUP_WAIT_LOG")

            sleep(10000)

            GetDebugPoint().disableDebugPointForAllFEs("FE.UPDATE_CATALOG_AFTER_VISIBLE")

            sleep(10000)

            def nonMasterFeIndex = cluster.getOneFollowerFe().index 
            def fe = cluster.getFeByIndex(nonMasterFeIndex)
            def feUrl = "jdbc:mysql://${fe.host}:${fe.queryPort}/?useLocalSessionState=false&allowLoadLocalInfile=false"
            feUrl = context.config.buildUrlWithDb(feUrl, context.dbName)
            connect('root', '', feUrl) {
                def res = sql """ select k1, k2, k3, k4 from ${tbl} """
                log.info("res: ${res}")
                res = sql " select k4 from ${tbl} where k4 = 1 "
                assertEquals(res.size(), 1)
            }


        } finally {
        }
    }
}
