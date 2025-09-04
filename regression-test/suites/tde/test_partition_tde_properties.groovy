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

suite("test_partition_tde_properties", "docker") {
    def tdeAlgorithm = ["AES256", "SM4"]
    def cloudMode = [/*false,*/ true]
    cloudMode.each {mode ->
        tdeAlgorithm.each {algorithm ->
            def options = new ClusterOptions()
            options.cloudMode = mode
            options.enableDebugPoints()
            options.beNum = 3
            options.feConfigs += [
                    'cloud_cluster_check_interval_second=1',
                    'sys_log_verbose_modules=org',
                    "doris_tde_key_endpoint=${context.config.tdeKeyEndpoint}",
                    "doris_tde_key_region=${context.config.tdeKeyRegion}",
                    "doris_tde_key_provider=${context.config.tdeKeyProvider}",
                    "doris_tde_algorithm=${algorithm}",
                    "doris_tde_key_id=${context.config.tdeKeyId}"
            ]
            options.tdeAk = context.config.tdeAk
            options.tdeSk = context.config.tdeSk

            docker(options) {
                def tblName = "test_partition_tde_properties"
                sql """ DROP TABLE IF EXISTS ${tblName} """
                sql """
                CREATE TABLE IF NOT EXISTS ${tblName} (
                    `k` int NOT NULL,
                    `v` varchar(10) NOT NULL)
                UNIQUE KEY(`k`)
                PARTITION BY LIST(k) (
                    PARTITION p1 VALUES IN ("1","2","3","4")
                ) 
                DISTRIBUTED BY HASH(`k`) BUCKETS 8
                PROPERTIES (
                    "replication_allocation" = "tag.location.default: 3",
                    "enable_unique_key_merge_on_write" = "true"
                )
                """

                sql """ INSERT INTO ${tblName} VALUES(1, "2") """

                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                // assert encrypted
                def tablets = sql_return_maparray "SHOW TABLETS FROM ${tblName}"
                def backendId_to_backendIP = [:]
                def backendId_to_backendHttpPort = [:]
                getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
                for (int i=0; i<backendId_to_backendIP.size(); i++){
                    def beHttpAddress =backendId_to_backendIP.entrySet()[i].getValue()+":"+backendId_to_backendHttpPort.entrySet()[i].getValue()
                    for (tablet in tablets) {
                        def tabletId = tablet.TabletId
                        def (code, text, err) = curl("GET", beHttpAddress+ "/api/check_tablet_encryption?tablet_id=${tabletId}",
                                null/*body*/, 1000/*timeoutSec*/)
                        assertTrue(text.contains("all encrypted"))
                    }
                }


                cluster.restartFrontends()
                sleep(30000)
                context.reconnectFe()

                setFeConfig("doris_tde_algorithm", "PLAINTEXT")

                sql """ ALTER TABLE ${tblName} ADD PARTITION p2 VALUES IN ("5","6","7","8") """

                // wait

                sql """ INSERT INTO ${tblName} VALUES(5, "6") """

                qt_sql """ SELECT * FROM ${tblName} ORDER BY `k` """

                // assert encrypted
                tablets = sql_return_maparray "SHOW TABLETS FROM ${tblName}"
                backendId_to_backendIP = [:]
                backendId_to_backendHttpPort = [:]
                getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
                for (int i=0; i<backendId_to_backendIP.size(); i++){
                    def beHttpAddress =backendId_to_backendIP.entrySet()[i].getValue()+":"+backendId_to_backendHttpPort.entrySet()[i].getValue()
                    for (tablet in tablets) {
                        def tabletId = tablet.TabletId
                        def (code, text, err) = curl("GET", beHttpAddress+ "/api/check_tablet_encryption?tablet_id=${tabletId}",
                                null/*body*/, 1000/*timeoutSec*/)
                        assertTrue(text.contains("all encrypted"))
                    }
                }
            }
        }
    }
}
