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

suite("test_insert_from_follower", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(3)
    options.setBeNum(3)
    docker(options) {
        def masterFe = cluster.getMasterFe()
        def allFes = cluster.getAllFrontends()
        def followerFes = allFes.findAll { fe -> fe.index != masterFe.index }
        def followerFe = followerFes[0]
        logger.info("Master FE: ${masterFe.host}")
        logger.info("Using follower FE: ${followerFe.host}")
        // Connect to follower FE
        def url = String.format(
                "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                followerFe.host, followerFe.queryPort)
        logger.info("Connecting to follower FE: ${url}")
        context.connectTo(url, context.config.jdbcUser, context.config.jdbcPassword)

        sql "drop database if exists test_insert_from_follower"
        sql "create database test_insert_from_follower"
        sql "use test_insert_from_follower"
        def tbl = 'test_insert_from_follower_tbl'
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sql """
          CREATE TABLE ${tbl} (
            `k1` int(11) NULL,
            `k2` char(5) NULL
          )
          DUPLICATE KEY(`k1`, `k2`)
          COMMENT 'OLAP'
          DISTRIBUTED BY HASH(`k1`) BUCKETS 2
          PROPERTIES (
            "replication_num"="3"
          );
        """
        
        def loadRes = sql """ INSERT INTO ${tbl} (k1, k2) VALUES (1, "a"), (2, "b"), (3, "c"), (4, "e");"""
        logger.info("loadRes: ${loadRes}")
        assertTrue(loadRes[0][0] == 4)
        sql """ DROP TABLE IF EXISTS ${tbl} """
        sleep(5000)
    }
}
