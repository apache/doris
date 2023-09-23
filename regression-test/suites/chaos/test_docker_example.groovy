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

suite("test_docker_example") {
    docker {
        sql """create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10"""
        sql """insert into tb1 values (1),(2),(3)"""

        cluster.checkBeIsAlive(2, true)

        // stop backend 2, 3
        cluster.stopBackends(2, 3)
        // wait be lost heartbeat
        Thread.sleep(6000)

        cluster.checkBeIsAlive(2, false)

        test {
            sql """insert into tb1 values (4),(5),(6)"""

            // REPLICA_FEW_ERR
            exception "errCode = 3,"
        }
    }

    // run another docker
    def options = new ClusterOptions()
    // add fe config items
    options.feConfigs = ["example_conf_k1=v1", "example_conf_k2=v2"]
    // contains 5 backends
    options.beNum = 5
    // each backend has 3 disks
    options.beDiskNum = 3
    docker (options) {
        sql """create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10 properties ("replication_num"="5")"""
    }
}
