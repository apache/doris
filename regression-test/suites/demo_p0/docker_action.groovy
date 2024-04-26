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

suite('docker_action') {
    // run a new docker
    docker {
        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10'''
        sql '''insert into tb1 values (1),(2),(3)'''

        cluster.checkBeIsAlive(2, true)

        // stop backend 2, 3
        cluster.stopBackends(2, 3)

        cluster.checkBeIsAlive(2, false)

        test {
            sql '''insert into tb1 values (4),(5),(6)'''

            // REPLICA_FEW_ERR
            exception 'errCode = 3,'
        }
    }

    def options = new ClusterOptions()
    // add fe config items
    options.feConfigs = ['example_conf_k1=v1', 'example_conf_k2=v2']
    // contains 5 backends
    options.beNum = 5
    // each backend has 1 HDD disk and 3 SSD disks
    options.beDisks = ['HDD=1', 'SSD=3']

    // run another docker
    docker(options) {
        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10 properties ("replication_num"="5")'''
    }

    def options2 = new ClusterOptions()
    options2.beNum = 1
    // create cloud cluster
    options2.cloudMode = true
    //// cloud docker only run in cloud pipeline, but enable it run in none-cloud pipeline
    // options2.skipRunWhenPipelineDiff = false
    // run another docker, create a cloud cluster
    docker(options2) {
        // cloud cluster will ignore replication_num, always set to 1. so create table succ even has 1 be.
        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10 properties ("replication_num"="1000")'''

        sql 'set global enable_memtable_on_sink_node = false'
        sql 'insert into tb1 values (2)'
    }
}
