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

// Run docker suite steps:
// 1. Read 'docker/runtime/doris-compose/Readme.md', make sure you can setup a doris docker cluster;
// 2. update regression-conf-custom.groovy with config:
//    image = "xxxx"                // your doris docker image
//    excludeDockerTest = false     // do run docker suite, default is true
//    dockerEndDeleteFiles = false  // after run docker suite, whether delete contains's log and data in directory '/tmp/doris/<suite-name>'

// When run docker suite, then no need an external doris cluster.
// But whether run a docker suite, need more check.
// Firstly, get the pipeline's run mode (cloud or not_cloud):
// If there's an external doris cluster, then fetch pipeline's runMode from it.
// If there's no external doris cluster, then set pipeline's runMode with command args.
//    for example:  sh run-regression-test.sh --run docker_action  -runMode=cloud/not_cloud
// Secondly, compare ClusterOptions.cloudMode and pipeline's runMode
// If ClusterOptions.cloudMode = null then let ClusterOptions.cloudMode = pipeline's cloudMode, and run docker suite.
// if ClusterOptions.cloudMode = true or false, if cloudMode == pipeline's cloudMode or pipeline's cloudMode is unknown,
//      then run docker suite, otherwise don't run docker suite.

// NOTICE:
// 1. Need add 'docker' to suite's group, and don't add 'nonConcurrent' to it;
// 2. In docker closure:
//    a. Don't use 'Awaitility.await()...until(f)', but use 'dockerAwaitUntil(..., f)';
// 3. No need to use code ` if (isCloudMode()) { return } `  in docker suites,
// instead should use `ClusterOptions.cloudMode = true/false` is enough.
// Because when run docker suite without an external doris cluster, if suite use code `isCloudMode()`, it need specific -runMode=cloud/not_cloud.
// On the contrary, `ClusterOptions.cloudMode = true/false` no need specific -runMode=cloud/not_cloud when no external doris cluster exists.

suite('docker_action', 'docker') {
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
    options.feConfigs += ['example_conf_k1=v1', 'example_conf_k2=v2']
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
    // run another docker, create a cloud cluster
    docker(options2) {
        // cloud cluster will ignore replication_num, always set to 1. so create table succ even has 1 be.
        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10 properties ("replication_num"="1000")'''

        sql 'set global enable_memtable_on_sink_node = false'
        sql 'insert into tb1 values (2)'
    }
}
