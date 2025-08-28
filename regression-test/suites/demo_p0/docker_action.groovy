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

// Every docker suite will connect to a docker cluster.
// The docker cluster is new created and independent, not contains history data,
// not affect the external doris cluster, not affect other docker cluster.

// Run docker suite steps:
// 1. Before run docker regreesion test, make sure you can setup a doris docker cluster.
//    Read readme in [doris-compose](https://github.com/apache/doris/tree/master/docker/runtime/doris-compose)'
//    to setup a docker doris cluster;
// 2. Then run the docker suite, and setup regression-conf-custom.groovy with following config:
//    image = "xxxx"                // your doris docker image
//    excludeDockerTest = false     // do run docker suite, default is true
//    dockerEndDeleteFiles = false  // after run docker suite, whether delete contains's log and data in directory '/tmp/doris/<suite-name>'

// When run docker suite, the regression test no need to connect to an external doris cluster,
// but can still connect to one just like run a non-docker suite.
// But whether run a docker suite, need more check.
// Firstly, get the regression test's run mode (cloud or not_cloud):
// a) If the regression test connect to an external doris cluster,
//    then will use the external doris cluster's runMode(cloud or not_cloud) as the regression runMode.
// b) If there's no external doris cluster, then user can set the regression runMode with command arg `-runMode`.
//    for example:
//       `sh run-regression-test.sh --run -d demo_p0 -s docker_action  -runMode=cloud/not_cloud`
//    what's more, if the docker suite not contains 'isCloudMode()', then no need specify the command arg `-runMode`.
//    for exmaple, if the regression not connect to an external doris cluster, then command:
//      `sh run-regression-test.sh --run -d demo_p0 -s docker_action`
//    will run both cloud case and not_cloud case.
// Secondly, compare ClusterOptions.cloudMode and the regression's runMode
// a) If ClusterOptions.cloudMode = null then let ClusterOptions.cloudMode = the regression's cloudMode, and run docker suite.
// b) if ClusterOptions.cloudMode = true or false, and regreesion runMode equals equals the suite's cloudMode
//    or regression's cloudMode is unknown, then run docker suite.
//
//
//  By default, after run a docker suite, whether run succ or fail, the suite's relate docker cluster will auto destroy.
// If user don't want to destroy the docker cluster after the test, then user can specify with arg `-noKillDocker`
// for exmaple:
//   `sh run-regression-test.sh --run -d demo_p0 -s docker_action -noKillDocker`
// will run 3 docker cluster, and the last docker cluster will not destroy .

// NOTICE, for code:
// 1. Need add 'docker' to suite's group, and don't add 'nonConcurrent' to it;
// 2. No need to use code ` if (isCloudMode()) { return } `  in docker suites,
//    instead should use `ClusterOptions.cloudMode = true/false` is enough.
//    Because when run docker suite without an external doris cluster, if suite use code `isCloudMode()`, it need specific -runMode=cloud/not_cloud.
//    On the contrary, `ClusterOptions.cloudMode = true/false` no need specific -runMode=cloud/not_cloud when no external doris cluster exists.
// 3. For more options and functions usage, read the file `suite/SuiteCluster.groovy` in regression framework.

suite('docker_action', 'docker') {
    // run a new docker
    docker {
        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10'''
        sql '''insert into tb1 values (1),(2),(3)'''

        cluster.checkBeIsAlive(2, true)

        // fe and be's index start from 1, not 0.
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
