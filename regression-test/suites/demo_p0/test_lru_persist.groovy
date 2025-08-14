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

suite('test_lru_persist', 'docker') {
    def options = new ClusterOptions()
    
    options.feNum = 1
    options.beNum = 1
    options.msNum = 1
    options.cloudMode = true
    options.feConfigs += ['example_conf_k1=v1', 'example_conf_k2=v2']
    options.beConfigs += ['enable_file_cache=true', 'enable_java_support=false', 'file_cache_enter_disk_resource_limit_mode_percent=99',
                          'file_cache_background_lru_dump_interval_ms=2000', 'file_cache_background_lru_log_replay_interval_ms=500',
                          'disable_auto_compation=true', 'file_cache_enter_need_evict_cache_in_advance_percent=99',
                          'file_cache_background_lru_dump_update_cnt_threshold=0'
                        ]

    // run another docker
    docker(options) {
        cluster.checkFeIsAlive(1, true)
        cluster.checkBeIsAlive(1, true)
        sql '''set global enable_auto_analyze=false'''

        sql '''create table tb1 (k int) DISTRIBUTED BY HASH(k) BUCKETS 10 properties ("replication_num"="1")'''
        sql '''insert into tb1 values (1),(2),(3)'''
        sql '''insert into tb1 values (4),(5),(6)'''
        sql '''insert into tb1 values (7),(8),(9)'''
        sql '''insert into tb1 values (10),(11),(12)'''

        def be = cluster.getBeByIndex(1)
        def beBasePath = be.getBasePath()
        def cachePath = beBasePath + "/storage/file_cache/"

        sleep(15000);
        cluster.stopBackends(1)

        def normalBefore = "md5sum ${cachePath}/lru_dump_normal.tail".execute().text.trim().split()[0]
        logger.info("normalBefore: ${normalBefore}")

        cluster.startBackends(1)
        sleep(10000);

        cluster.stopBackends(1)

        // check md5sum again after be restart
        def normalAfter = "md5sum ${cachePath}/lru_dump_normal.tail".execute().text.trim().split()[0]
        logger.info("normalAfter: ${normalAfter}")

        assert normalBefore == normalAfter
    }
}
