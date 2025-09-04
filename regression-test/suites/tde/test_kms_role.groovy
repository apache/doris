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
import groovy.json.JsonSlurper
import groovy.json.JsonOutput

suite('test_kms_role', 'docker') {
    def options = new ClusterOptions()
    String tableName_1 = "test_kms_aksk_1"
    String tableName_2 = "test_kms_aksk_2"

    logger.info("ms1 addr={}, port={}, ms endpoint={}", context.config.tdeKeyEndpoint)

    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        "doris_tde_key_endpoint=${context.config.tdeKeyEndpoint}",
        "doris_tde_key_region=${context.config.tdeKeyRegion}",
        "doris_tde_key_provider=${context.config.tdeKeyProvider}",
        "doris_tde_algorithm=${context.config.tdeAlgorithm}",
    ]
    options.feNum = 2
    options.beNum = 1
    options.cloudMode = true
    options.connectToFollower = false
    //options.tdeAk = context.config.tdeAk
    //options.tdeSk = context.config.tdeSk

    for (def j = 0; j < 2; j++) {
        docker(options) {
            sql """
               CREATE TABLE ${tableName_1}
               (
                   `id` BIGINT,
                   `deleted` TINYINT,
                   `type` String,
                   `author` String,
                   `comment` String,
                   INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment'
               )
               unique KEY(`id`)
               DISTRIBUTED BY HASH(`id`) BUCKETS 10
               PROPERTIES ("replication_num" = "1");
            """
            sql """
               insert into ${tableName_1} values (1, 1, 'a', 'b', 'c');
            """

            sql """
               select * from ${tableName_1};
            """

            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()

            sql """
               select * from ${tableName_1};
            """

            sql """
               CREATE TABLE ${tableName_2}
               (
                   `id` BIGINT,
                   `deleted` TINYINT,
                   `type` String,
                   `author` String,
                   `comment` String,
                   INDEX idx_comment (`comment`) USING INVERTED PROPERTIES("parser" = "english") COMMENT 'inverted index for comment'
               )
               unique KEY(`id`)
               DISTRIBUTED BY HASH(`id`) BUCKETS 10
               PROPERTIES ("replication_num" = "1");
            """

            sql """
               insert into ${tableName_2} values (1, 1, 'a', 'b', 'c');
            """

            sql """
               select * from ${tableName_2};
            """
        }
        // connect to follower, run again
        options.connectToFollower = true
    }
}
