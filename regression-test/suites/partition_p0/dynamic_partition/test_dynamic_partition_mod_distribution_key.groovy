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

suite("test_dynamic_partition_mod_distribution_key", "docker") {
    def options = new ClusterOptions()
    options.setFeNum(2)
    docker(options) {
        // FIXME: for historical bugs, this case will fail if adding k2 as dup key or unique key
        // see in https://github.com/apache/doris/issues/39798
        def keys = ["DUPLICATE KEY (k1)", "UNIQUE KEY (k1)", "AGGREGATE KEY (k1, k2)"]
        def aggTypes = ["", "", "REPLACE"]
        for (i in 0..<3) {
            def key = keys.get(i)
            def aggType = aggTypes.get(i)
            def tableName = "test_dynamic_partition_mod_distribution_key"
            sql """ DROP TABLE IF EXISTS ${tableName} """

            sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                k1 DATE NOT NULL,
                k2 VARCHAR(20) NOT NULL,
                v INT ${aggType}
            ) ${key} 
            PARTITION BY RANGE(k1) ()
            DISTRIBUTED BY HASH(k1, k2) BUCKETS 1
            PROPERTIES (
                "dynamic_partition.enable"="true",
                "dynamic_partition.end"="3",
                "dynamic_partition.buckets"="1",
                "dynamic_partition.start"="-3",
                "dynamic_partition.prefix"="p",
                "dynamic_partition.time_unit"="DAY",
                "dynamic_partition.create_history_partition"="true",
                "dynamic_partition.replication_allocation" = "tag.location.default: 1")
            """

            sql """ alter table ${tableName} modify column k1 comment 'new_comment_for_k1' """
            sql """ alter table ${tableName} modify column k2 varchar(255) """

            cluster.restartFrontends()
            sleep(30000)
            context.reconnectFe()

            sql """ ADMIN SET FRONTEND CONFIG ('dynamic_partition_check_interval_seconds' = '1') """
            sql """ alter table ${tableName} set('dynamic_partition.end'='5') """
            result = sql "show partitions from ${tableName}"
            for (def retry = 0; retry < 10; retry++) { // at most wait 120s
                if (result.size() == 9) {
                    break;
                }
                logger.info("wait dynamic partition scheduler, sleep 1s")
                sleep(1000)  // sleep 1s
                result = sql "show partitions from ${tableName}"
            }
            assertEquals(9, result.size())
        }
    }
}
