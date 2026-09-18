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

suite("test_cloud_colocate_consistent_hash_join", "multi_cluster,docker") {
    def options = new ClusterOptions()
    options.setFeNum(1)
    options.setBeNum(3)
    options.cloudMode = true
    options.feConfigs += [
        "enable_cloud_colocate_consistent_hash=true"
    ]

    docker(options) {
        sql "DROP TABLE IF EXISTS test_cloud_colocate_consistent_hash_join_t1"
        sql "DROP TABLE IF EXISTS test_cloud_colocate_consistent_hash_join_t2"

        sql """
            CREATE TABLE test_cloud_colocate_consistent_hash_join_t1 (
                k INT,
                v1 INT
            )
            DISTRIBUTED BY HASH(k) BUCKETS 8
            PROPERTIES (
                "replication_num" = "1",
                "colocate_with" = "test_cloud_colocate_consistent_hash_join_group"
            )
        """

        sql """
            CREATE TABLE test_cloud_colocate_consistent_hash_join_t2 (
                k INT,
                v2 INT
            )
            DISTRIBUTED BY HASH(k) BUCKETS 8
            PROPERTIES (
                "replication_num" = "1",
                "colocate_with" = "test_cloud_colocate_consistent_hash_join_group"
            )
        """

        sql "INSERT INTO test_cloud_colocate_consistent_hash_join_t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40)"
        sql "INSERT INTO test_cloud_colocate_consistent_hash_join_t2 VALUES (1, 100), (2, 200), (4, 400), (5, 500)"

        order_qt_join """
            SELECT t1.k, t1.v1, t2.v2
            FROM test_cloud_colocate_consistent_hash_join_t1 t1
            JOIN test_cloud_colocate_consistent_hash_join_t2 t2 ON t1.k = t2.k
            ORDER BY t1.k
        """
    }
}
