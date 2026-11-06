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

suite("test_cloud_values_index_partition_lifecycle", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    docker(options) {
        setFeConfig("enable_partition_inverted_index_storage_format_rollout", "true")
        sql "SET enable_add_index_for_new_data = true"
        sql "DROP TABLE IF EXISTS test_cloud_values_index_partition_lifecycle"
        sql """
            CREATE TABLE test_cloud_values_index_partition_lifecycle (
                id INT,
                v VARIANT
            ) DUPLICATE KEY(id)
            PARTITION BY RANGE(id) (PARTITION p_old VALUES LESS THAN (100))
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1", "inverted_index_storage_format"="V2")
        """
        sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                SET ("partition.inverted_index_storage_format"="SNII")"""
        sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                ADD PARTITION p_live VALUES LESS THAN (200)"""
        test {
            sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                    ADD INDEX root_idx(v) USING INVERTED
                    PROPERTIES("variant_index_scope"="values", "parser"="english")"""
            exception "requires SNII for all existing and future partitions"
        }
        sql "ALTER TABLE test_cloud_values_index_partition_lifecycle DROP PARTITION p_old"

        // The table property stays V2; all live and future partitions are now SNII.
        sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                ADD INDEX root_idx(v) USING INVERTED
                PROPERTIES("variant_index_scope"="values", "parser"="english")"""
        test {
            sql "RECOVER PARTITION p_old FROM test_cloud_values_index_partition_lifecycle"
            exception "requires SNII"
        }
        sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                ADD PARTITION p_keep VALUES LESS THAN (300)"""
        sql "ALTER TABLE test_cloud_values_index_partition_lifecycle DROP PARTITION p_live"
        sql "RECOVER PARTITION p_live FROM test_cloud_values_index_partition_lifecycle"

        sql "DROP INDEX root_idx ON test_cloud_values_index_partition_lifecycle"
        sql """ALTER TABLE test_cloud_values_index_partition_lifecycle
                ADD INDEX root_idx(v) USING INVERTED
                PROPERTIES("variant_index_scope"="values", "parser"="english")"""
        // Rejection must leave the old partition in the recycle bin, including after re-ADD.
        test {
            sql "RECOVER PARTITION p_old FROM test_cloud_values_index_partition_lifecycle"
            exception "requires SNII"
        }
    }
}
