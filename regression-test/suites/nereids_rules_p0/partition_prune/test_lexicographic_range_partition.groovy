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

suite("test_lexicographic_range_partition") {
    sql "DROP TABLE IF EXISTS lexicographic_range_partition"
    sql """
        CREATE TABLE lexicographic_range_partition (
            k1 INT NOT NULL,
            k2 INT NOT NULL,
            k3 INT NOT NULL,
            v INT NOT NULL
        )
        DUPLICATE KEY(k1, k2, k3)
        PARTITION BY RANGE(k1, k2, k3) (
            PARTITION p_target VALUES [("1", "10", "100"), ("100", "20", "200"))
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO lexicographic_range_partition VALUES
            (1, 11, 50, 1),
            (100, 19, 250, 2),
            (100, 20, 199, 3),
            (1, 10, 100, 4)
    """

    def assertTargetPartition = { String query ->
        explain {
            sql query
            contains "partitions=1/1 (p_target)"
        }
    }

    sql "SET partition_pruning_expand_threshold=200"
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=1 AND k2=11 AND k3=50")
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=19 AND k3=250")
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=20 AND k3=199")
    qt_expanded_lower_suffix "SELECT v FROM lexicographic_range_partition WHERE k1=1 AND k2=11 AND k3=50"
    qt_expanded_upper_suffix "SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=19 AND k3=250"
    qt_expanded_upper_equal_prefix "SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=20 AND k3=199"

    sql "SET partition_pruning_expand_threshold=1"
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=1 AND k2=11 AND k3=50")
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=19 AND k3=250")
    assertTargetPartition("SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=20 AND k3=199")
    qt_unexpanded_lower_suffix "SELECT v FROM lexicographic_range_partition WHERE k1=1 AND k2=11 AND k3=50"
    qt_unexpanded_upper_suffix "SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=19 AND k3=250"
    qt_unexpanded_upper_equal_prefix "SELECT v FROM lexicographic_range_partition WHERE k1=100 AND k2=20 AND k3=199"
}
