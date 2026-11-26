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

// In cloud mode, BUILD INDEX ON PARTITION builds the index only for the named partitions.
suite("test_cloud_build_index_on_partition") {
    if (!isCloudMode()) {
        return
    }

    def timeout = 60000

    // With a light CREATE INDEX, rows loaded before the index get it only from BUILD INDEX.
    sql "set enable_add_index_for_new_data = true"
    sql "set enable_profile = true"
    sql "set profile_level = 2"

    sql "DROP TABLE IF EXISTS test_cloud_build_index_on_partition"
    sql """
        CREATE TABLE test_cloud_build_index_on_partition (
            k1 int NOT NULL,
            v1 varchar(50) NOT NULL
        )
        DUPLICATE KEY(`k1`)
        PARTITION BY RANGE(`k1`) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    """
    sql """
        INSERT INTO test_cloud_build_index_on_partition VALUES
        (1, 'aaaaa'), (2, 'bbbbb'), (3, 'ccccc'),
        (101, 'aaaaa'), (102, 'bbbbb'), (103, 'ccccc')
    """

    def p1_query = "SELECT * FROM test_cloud_build_index_on_partition WHERE k1 < 100 AND v1 = 'aaaaa'"
    def p2_query = "SELECT * FROM test_cloud_build_index_on_partition WHERE k1 >= 100 AND v1 = 'aaaaa'"

    sql "CREATE INDEX idx_v1 ON test_cloud_build_index_on_partition(v1) USING INVERTED"
    check_inverted_index_filter_rows(p1_query, 0)
    check_inverted_index_filter_rows(p2_query, 0)

    // SHOW BUILD INDEX also lists jobs of earlier tables with this name, so compare counts.
    def show_p2_jobs = """SHOW BUILD INDEX WHERE TableName = "test_cloud_build_index_on_partition" AND PartitionName = "p2";"""
    def p2_job_count = sql(show_p2_jobs).size()

    // Each partition holds three rows and one match, so a built index filters two rows.
    sql "BUILD INDEX ON test_cloud_build_index_on_partition PARTITION (p1)"
    wait_for_last_build_index_finish("test_cloud_build_index_on_partition", timeout)
    assertEquals(p2_job_count, sql(show_p2_jobs).size())
    check_inverted_index_filter_rows(p1_query, 2)
    check_inverted_index_filter_rows(p2_query, 0)

    sql "BUILD INDEX ON test_cloud_build_index_on_partition PARTITION (p2)"
    wait_for_last_build_index_finish("test_cloud_build_index_on_partition", timeout)
    check_inverted_index_filter_rows(p1_query, 2)
    check_inverted_index_filter_rows(p2_query, 2)
}
