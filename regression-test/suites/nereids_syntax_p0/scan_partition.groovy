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

suite("test_scan_part") {
    sql """DROP TABLE IF EXISTS test_part;"""
    sql """CREATE TABLE test_part (
            id INT,
            i1 INT NOT NULL,
            i2 VARCHAR(32) NOT NULL
    ) DUPLICATE KEY(`id`)
    PARTITION BY LIST(i2)
    (
            PARTITION p1 VALUES IN('1'),
            PARTITION p2 VALUES IN('5'),
    PARTITION p3 VALUES IN('9')
    )
    DISTRIBUTED BY HASH(`id`)
    BUCKETS 3
    PROPERTIES("replication_allocation" = "tag.location.default:1");
    """
    sql """INSERT INTO test_part VALUES (1, 1, '1');"""
    sql """INSERT INTO test_part VALUES (1, 1, '1');"""
    sql """INSERT INTO test_part VALUES (1, 5, '5');"""
    sql """INSERT INTO test_part VALUES (1, 5, '5');"""
    sql """INSERT INTO test_part VALUES (2, 5, '5');"""
    sql """INSERT INTO test_part VALUES (3, 5, '1');"""
    sql """INSERT INTO test_part VALUES (4, 5, '5');"""
    sql """INSERT INTO test_part VALUES (7, 5, '9');"""
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "SET parallel_fragment_exec_instance_num=8"
    qt_sql """
            SELECT id, SUM(i1) FROM test_part WHERE i2 IN ('1','5')
           GROUP BY id HAVING id = 1;
           """
}