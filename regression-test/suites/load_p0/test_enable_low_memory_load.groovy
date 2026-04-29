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

suite("test_enable_low_memory_load", "load_p0") {
    sql "DROP TABLE IF EXISTS test_enable_low_memory_load"
    sql "DROP TABLE IF EXISTS test_enable_low_memory_load_with_key"

    sql """
        CREATE TABLE test_enable_low_memory_load (
            k1 INT,
            v1 VARCHAR(20)
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_duplicate_without_keys_by_default" = "true",
            "enable_low_memory_load" = "true"
        )
    """

    def result = sql "SHOW CREATE TABLE test_enable_low_memory_load"
    assertTrue(result[0][1].contains('"enable_low_memory_load" = "true"'))

    sql "INSERT INTO test_enable_low_memory_load VALUES (2, 'b'), (1, 'a')"
    result = sql "SELECT * FROM test_enable_low_memory_load ORDER BY k1"
    assertEquals([[1, 'a'], [2, 'b']], result)

    test {
        sql """
            CREATE TABLE test_enable_low_memory_load_with_key (
                k1 INT,
                v1 VARCHAR(20)
            ) ENGINE=OLAP
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_low_memory_load" = "true"
            )
        """
        exception "only supports duplicate key tables without key columns"
    }
}
