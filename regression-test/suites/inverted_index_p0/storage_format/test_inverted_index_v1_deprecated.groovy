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

suite("test_inverted_index_v1_deprecated", "p0") {
    sql "set enable_add_index_for_new_data = true"

    // explicit v1 (lowercase) must be rejected
    test {
        sql """
            CREATE TABLE test_v1_rejected (
              k INT,
              v STRING,
              INDEX idx_v (v) USING INVERTED
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "v1"
            )
        """
        exception "Inverted index V1 is deprecated and no longer allowed for new index creation."
    }

    // uppercase V1 must also be rejected
    test {
        sql """
            CREATE TABLE test_v1_rejected_upper (
              k INT,
              v STRING,
              INDEX idx_v (v) USING INVERTED
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1",
              "inverted_index_storage_format" = "V1"
            )
        """
        exception "Inverted index V1 is deprecated and no longer allowed for new index creation."
    }

    // no format specified -> default succeeds and format is not V1
    // ALTER TABLE ADD INDEX on default table succeeds
    try {
        sql "DROP TABLE IF EXISTS test_v1_deprecated_default"
        sql """
            CREATE TABLE test_v1_deprecated_default (
              k INT,
              v STRING
            ) ENGINE=OLAP DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
              "replication_allocation" = "tag.location.default: 1"
            )
        """
        def showCreate = sql "SHOW CREATE TABLE test_v1_deprecated_default"
        assertTrue(showCreate.size() > 0)
        assertFalse(showCreate[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))

        sql "ALTER TABLE test_v1_deprecated_default ADD INDEX idx_v (v) USING INVERTED"
        def showCreateAfter = sql "SHOW CREATE TABLE test_v1_deprecated_default"
        assertTrue(showCreateAfter[0][1].contains("idx_v"))
        assertFalse(showCreateAfter[0][1].contains("\"inverted_index_storage_format\" = \"V1\""))
    } finally {
        sql "DROP TABLE IF EXISTS test_v1_deprecated_default"
    }
}
