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

suite("test_score_on_agg_mor_table", "p0") {
    sql "DROP TABLE IF EXISTS test_score_on_agg_table"
    sql "DROP TABLE IF EXISTS test_score_on_mor_table"

    sql """
        CREATE TABLE test_score_on_agg_table (
            k1 INT,
            content VARCHAR(255),
            v1 INT SUM,
            INDEX idx_content (content) USING INVERTED
        )
        AGGREGATE KEY(k1, content)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE test_score_on_mor_table (
            k1 INT,
            content VARCHAR(255),
            INDEX idx_content (content) USING INVERTED
        )
        UNIQUE KEY(k1, content)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        )
    """

    test {
        sql """
            SELECT score() AS s FROM test_score_on_agg_table
            WHERE content MATCH 'doris' ORDER BY s LIMIT 10
        """
        exception "score() function is not supported on AGG_KEYS table or merge-on-read UNIQUE_KEYS table"
    }

    test {
        sql """
            SELECT score() AS s FROM test_score_on_mor_table
            WHERE content MATCH 'doris' ORDER BY s LIMIT 10
        """
        exception "score() function is not supported on AGG_KEYS table or merge-on-read UNIQUE_KEYS table"
    }
}
