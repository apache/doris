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

suite("mixed_join_key_statistics") {
    sql "DROP TABLE IF EXISTS mixed_join_key_left"
    sql "DROP TABLE IF EXISTS mixed_join_key_right"
    sql """
        CREATE TABLE mixed_join_key_left (k INT NOT NULL, app INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE mixed_join_key_right (k INT NOT NULL, app INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES("replication_num" = "1")
    """
    sql 'INSERT INTO mixed_join_key_left SELECT number % 2, 1 FROM numbers("number" = "20")'
    sql 'INSERT INTO mixed_join_key_right SELECT number % 2, 1 FROM numbers("number" = "20")'
    sql """
        ALTER TABLE mixed_join_key_left MODIFY COLUMN k SET STATS
        ('row_count'='1000', 'ndv'='10', 'min_value'='0', 'max_value'='9', 'num_nulls'='0')
    """
    sql """
        ALTER TABLE mixed_join_key_right MODIFY COLUMN k SET STATS
        ('row_count'='1000', 'ndv'='10', 'min_value'='0', 'max_value'='9', 'num_nulls'='0')
    """

    explain {
        sql """
            OPTIMIZED PLAN
            SELECT l.k FROM mixed_join_key_left l JOIN mixed_join_key_right r ON l.k = r.k AND l.app = r.app
        """
        contains "stats=100,000, type=INNER_JOIN"
    }

    order_qt_join_result """
        SELECT l.k, COUNT(*)
        FROM mixed_join_key_left l JOIN mixed_join_key_right r ON l.k = r.k AND l.app = r.app
        GROUP BY l.k
    """
}
