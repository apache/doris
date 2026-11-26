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

suite("test_ip_cidr_not_null", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_ip_cidr_not_null"
    sql """
        CREATE TABLE test_ip_cidr_not_null (
            id INT NOT NULL,
            address IPV4 NULL,
            INDEX address_index (address) USING INVERTED
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_ip_cidr_not_null VALUES
            (1, '192.168.1.1'),
            (2, NULL),
            (3, '192.168.2.1'),
            (4, '192.168.1.255')
    """

    sql "SET debug_skip_fold_constant = true"
    sql "SET inverted_index_skip_threshold = 0"
    sql "SET enable_segment_limit_pushdown = true"

    sql "SET enable_inverted_index_query = false"
    qt_sql_without_inverted_index """
        SELECT id
        FROM test_ip_cidr_not_null
        WHERE NOT is_ip_address_in_range(address, '192.168.1.0/24')
        ORDER BY id
    """

    sql "SET enable_inverted_index_query = true"
    qt_sql_with_inverted_index """
        SELECT id
        FROM test_ip_cidr_not_null
        WHERE NOT is_ip_address_in_range(address, '192.168.1.0/24')
        ORDER BY id
    """
    qt_sql_with_inverted_index_repeat """
        SELECT id
        FROM test_ip_cidr_not_null
        WHERE NOT is_ip_address_in_range(address, '192.168.1.0/24')
        ORDER BY id
    """
}
