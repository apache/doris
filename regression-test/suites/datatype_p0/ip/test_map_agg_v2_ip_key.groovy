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

suite("test_map_agg_v2_ip_key") {
    sql "DROP TABLE IF EXISTS test_map_agg_v2_ip_key"
    sql """
        CREATE TABLE test_map_agg_v2_ip_key (
            id INT,
            ipv4_key IPV4,
            ipv6_key IPV6,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO test_map_agg_v2_ip_key VALUES
            (1, '192.168.0.1', '2001:db8::1', 10),
            (1, '192.168.0.2', '2001:db8::2', 20)
    """

    qt_ipv4_key """
        SELECT array_sort(map_keys(map_agg_v2(ipv4_key, value)))
        FROM test_map_agg_v2_ip_key
    """

    qt_ipv6_key """
        SELECT array_sort(map_keys(map_agg_v2(ipv6_key, value)))
        FROM test_map_agg_v2_ip_key
    """
}
