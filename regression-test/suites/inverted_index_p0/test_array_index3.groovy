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

suite("test_array_index3") {
    def tableName = "array_test_ip"

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time) {
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    sql "DROP TABLE IF EXISTS ${tableName}"

    // Create table with IP array types
    sql """
        CREATE TABLE ${tableName} (
            id int,
            ipv4_arr ARRAY<IPV4>,
            ipv6_arr ARRAY<IPV6>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Insert test data with IPv4 and IPv6 addresses
    sql """ INSERT INTO ${tableName} VALUES
        (1, ['192.168.1.1', '10.0.0.1', '172.16.0.1'], ['2001:db8::1', '2001:db8::2', 'fe80::1']),
        (2, ['192.168.1.2', '10.0.0.2'], ['2001:db8::3', '2001:db8::4', 'fe80::2']),
        (3, NULL, NULL),
        (4, [], []),
        (5, ['192.168.1.100', '192.168.1.101'], ['2001:db8::100', '2001:db8::101']),
        (6, ['8.8.8.8', '8.8.4.4'], ['2001:4860:4860::8888', '2001:4860:4860::8844']);
    """

    // Create indexes on IP array types - should succeed
    sql """ ALTER TABLE ${tableName} ADD INDEX idx_ipv4_arr (ipv4_arr) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    sql """ ALTER TABLE ${tableName} ADD INDEX idx_ipv6_arr (ipv6_arr) USING INVERTED; """
    wait_for_latest_op_on_table_finish(tableName, timeout)

    // Test array_contains with IPv4 addresses
    qt_sql """
        SELECT id, ipv4_arr
        FROM ${tableName}
        WHERE array_contains(ipv4_arr, '192.168.1.1')
            OR array_contains(ipv4_arr, '8.8.8.8')
        ORDER BY id;
    """

    // Test array_contains with IPv6 addresses
    qt_sql """
        SELECT id, ipv6_arr
        FROM ${tableName}
        WHERE array_contains(ipv6_arr, '2001:db8::1')
            OR array_contains(ipv6_arr, '2001:4860:4860::8888')
        ORDER BY id;
    """

    // Test array_contains with multiple IP conditions
    qt_sql """
        SELECT id
        FROM ${tableName}
        WHERE array_contains(ipv4_arr, '192.168.1.1')
            AND array_contains(ipv6_arr, '2001:db8::1')
        ORDER BY id;
    """

    // Test array_contains with NULL and empty IP arrays
    qt_sql """
        SELECT id, ipv4_arr
        FROM ${tableName}
        WHERE array_contains(ipv4_arr, '192.168.1.1')
            OR ipv4_arr IS NULL
            OR ipv4_arr = []
        ORDER BY id;
    """

    // Test array_contains with non-existent IP addresses
    qt_sql """
        SELECT id, ipv4_arr
        FROM ${tableName}
        WHERE array_contains(ipv4_arr, '192.168.1.999')
        ORDER BY id;
    """

}
