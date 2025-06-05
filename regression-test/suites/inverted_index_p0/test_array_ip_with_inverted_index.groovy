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

suite("test_array_ip_with_inverted_index") {
    def tbl = "tai_ip_test"
    sql "DROP TABLE IF EXISTS ${tbl}"

    sql """
        CREATE TABLE IF NOT EXISTS ${tbl} (
            id int,
            ips_v4 array<ipv4> NULL,
            ips_v6 array<ipv6> NULL,
            ipv4_1 ipv4 NULL,
            ipv4_2 ipv4 NULL,
            ipv6_1 ipv6 NULL,
            ipv6_2 ipv6 NULL,
            INDEX idx_ips_v4(ips_v4) USING INVERTED COMMENT '',
            INDEX idx_ips_v6(ips_v6) USING INVERTED COMMENT '',
            INDEX idx_ipv4_1(ipv4_1) USING INVERTED COMMENT '',
            INDEX idx_ipv6_1(ipv6_1) USING INVERTED COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // insert some data
    sql """ INSERT INTO ${tbl} VALUES (1, ['192.168.0.1', '10.0.0.1'], ['2001:db8::1', 'fe80::1'], '192.168.0.1', '10.0.0.1', '2001:db8::1', 'fe80::1'); """
    sql """ INSERT INTO ${tbl} VALUES (2, ['172.16.0.1'], ['::1'], '172.16.0.1', '1.1.1.1', '::1', '::2'); """
    sql """ INSERT INTO ${tbl} VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL); """
    sql """ INSERT INTO ${tbl} VALUES (4, ['1.1.1.1'], ['2001:db8::2'], '1.1.1.1', NULL, '2001:db8::2', NULL); """

    // for array<ipv4> 和 array<ipv6> query array_contains/arrays_overlap
    def ipv4_test_values = ["'192.168.0.1'", "'10.0.0.1'", "'1.1.1.1'", null]
    for (def p : ipv4_test_values) {
        order_qt_sql """ select * from ${tbl} where array_contains(ips_v4, ${p}) order by id; """
        order_qt_sql """ select * from ${tbl} where !array_contains(ips_v4, ${p}) order by id; """
    }
    def ipv6_test_values = ["'2001:db8::1'", "'fe80::1'", "'::1'", "'2001:db8::2'", null]
    for (def p : ipv6_test_values) {
        order_qt_sql """ select * from ${tbl} where array_contains(ips_v6, ${p}) order by id; """
        order_qt_sql """ select * from ${tbl} where !array_contains(ips_v6, ${p}) order by id; """
    }
    def ipv4_overlap = ["['192.168.0.1','10.0.0.1']", "['1.1.1.1']", "[]", null]
    for (def p : ipv4_overlap) {
        order_qt_sql """ select * from ${tbl} where arrays_overlap(ips_v4, ${p}) order by id; """
        order_qt_sql """ select * from ${tbl} where !arrays_overlap(ips_v4, ${p}) order by id; """
    }
    def ipv6_overlap = ["['2001:db8::1','fe80::1']", "['::1']", "[]", null]
    for (def p : ipv6_overlap) {
        order_qt_sql """ select * from ${tbl} where arrays_overlap(ips_v6, ${p}) order by id; """
        order_qt_sql """ select * from ${tbl} where !arrays_overlap(ips_v6, ${p}) order by id; """
    }

    // for ipv4_1/ipv4_2/ipv6_1/ipv6_2 query =、IN、IS NULL
    order_qt_sql """ select * from ${tbl} where ipv4_1 = '192.168.0.1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv4_2 = '10.0.0.1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv6_1 = '2001:db8::1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv6_2 = 'fe80::1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv4_1 in ('192.168.0.1', '1.1.1.1') order by id; """
    order_qt_sql """ select * from ${tbl} where ipv6_2 is null order by id; """

    // alter for inverted index
    sql """ ALTER TABLE ${tbl} ADD INDEX idx_ipv4_2(ipv4_2) USING INVERTED COMMENT ''; """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }
    sql """ ALTER TABLE ${tbl} ADD INDEX idx_ipv6_2(ipv6_2) USING INVERTED COMMENT ''; """
    waitForSchemaChangeDone {
        sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1 """
        time 600
    }


    order_qt_sql """ select * from ${tbl} where ipv4_2 = '10.0.0.1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv4_2 in ('10.0.0.1', '1.1.1.1') order by id; """
    order_qt_sql """ select * from ${tbl} where ipv6_2 = 'fe80::1' order by id; """
    order_qt_sql """ select * from ${tbl} where ipv6_2 is null order by id; """
}

