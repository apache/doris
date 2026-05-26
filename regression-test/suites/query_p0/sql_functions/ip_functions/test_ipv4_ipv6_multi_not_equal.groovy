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

// Regression test for CIR-20160 / apache/doris#62672:
// Multiple != / NOT BETWEEN / NOT IN conditions on the same IPv4/IPv6
// column used to fold into the first conjunct because
// IPv4Literal/IPv6Literal.compareLiteral() always returned 0,
// making every IP literal compare-equal in legacy expr deduplication.
suite("test_ipv4_ipv6_multi_not_equal") {
    def tbl = "test_ip_multi_not_equal"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
            id  INT,
            ip4 IPV4,
            ip6 IPV6
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    sql """
        INSERT INTO ${tbl} VALUES
          (1, '1.1.1.1',     '::1'),
          (2, '1.1.1.2',     '::2'),
          (3, '1.1.1.3',     '::3'),
          (4, '2.2.2.2',     '2001::1'),
          (5, '3.3.3.3',     '2001::2'),
          (6, '10.1.1.1',    'fe80::1'),
          (7, '10.1.1.5',    'fe80::5'),
          (8, '10.1.1.10',   'fe80::a'),
          (9, '192.168.1.1', '2001:db8::1'),
          (10,'192.168.1.2', '2001:db8::2')
    """

    // -- Case 1: multiple != on IPv4 -------------------------------------
    qt_ipv4_multi_ne """
        SELECT id, ip4 FROM ${tbl}
         WHERE ip4 != '1.1.1.1' AND ip4 != '1.1.1.2'
         ORDER BY id
    """
    // EXPLAIN must retain BOTH literals; before the fix only "1.1.1.1"
    // survived because the second != predicate was dropped as duplicate.
    explain {
        sql "SELECT id FROM ${tbl} WHERE ip4 != '1.1.1.1' AND ip4 != '1.1.1.2'"
        contains "1.1.1.1"
        contains "1.1.1.2"
    }

    // -- Case 2: two NOT BETWEEN ranges on IPv4 --------------------------
    qt_ipv4_not_between """
        SELECT id, ip4 FROM ${tbl}
         WHERE NOT (ip4 BETWEEN '1.1.1.1'  AND '1.1.1.10')
           AND NOT (ip4 BETWEEN '10.1.1.1' AND '10.1.1.10')
         ORDER BY id
    """
    explain {
        sql """
            SELECT id FROM ${tbl}
             WHERE NOT (ip4 BETWEEN '1.1.1.1'  AND '1.1.1.10')
               AND NOT (ip4 BETWEEN '10.1.1.1' AND '10.1.1.10')
        """
        contains "1.1.1.1"
        contains "10.1.1.1"
    }

    // -- Case 3: multiple != on IPv6 -------------------------------------
    qt_ipv6_multi_ne """
        SELECT id, ip6 FROM ${tbl}
         WHERE ip6 != '::1' AND ip6 != '::2'
         ORDER BY id
    """
    explain {
        sql "SELECT id FROM ${tbl} WHERE ip6 != '::1' AND ip6 != '::2'"
        contains "::1"
        contains "::2"
    }

    // -- Case 4: NOT IN + != combined on IPv4 ----------------------------
    qt_ipv4_not_in_plus_ne """
        SELECT id, ip4 FROM ${tbl}
         WHERE ip4 NOT IN ('1.1.1.1','1.1.1.2') AND ip4 != '1.1.1.3'
         ORDER BY id
    """
    explain {
        sql """
            SELECT id FROM ${tbl}
             WHERE ip4 NOT IN ('1.1.1.1','1.1.1.2') AND ip4 != '1.1.1.3'
        """
        contains "1.1.1.1"
        contains "1.1.1.2"
        contains "1.1.1.3"
    }
}
