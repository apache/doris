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

// Regression for issue #62672: on IPv4/IPv6 columns, multiple `!=` (or NOT)
// predicates on the same column were wrongly de-duplicated, so only the first
// condition took effect. The root cause was that legacy IPv4Literal/IPv6Literal
// returned 0 from compareLiteral() regardless of value, making any two IP
// literals compare as equal.
suite("test_ip_neq_predicate") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    def tbl = "test_ip_neq_predicate"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql """
        CREATE TABLE ${tbl} (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql "insert into ${tbl} values(1, '1.1.1.1', '::ffff:1.1.1.1')"
    sql "insert into ${tbl} values(2, '1.1.1.2', '::ffff:1.1.1.2')"
    sql "insert into ${tbl} values(3, '1.1.1.3', '::ffff:1.1.1.3')"

    // sanity: a single `!=` works
    test {
        sql "select id from ${tbl} where ip_v4 != '1.1.1.1' order by id"
        result([[2], [3]])
    }

    // issue #62672: multiple `!=` on the same IPv4 column must all take effect
    test {
        sql "select id from ${tbl} where ip_v4 != '1.1.1.1' and ip_v4 != '1.1.1.2' order by id"
        result([[3]])
    }

    // issue #62672: multiple `!=` on the same IPv6 column must all take effect
    test {
        sql """select id from ${tbl}
               where ip_v6 != '::ffff:1.1.1.1' and ip_v6 != '::ffff:1.1.1.2' order by id"""
        result([[3]])
    }

    // multiple NOT BETWEEN on the same IPv4 column
    test {
        sql """select id from ${tbl}
               where not (ip_v4 between '1.1.1.1' and '1.1.1.1')
                 and not (ip_v4 between '1.1.1.2' and '1.1.1.2') order by id"""
        result([[3]])
    }

    // multiple NOT BETWEEN on the same IPv6 column
    test {
        sql """select id from ${tbl}
               where not (ip_v6 between '::ffff:1.1.1.1' and '::ffff:1.1.1.1')
                 and not (ip_v6 between '::ffff:1.1.1.2' and '::ffff:1.1.1.2') order by id"""
        result([[3]])
    }

    // both `!=` predicates must survive planning, not just the first one
    explain {
        sql "select id from ${tbl} where ip_v4 != '1.1.1.1' and ip_v4 != '1.1.1.2'"
        contains("1.1.1.1")
        contains("1.1.1.2")
    }

    sql "DROP TABLE IF EXISTS ${tbl}"
}
