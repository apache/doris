
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

suite("test_ip_crud") {
    sql """ DROP TABLE IF EXISTS test_unique_ip_crud """
    sql """ DROP TABLE IF EXISTS test_dup_ip_crud """

    // test ip datatype in unique table
    sql """
        CREATE TABLE test_unique_ip_crud (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        UNIQUE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into test_unique_ip_crud values(1, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e')"
    sql "insert into test_unique_ip_crud values(2, '42.117.228.166', '2001:16a0:2:200a::2')"
    sql "insert into test_unique_ip_crud values(3, '119.36.22.147', '2001:4888:1f:e891:161:26::')"

    qt_sql1 "select * from test_unique_ip_crud order by id"
    qt_sql2 "select * from test_unique_ip_crud where ip_v4='42.117.228.166'"
    qt_sql3 "select * from test_unique_ip_crud where ip_v6='2a02:e980:83:5b09:ecb8:c669:b336:650e'"
    qt_sql4 "select * from test_unique_ip_crud where ip_v4='119.36.22.147' and ip_v6='2001:4888:1f:e891:161:26::'"

    // Only unique table could be updated
    sql "update test_unique_ip_crud set ip_v4=0, ip_v6='2804:64:0:25::1' where id=3"
    qt_sql5 "select * from test_unique_ip_crud order by id"

    // test ip datatype in aggregate table
    sql """
        CREATE TABLE test_agg_ip_crud (
          `id` int,
          `ip_v4` ipv4 REPLACE,
          `ip_v6` ipv6 REPLACE
        ) ENGINE=OLAP
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into test_agg_ip_crud values(1, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e')"
    sql "insert into test_agg_ip_crud values(2, '42.117.228.166', '2001:16a0:2:200a::2')"
    sql "insert into test_agg_ip_crud values(3, '119.36.22.147', '2001:4888:1f:e891:161:26::')"

    qt_sql6 "select * from test_agg_ip_crud order by id"
    qt_sql7 "select * from test_agg_ip_crud where ip_v4='42.117.228.166'"
    qt_sql8 "select * from test_agg_ip_crud where ip_v6='2a02:e980:83:5b09:ecb8:c669:b336:650e'"
    qt_sql9 "select * from test_agg_ip_crud where ip_v4='119.36.22.147' and ip_v6='2001:4888:1f:e891:161:26::'"

    // test ip datatype in duplicate table
    sql """
        CREATE TABLE test_dup_ip_crud (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql "insert into test_dup_ip_crud values(1, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e')"
    sql "insert into test_dup_ip_crud values(2, '42.117.228.166', '2001:16a0:2:200a::2')"
    sql "insert into test_dup_ip_crud values(3, '119.36.22.147', '2001:4888:1f:e891:161:26::')"

    qt_sql10 "select * from test_dup_ip_crud order by id"
    qt_sql11 "select * from test_dup_ip_crud where ip_v4='42.117.228.166'"
    qt_sql12 "select * from test_dup_ip_crud where ip_v6='2a02:e980:83:5b09:ecb8:c669:b336:650e'"
    qt_sql13 "select * from test_dup_ip_crud where ip_v4='119.36.22.147' and ip_v6='2001:4888:1f:e891:161:26::'"

    sql "delete from test_dup_ip_crud where ip_v4='42.117.228.166'"
    qt_sql14 "select * from test_dup_ip_crud order by id"
    sql "delete from test_dup_ip_crud where ip_v6='2001:4888:1f:e891:161:26::'"
    qt_sql15 "select * from test_dup_ip_crud order by id"

    sql "DROP TABLE test_unique_ip_crud"
    sql "DROP TABLE test_agg_ip_crud"
    sql "DROP TABLE test_dup_ip_crud"
}
