
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
suite("test_ip_rf") {
    sql """ DROP TABLE IF EXISTS ip_test """
    sql """ DROP TABLE IF EXISTS ip_test2 """
    sql """
        CREATE TABLE ip_test (
            `id` int,
            `ip_v4` ipv4,
            `ip_v6` ipv6
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`id`) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

    sql """
        CREATE TABLE ip_test2 (
            `id` int,
            `ip_v4` ipv4,
            `ip_v6` ipv6
            ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`id`) BUCKETS 4
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
        """

    sql """
        insert into ip_test values(1, '0.0.0.0','2001:16a0:2:200a::2'),(2, '127.0.0.1','2a02:e980:83:5b09:ecb8:c669:b336:650e');
        """

    sql """
       insert into ip_test2 values(1, '0.0.0.0','2001:16a0:2:200a::2'),(2, '127.0.0.1','2a02:e980:83:5b09:ecb8:c669:b336:650e'),(3, '59.50.185.152','2001:4888:1f:e891:161:26::'),(4, '255.255.255.255','2001:4888:1f:e891:161:26::');

        """

    sql "ANALYZE TABLE ip_test WITH sync;"
    sql "ANALYZE TABLE ip_test2 WITH sync;"

    sql "set runtime_filter_type=0;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v4=b.ip_v4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v6=b.ip_v6;"
    sql "set runtime_filter_type=1;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v4=b.ip_v4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v6=b.ip_v6;"
    sql "set runtime_filter_type=2;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v4=b.ip_v4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v6=b.ip_v6;"
    sql "set runtime_filter_type=4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v4=b.ip_v4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v6=b.ip_v6;"
    sql "set runtime_filter_type=8;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v4=b.ip_v4;"
    qt_sql "select count(*) from ip_test a, ip_test2 b where a.ip_v6=b.ip_v6;"
   
}
