
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

suite("test_ip_in_predicate") {
    def tbName = "test_ip_in_predicate"
    sql """ DROP TABLE IF EXISTS ${tbName} """
    sql """
        CREATE TABLE ${tbName} (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql "insert into ${tbName} values(1, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e')"
    sql "insert into ${tbName} values(2, '42.117.228.166', '2001:16a0:2:200a::2')"
    sql "insert into ${tbName} values(3, '119.36.22.147', '2001:4888:1f:e891:161:26::')"

    qt_sql1 "select * from ${tbName} order by id"
    qt_sql2 "select * from ${tbName} where ip_v4 in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql3 "select * from ${tbName} where ip_v4 not in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql4 "select * from ${tbName} where ip_v6 in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql5 "select * from ${tbName} where ip_v6 not in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    sql "DROP TABLE ${tbName}"
}
