
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

suite("test_ip_in_inverted_index") {
    def tbName = "test_ip_with_inverted_index"
    sql """ DROP TABLE IF EXISTS test_ip_with_inverted_index """

    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_ip_with_inverted_index (
          `id` int,
          `ip_v4` ipv4,
          `ip_v6` ipv6,
          INDEX v4_index (`ip_v4`) USING INVERTED COMMENT '',
          INDEX v6_index (`ip_v6`) USING INVERTED COMMENT '',
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        """

    sql """insert into test_ip_with_inverted_index values(111, '59.50.185.152', '2a02:e980:83:5b09:ecb8:c669:b336:650e'),(113, '119.36.22.147', '2001:4888:1f:e891:161:26::')"""
    sql "insert into test_ip_with_inverted_index values(112, '42.117.228.166', '2001:16a0:2:200a::2')"
    
    // query with inverted index and no inverted index
    sql """set enable_inverted_index_query=true; """
    qt_sql_index_1 "select * from test_ip_with_inverted_index where ip_v4 = '59.50.185.152' order by id;"
    qt_sql_index_2 "select * from test_ip_with_inverted_index where ip_v4 in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql_index_3 "select * from test_ip_with_inverted_index where ip_v4 not in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql_index_4 "select * from test_ip_with_inverted_index where ip_v6 = '2001:16a0:2:200a::2' order by id;"
    qt_sql_index_4 "select * from test_ip_with_inverted_index where ip_v6 in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql_index_5 "select * from test_ip_with_inverted_index where ip_v6 not in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    // empty set query
    qt_sql_index_6 "select * from test_ip_with_inverted_index where ip_v4 = '36.186.75.121' order by id;"
    qt_sql_index_7 "select * from test_ip_with_inverted_index where ip_v4 in ('61.58.96.173', '36.186.75.121') order by id;"
    qt_sql_index_8 "select * from test_ip_with_inverted_index where ip_v6 = '2a01:b740:a09::1' order by id;"
    qt_sql_index_9 "select * from test_ip_with_inverted_index where ip_v6 in ('2a01:b740:a09::1', '2001:4888:1f:e891:161:26::') order by id"
    // comparable query
    qt_sql_index_10 "select * from test_ip_with_inverted_index where ip_v4 < '59.50.185.152' order by id;"
    qt_sql_index_11 "select * from test_ip_with_inverted_index where ip_v4 >= '119.36.22.147' order by id;"
    qt_sql_index_12 "select * from test_ip_with_inverted_index where ip_v6 < '2001:16a0:2:200a::2' order by id;"
    qt_sql_index_13 "select * from test_ip_with_inverted_index where ip_v6 >= '2001:4888:1f:e891:161:26::' order by id;"

    sql """set enable_inverted_index_query=false; """
    qt_sql1 "select * from test_ip_with_inverted_index where ip_v4 = '59.50.185.152' order by id;"
    qt_sql2 "select * from test_ip_with_inverted_index where ip_v4 in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql3 "select * from test_ip_with_inverted_index where ip_v4 not in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql4 "select * from test_ip_with_inverted_index where ip_v6 = '2001:16a0:2:200a::2' order by id;"
    qt_sql4 "select * from test_ip_with_inverted_index where ip_v6 in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql5 "select * from test_ip_with_inverted_index where ip_v6 not in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    // empty set query
    qt_sql6 "select * from test_ip_with_inverted_index where ip_v4 = '36.186.75.121' order by id;"
    qt_sql7 "select * from test_ip_with_inverted_index where ip_v4 in ('61.58.96.173', '36.186.75.121') order by id;"
    qt_sql8 "select * from test_ip_with_inverted_index where ip_v6 = '2a01:b740:a09::1' order by id;"
    qt_sql9 "select * from test_ip_with_inverted_index where ip_v6 in ('2a01:b740:a09::1', '2001:4888:1f:e891:161:26::') order by id"
    // comparable query
    qt_sql10 "select * from test_ip_with_inverted_index where ip_v4 < '59.50.185.152' order by id;"
    qt_sql11 "select * from test_ip_with_inverted_index where ip_v4 >= '119.36.22.147' order by id;"
    qt_sql12 "select * from test_ip_with_inverted_index where ip_v6 < '2001:16a0:2:200a::2' order by id;"
    qt_sql13 "select * from test_ip_with_inverted_index where ip_v6 >= '2001:4888:1f:e891:161:26::' order by id;"


    // with stream_load data
    streamLoad {
            table tbName
            set 'column_separator', ','
            file 'test_data/test.csv'
            time 10000 // limit inflight 10s
            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows
    }
    qt_sql "select count() from test_ip_with_inverted_index"
    // query with inverted index and no inverted index
    sql """set enable_inverted_index_query=true; """
    qt_sql_index_1 "select * from test_ip_with_inverted_index where ip_v4 = '59.50.185.152' order by id;"
    qt_sql_index_2 "select * from test_ip_with_inverted_index where ip_v4 in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql_index_3 "select * from test_ip_with_inverted_index where ip_v4 not in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql_index_4 "select * from test_ip_with_inverted_index where ip_v6 = '2001:16a0:2:200a::2' order by id;"
    qt_sql_index_4 "select * from test_ip_with_inverted_index where ip_v6 in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql_index_5 "select * from test_ip_with_inverted_index where ip_v6 not in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql_index_6 "select * from test_ip_with_inverted_index where ip_v4 = '36.186.75.121' order by id;"
    qt_sql_index_7 "select * from test_ip_with_inverted_index where ip_v4 in ('61.58.96.173', '36.186.75.121') order by id;"
    qt_sql_index_8 "select * from test_ip_with_inverted_index where ip_v6 = '2a01:b740:a09::1' order by id;"
    qt_sql_index_9 "select * from test_ip_with_inverted_index where ip_v6 in ('2a01:b740:a09::1', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql_index_10 "select * from test_ip_with_inverted_index where ip_v4 < '59.50.185.152' order by id;"
    qt_sql_index_11 "select * from test_ip_with_inverted_index where ip_v4 >= '119.36.22.147' order by id;"
    qt_sql_index_12 "select * from test_ip_with_inverted_index where ip_v6 < '2001:16a0:2:200a::2' order by id;"
    qt_sql_index_13 "select * from test_ip_with_inverted_index where ip_v6 >= '2001:4888:1f:e891:161:26::' order by id;"

    sql """set enable_inverted_index_query=false; """
    qt_sql1 "select * from test_ip_with_inverted_index where ip_v4 = '59.50.185.152' order by id;"
    qt_sql2 "select * from test_ip_with_inverted_index where ip_v4 in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql3 "select * from test_ip_with_inverted_index where ip_v4 not in ('42.117.228.166', '119.36.22.147') order by id"
    qt_sql4 "select * from test_ip_with_inverted_index where ip_v6 = '2001:16a0:2:200a::2' order by id;"
    qt_sql4 "select * from test_ip_with_inverted_index where ip_v6 in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql5 "select * from test_ip_with_inverted_index where ip_v6 not in ('2a02:e980:83:5b09:ecb8:c669:b336:650e', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql6 "select * from test_ip_with_inverted_index where ip_v4 = '36.186.75.121' order by id;"
    qt_sql7 "select * from test_ip_with_inverted_index where ip_v4 in ('61.58.96.173', '36.186.75.121') order by id;"
    qt_sql8 "select * from test_ip_with_inverted_index where ip_v6 = '2a01:b740:a09::1' order by id;"
    qt_sql9 "select * from test_ip_with_inverted_index where ip_v6 in ('2a01:b740:a09::1', '2001:4888:1f:e891:161:26::') order by id"
    qt_sql10 "select * from test_ip_with_inverted_index where ip_v4 < '59.50.185.152' order by id;"
    qt_sql11 "select * from test_ip_with_inverted_index where ip_v4 >= '119.36.22.147' order by id;"
    qt_sql12 "select * from test_ip_with_inverted_index where ip_v6 < '2001:16a0:2:200a::2' order by id;"
    qt_sql13 "select * from test_ip_with_inverted_index where ip_v6 >= '2001:4888:1f:e891:161:26::' order by id;"


}
