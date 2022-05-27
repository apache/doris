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

suite("test_hll_int", "datatype") {
    sql "DROP TABLE IF EXISTS test_int_hll"
    sql """
        CREATE TABLE test_int_hll (`id` int COMMENT "", `hll_set` hll hll_union COMMENT "") 
        ENGINE=OLAP DISTRIBUTED BY HASH(`id`) BUCKETS 5 properties("replication_num" = "1");
        """
    sql "insert into test_int_hll values(1, hll_hash(1)), (2, hll_hash(2)), (3, hll_hash(3))"
    qt_sql1 "select hll_union_agg(hll_set), count(*) from test_int_hll"
    // qt_sql2 "select id, hll_candinality(hll_set) from test_int_hll"
    sql "DROP TABLE test_int_hll"
}

