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

suite("test_rand_filter") {
    sql"""set enable_nereids_planner=false;"""
    sql """ DROP TABLE IF EXISTS test_rand_filter_t """
    sql """
            CREATE TABLE  test_rand_filter_t (
                a int
            )
            DUPLICATE KEY(a)
            DISTRIBUTED BY HASH(a) BUCKETS 1
            PROPERTIES (
              "replication_num" = "1"
            )
        """
    explain {
        sql("""select * from test_rand_filter_t where rand() < 0.5 union all select * from test_rand_filter_t where rand() > 0.3;""")
        notContains("AND")
    }
    explain {
        sql("""select * from test_rand_filter_t 
                union all (select * from test_rand_filter_t where rand() < 0.3) 
                union all (select * from test_rand_filter_t where a > 5 and rand() < 0.4);""")
        notContains("rand() < 0.3 AND rand() < 0.4")
    }
    sql """ DROP TABLE IF EXISTS test_rand_filter_t """
}
