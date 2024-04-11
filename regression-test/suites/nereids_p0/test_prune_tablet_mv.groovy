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

suite("test_prune_tablet_mv") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS test_prune_tablet_t2"
    sql """
	    create table test_prune_tablet_t2(id int, c1 boolean) distributed by hash(id) BUCKETS 16 properties('replication_num'='1');
    """

    createMV( """
	    CREATE
        MATERIALIZED VIEW mv_t2 AS
        SELECT c1,
            id
        FROM test_prune_tablet_t2
        ORDER BY c1,
        id; 
    """)
    sql "insert into test_prune_tablet_t2 values(1,0),(2,0),(3,0),(4,0),(5,0),(6,0),(7,0);"

    explain {
        sql("select * from test_prune_tablet_t2 where c1 = 0 and id = 3;")
        contains "mv_t2"
        contains "tablets=1/16"
    }

    sql "DROP TABLE IF EXISTS test_prune_tablet_t2"
}