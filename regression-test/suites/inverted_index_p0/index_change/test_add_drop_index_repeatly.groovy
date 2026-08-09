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

suite("test_add_drop_index_repeatly"){
    sql "set enable_add_index_for_new_data = true"

    def timeout = 300000

    def tbl = 'test_add_drop_index_repeatly'
    sql """ DROP TABLE IF EXISTS ${tbl} """
    sql """
        CREATE TABLE ${tbl} (
        `k1` int(11) NULL,
        `k2` int(11) NULL
        )
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
        """
    for (def i = 1; i <= 5; i++) {
        sql "INSERT INTO ${tbl} VALUES (${i}, ${10 * i})"
    }

    sql """ sync """

    for (def i = 1; i <= 10; i++) {
        // create index on table 
        sql """ create index idx_k2 on ${tbl}(k2) using inverted """

        // build index
        run_index_change_job_and_wait(tbl, timeout) {
            build_index_on_table("idx_k2", tbl)
        }

        run_index_change_job_and_wait(tbl, timeout) {
            sql """ drop index idx_k2 on ${tbl} """
        }
    }
}
