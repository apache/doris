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

suite("test_rollup_prune_tablet") {
    sql "SET enable_nereids_planner=false"

    sql "DROP TABLE IF EXISTS test_prune_tablet_t2"
    sql """
	    CREATE TABLE `test_prune_tablet_t2` (
                     `id` INT NULL,
                     `c1` BOOLEAN NULL,
                     `id2` BIGINT NULL,
                     `id3` BIGINT NULL,
                     `id4` BIGINT NULL
                    ) distributed by hash(id) BUCKETS 16 properties('replication_num'='1');
    """

    createMV( """
	    alter table test_prune_tablet_t2 add rollup example_rollup_index(c1,id); 
    """)

    sql """insert into test_prune_tablet_t2 values
            (1,0,1,1,1),
            (2,0,2,2,2),
            (3,0,3,3,3),
            (4,0,4,4,4),
            (5,0,5,5,5),
            (6,0,6,6,6),
            (7,0,7,7,7);"""

    explain {
        sql("select id,sum(c1) from test_prune_tablet_t2 where c1=0 and id = 3 group by id;")
        contains "example_rollup_index"
        contains "tablets=1/16"
    }

    sql "DROP TABLE IF EXISTS test_prune_tablet_t2"
}