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

suite("not_equal") {
    multi_sql """
        drop table if exists random_tbl_test;
        CREATE TABLE `random_tbl_test` (
          `id` int NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY RANDOM BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        
        insert into random_tbl_test select * from numbers('number'='100');
        
        set enable_nereids_planner=false;
        set parallel_pipeline_task_num=1;
        set enable_pipeline_engine=true;
        set enable_pipeline_x_engine=false;
        set enable_shared_scan=true;"""

    explain {
        sql "select * from random_tbl_test where 1 ! = 2"
        check { String explainStr ->
            assertEquals(2, explainStr.count("PLAN FRAGMENT"))
        }
    }

    test {
        sql "select * from random_tbl_test where 1 ! = 2"
        rowNum(100)
    }
}