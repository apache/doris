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

 suite("test_disable_parallel_scan") {
     sql "SET enable_nereids_planner=true"
     sql "SET enable_fallback_to_original_planner=false"
     sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

     sql """drop table if exists sequence_count_test3;"""
     sql """CREATE TABLE sequence_count_test3(
                    `uid` int COMMENT 'user id',
                    `date` datetime COMMENT 'date time', 
                    `number` int NULL COMMENT 'number' 
                            )
            DUPLICATE KEY(uid) 
            DISTRIBUTED BY HASH(uid) BUCKETS 1 
            PROPERTIES ( 
                "replication_num" = "1"
            );"""
     explain {
        sql("""
            SELECT
                uid,
                DATE,
                e.NUMBER AS EVENT_GROUP,
            CASE
                    
                    WHEN (
                        (
                            UNIX_TIMESTAMP( DATE ) - (
                                lag ( UNIX_TIMESTAMP( DATE ), 1, 0 ) over ( PARTITION BY uid ORDER BY DATE  ) 
                            ) 
                        ) > 600 
                        ) THEN
                        1 ELSE 0 
                    END AS SESSION_FLAG 
                FROM
                    sequence_count_test3 e 
        """)
        contains "HAS_COLO_PLAN_NODE: true"
     }

     sql """drop table if exists sequence_count_test3;"""
 }