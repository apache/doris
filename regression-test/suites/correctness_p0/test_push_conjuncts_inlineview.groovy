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

suite("test_push_conjuncts_inlineview") {
 sql """ set enable_nereids_planner=false"""
 sql """ DROP TABLE IF EXISTS `push_conjunct_table` """
 sql """
        CREATE TABLE `push_conjunct_table` (
        `a_key` varchar(255) NULL ,
        `d_key` varchar(255) NULL ,
        `c_key` varchar(32) NULL ,
        `b_key` date NOT NULL 
        ) ENGINE=OLAP
        UNIQUE KEY(`a_key`, `d_key`, `c_key`)
        DISTRIBUTED BY HASH(`a_key`, `d_key`, `c_key`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        ); 
 """
 explain {
        sql("""select
                    1
                from
                    (
                        select
                            rank() over(
                                partition by a_key
                                , c_key
                                , d_key
                            order by
                                b_key desc
                            ) as px
                        from
                            push_conjunct_table a

                    union all
                        select 2 as px
                        from
                            push_conjunct_table a
                    )a
                where
                    a.px = 1;""")
        contains "4:VSELECT"
    }

 sql """ DROP TABLE IF EXISTS `push_conjunct_table` """
}

