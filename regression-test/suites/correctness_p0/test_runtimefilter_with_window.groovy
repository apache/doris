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

suite("test_runtimefilter_with_window") {
 sql """ set enable_nereids_planner=true"""
 sql """ set disable_join_reorder=true"""
 sql """ set enable_runtime_filter_prune=false"""
sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

 sql """ DROP TABLE IF EXISTS `test_runtimefilter_with_window_table1` """
 sql """ DROP TABLE IF EXISTS `test_runtimefilter_with_window_table2` """
 sql """
        CREATE TABLE `test_runtimefilter_with_window_table1` (
        `param` varchar(65533) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`param`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`param`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
 """
 sql """
        CREATE TABLE `test_runtimefilter_with_window_table2` (
        `phone` varchar(65533) NULL ,
        `channel_param` text NULL ,
        `createtime` datetime NULL,
        `liuzi_status` tinyint(4) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`phone`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`phone`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
 """
 explain {
        sql("""select  a.phone
                        ,a.channel_param
                        ,a.createtime
                        ,rn
                        ,if(rn = 1,1,0) as liuzi_status
                from    (
                            select a.phone,a.channel_param,a.createtime
                            ,row_number() over(partition by phone order by createtime asc) as rn
                            from test_runtimefilter_with_window_table2 a
                        ) a join    (
                            select  param
                            from    test_runtimefilter_with_window_table1
                        ) b
                on      a.channel_param = b.param; """)
        notContains "runtime filters"
    }

 explain {
        sql("""select  a.phone
                        ,a.channel_param
                        ,a.createtime
                from    (
                            select a.phone,a.channel_param,a.createtime
                            from test_runtimefilter_with_window_table2 a
                        ) a join    (
                            select  param
                            from    test_runtimefilter_with_window_table1
                        ) b
                on      a.channel_param = b.param; """)
        contains "runtime filters"
    }

 sql """ set enable_nereids_planner=false"""
 sql """ set disable_join_reorder=true"""
 sql """ set enable_runtime_filter_prune=false"""
 log.info("======origin planner1=================")
 explain {
        sql("""select  a.phone
                        ,a.channel_param
                        ,a.createtime
                        ,rn
                        ,if(rn = 1,1,0) as liuzi_status
                from    (
                            select a.phone,a.channel_param,a.createtime
                            ,row_number() over(partition by phone order by createtime asc) as rn
                            from test_runtimefilter_with_window_table2 a
                        ) a join    (
                            select  param
                            from    test_runtimefilter_with_window_table1
                        ) b
                on      a.channel_param = b.param; """)
        notContains "runtime filters"
    }
log.info("======origin planner2=================")
  explain {
        sql("""select  a.phone
                        ,a.channel_param
                        ,a.createtime
                from    (
                            select a.phone,a.channel_param,a.createtime
                            from test_runtimefilter_with_window_table2 a
                        ) a join    (
                            select  param
                            from    test_runtimefilter_with_window_table1
                        ) b
                on      a.channel_param = b.param; """)
        contains "runtime filters"
    }
}

