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

 suite("test_grouping_with_alias") {
     qt_select """ 
        select
        coalesce(col1, 'all') as col1,
        count(*) as cnt
        from
        (
            select
                null as col1
            union all
            select
                'a' as col1
        ) t
        group by
        grouping sets ((col1),())
        order by
        col1,
        cnt;
     """

    sql """DROP TABLE IF EXISTS `cf_member`; """
    sql """ 
        CREATE TABLE `cf_member` (
        `id` bigint(20) NOT NULL,
        `userinfoid` bigint(20) NULL,
        `username` text NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """insert into cf_member values(2, 2, '2'); """

    qt_select2 """select floor(id-1.0), count(*) from cf_member cm group by floor(id-1.0) order by floor(id-1.0);"""

    qt_select3 """select floor(id-1.0), count(*) from cf_member cm group by 1 order by 1;"""

    sql """DROP TABLE IF EXISTS `cf_member`; """
 } 