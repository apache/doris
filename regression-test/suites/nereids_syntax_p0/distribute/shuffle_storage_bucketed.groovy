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

suite("shuffle_storage_bucketed") {
    multi_sql """
        drop table if exists du;
        drop table if exists o;
        drop table if exists ds;
        
        CREATE TABLE `du` (
          `et` datetime NOT NULL,
          `st` datetime NOT NULL,
          `gc` varchar(50) NOT NULL,
          `pc` varchar(50) NOT NULL,
          `uk` varchar(255) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`et`, `st`, `gc`, `pc`, `uk`)
        DISTRIBUTED BY HASH(`gc`, `pc`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        CREATE TABLE `o` (
          `d` date NULL,
          `g` varchar(500) NULL,
          `p` varchar(500) NULL,
          `dt` datetime NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`d`, `g`, `p`)
        DISTRIBUTED BY HASH(`g`, `p`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        CREATE TABLE `ds` (
          `gc` varchar(50) NOT NULL,
          `pc` varchar(50) NOT NULL,
          `s` int NULL,
          `n` varchar(50) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`gc`, `pc`, `s`, `n`)
        DISTRIBUTED BY HASH(`gc`, `pc`, `s`, `n`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        
        set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
        """

    sql """
        explain plan
        select *
        from du
        right outer join
        (
          select g gc, p pc
          from o
          where date(dt) = '2020-05-25 00:00:00'
        ) r
        on r.gc=du.gc and r.pc=du.pc
        left join ds s
        on r.gc=s.gc and r.pc=s.pc;
        """
}
