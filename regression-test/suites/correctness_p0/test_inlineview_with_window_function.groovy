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

suite("test_inlineview_with_window_function") {
    sql """
        drop view if exists test_table_aa;
    """

    sql """
        drop table if exists test_table_bb;
    """
    
    sql """
        CREATE TABLE `test_table_bb` (
        `event_date` datetime NULL,
        `event_content` text NULL,
        `animal_id` bigint(20) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`event_date`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`event_date`) BUCKETS 48
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE VIEW test_table_aa AS
        SELECT 
        ev.event_date,
        ev.animal_id,
        get_json_int(ev.event_content, "\$.nurseOns") as nurseons
        FROM test_table_bb ev;
    """

    sql """
        SELECT row_number() over(PARTITION BY animal_id ORDER BY event_date DESC) rw
        FROM test_table_aa;
    """

    sql """
        select  
        e1
        from test_table_aa
        lateral view explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]) tmp1 as e1
        where animal_id <= e1;
    """

    sql """
        drop view if exists test_table_aa;
    """

    sql """
        drop table if exists test_table_bb;
    """

    sql """drop table if exists test_table_aaa;"""
    sql """drop table if exists test_table_bbb;"""
    sql """CREATE TABLE `test_table_aaa` (
            `ordernum` varchar(65533) NOT NULL ,
            `dnt` datetime NOT NULL ,
            `data` json NULL 
            ) ENGINE=OLAP
            DUPLICATE KEY(`ordernum`, `dnt`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

    sql """CREATE TABLE `test_table_bbb` (
            `dnt` varchar(200) NULL,
            `ordernum` varchar(200) NULL,
            `type` varchar(20) NULL,
            `powers` double SUM NULL,
            `p0` double REPLACE NULL,
            `heatj` double SUM NULL,
            `j0` double REPLACE NULL,
            `heatg` double SUM NULL,
            `g0` double REPLACE NULL,
            `solar` double SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`dnt`, `ordernum`, `type`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            ); """

    sql """DROP MATERIALIZED VIEW IF EXISTS ods_zn_dnt_max1 ON test_table_aaa;"""
    sql """create materialized view ods_zn_dnt_max1 as
            select ordernum,max(dnt) as dnt from test_table_aaa
            group by ordernum
            ORDER BY ordernum;"""

    sql """insert into test_table_aaa values('cib2205045_1_1s','2023/6/10 3:55:33','{"DB1":168939,"DNT":"2023-06-10 03:55:33"}');"""
    sql """insert into test_table_aaa values('cib2205045_1_1s','2023/6/10 3:56:33','{"DB1":168939,"DNT":"2023-06-10 03:56:33"}');"""
    sql """insert into test_table_aaa values('cib2205045_1_1s','2023/6/10 3:57:33','{"DB1":168939,"DNT":"2023-06-10 03:57:33"}');"""
    sql """insert into test_table_aaa values('cib2205045_1_1s','2023/6/10 3:58:33','{"DB1":168939,"DNT":"2023-06-10 03:58:33"}');"""

    qt_order """select
                '2023-06-10',
                tmp.ordernum,
                cast(nvl(if(tmp.p0-tmp1.p0>0,tmp.p0-tmp1.p0,tmp.p0-tmp.p1),0) as decimal(10,4)),
                nvl(tmp.p0,0),
                cast(nvl(if(tmp.j0-tmp1.j0>0,tmp.j0-tmp1.j0,tmp.j0-tmp.j1)*277.78,0) as decimal(10,4)),
                nvl(tmp.j0,0),
                cast(nvl(if(tmp.g0-tmp1.g0>0,tmp.g0-tmp1.g0,tmp.g0-tmp.g1)*277.78,0) as decimal(10,4)),
                nvl(tmp.g0,0),
                cast(nvl(tmp.solar,0) as decimal(20,4)),
                'day'
                from 
                (
                select
                    ordernum,
                    max(ljrl1) g0,min(ljrl1) g1,
                    max(ljrl2) j0,min(ljrl2) j1,
                    max(db1) p0,min(db1) p1,
                    max(fzl)*1600*0.278 solar
                from(
                    select ordernum,dnt,
                            cast(if(json_extract(data,'\$.LJRL1')=0 or json_extract(data,'\$.LJRL1') like '%E%',null,json_extract(data,'\$.LJRL1')) as double) ljrl1,
                            cast(if(json_extract(data,'\$.LJRL2')=0 or json_extract(data,'\$.LJRL2') like '%E%',null,json_extract(data,'\$.LJRL2')) as double) ljrl2,
                            first_value(cast(if(json_extract(data,'\$.FZL')=0 or json_extract(data,'\$.FZL') like '%E%',null,
                            json_extract(data,'\$.FZL')) as double)) over (partition by ordernum order by dnt desc) fzl,
                            cast(if(json_extract(data,'\$.DB1')=0 or json_extract(data,'\$.DB1') like '%E%',null,json_extract(data,'\$.DB1')) as double) db1
                    from test_table_aaa
                        )a1
                group by ordernum
                )tmp left join (
                select
                    ordernum,MAX(p0) p0,MAX(j0) j0,MAX(g0) g0
                from test_table_bbb
                    group by ordernum
                )tmp1
                on tmp.ordernum=tmp1.ordernum;"""

    sql """drop table if exists test_table_aaa2;"""
    sql """CREATE TABLE `test_table_aaa2` (
            `ordernum` varchar(65533) NOT NULL ,
            `dnt` datetime NOT NULL ,
            `data` json NULL 
            ) ENGINE=OLAP
            DUPLICATE KEY(`ordernum`, `dnt`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`ordernum`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:55:33','{"DB1":168939,"DNT":"2023-06-10 03:55:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:56:33','{"DB1":168939,"DNT":"2023-06-10 03:56:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:57:33','{"DB1":168939,"DNT":"2023-06-10 03:57:33"}' ;"""
    sql """insert into test_table_aaa2 select 'cib2205045_1_1s','2023/6/10 3:58:33','{"DB1":168939,"DNT":"2023-06-10 03:58:33"}' ;"""

}
