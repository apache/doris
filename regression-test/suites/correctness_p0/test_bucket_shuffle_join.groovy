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

suite("test_bucket_shuffle_join") {

    sql "set disable_join_reorder=true"
    sql "set parallel_pipeline_task_num=1"

    sql """ DROP TABLE IF EXISTS `test_colo1` """
    sql """ DROP TABLE IF EXISTS `test_colo2` """
    sql """ DROP TABLE IF EXISTS `test_colo3` """
    sql """
        CREATE TABLE `test_colo1` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """
    sql """
        CREATE TABLE `test_colo2` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_colo3` (
        `id` varchar(64) NULL,
        `name` varchar(64) NULL,
        `age` int NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`,`name`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`,`name`) BUCKETS 6
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """insert into test_colo1 values('1','a',12);"""
    sql """insert into test_colo2 values('1','a',12);"""
    sql """insert into test_colo3 values('1','a',12);"""

    explain {
        sql("select a.id,a.name,b.id,b.name,c.id,c.name from test_colo1 a inner join test_colo2 b on a.id = b.id and a.name = b.name inner join test_colo3 c on a.id=c.id and a.name= c.name")
        contains "join op: INNER JOIN(BUCKET_SHUFFLE)"
    }

    explain {
        sql("""select a.id,a.name,b.id,b.name 
                from (select * from test_colo1) a 
                inner join 
                (select * from test_colo2) b 
                on a.id = b.id  and a.name = b.name and a.name = b.name
                inner join 
                (select * from test_colo3) c 
                on a.id = c.id  and a.name = c.name and a.name = c.name""")
        contains "join op: INNER JOIN(BUCKET_SHUFFLE)"
    }

    sql """ DROP TABLE IF EXISTS shuffle_join_t1 """
    sql """ DROP TABLE IF EXISTS shuffle_join_t2 """

    sql """
        create table shuffle_join_t1 ( a varchar(10) not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table shuffle_join_t2 ( a varchar(5) not null, b string not null, c char(3) not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """insert into shuffle_join_t1 values("1");"""
    sql """insert into shuffle_join_t2 values("1","1","1");"""

    explain {
        sql("select * from shuffle_join_t1 t1 left join shuffle_join_t2 t2 on t1.a = t2.a;")
        contains "BUCKET_SHUFFLE"
    }

    explain {
        sql("select * from shuffle_join_t1 t1 left join shuffle_join_t2 t2 on t1.a = t2.b;")
        contains "BUCKET_SHUFFLE"
    }

    explain {
        sql("select * from shuffle_join_t1 t1 left join shuffle_join_t2 t2 on t1.a = t2.c;")
        contains "BUCKET_SHUFFLE"
    }

    sql """ DROP TABLE IF EXISTS shuffle_join_t1 """
    sql """ DROP TABLE IF EXISTS shuffle_join_t2 """
}
