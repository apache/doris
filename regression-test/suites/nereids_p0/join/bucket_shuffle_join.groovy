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

suite("bucket-shuffle-join") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    order_qt_test_bucket """
    select * from test_bucket_shuffle_join where rectime="2021-12-01 00:00:00" and id in (select k1 from test_join where k1 in (1,2))
    """

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
    sql """insert into shuffle_join_t1 values("1");"""
    sql """insert into shuffle_join_t1 values("1");"""
    sql """insert into shuffle_join_t1 values("1");"""
    sql """insert into shuffle_join_t2 values("1","1","1");"""
    sql """insert into shuffle_join_t2 values("1","1","1");"""
    sql """insert into shuffle_join_t2 values("1","1","1");"""
    sql """insert into shuffle_join_t2 values("1","1","1");"""

    sql """analyze table shuffle_join_t1 with sync;"""
    sql """analyze table shuffle_join_t2 with sync;"""

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
        contains "BUCKET_SHFFULE_HASH_PARTITIONED: c"
    }

}
