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

suite("compress_materialize") {
    sql """
    drop table if exists compress;
    CREATE TABLE `compress` (
    `k` varchar(5) NOT NULL,
    `v` int NOT NULL
    ) ENGINE=OLAP
    duplicate KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
    PROPERTIES (
    "replication_num" = "1"
    ); 


    insert into compress values ("aaaaaa", 1), ("aaaaaa", 2), ("bbbbb", 3), ("bbbbb", 4), ("bbbbb", 5);


    drop table if exists cmt2;
    CREATE TABLE `cmt2` (
    `k2` varchar(5) NOT NULL,
    `v2` int NOT NULL
    ) ENGINE=OLAP
    duplicate KEY(`k2`)
    DISTRIBUTED BY random
    PROPERTIES (
    "replication_num" = "1"
    ); 

    insert into cmt2 values ("aaaa", 1), ("b", 3);
    insert into cmt2 values("123456", 123456);
    
    set ENABLE_COMPRESS_MATERIALIZE = true;
    """

    explain{
        sql ("""
            select k from compress group by k;
            """)
        contains("encode_as_bigint")
    }
    order_qt_agg_exec "select k from compress group by k;"

    explain{
        sql ("""
            select k, substring(k, 1), sum(v) from compress group by k;
            """)
        contains("encode_as_bigint(k)")
    }
    order_qt_output_contains_gpk "select k, substring(k, 1) from compress group by k;"

    order_qt_expr """ select substring(k,1,3) from compress group by substring(k,1,3);"""
    explain{
        sql "select substring(k,1,3) from compress group by substring(k,1,3);"
        contains("encode_as_int(substring(k, 1, 3))")
    }

    explain {
        sql("select sum(v) from compress group by substring(k, 1, 3);")
        contains("group by: encode_as_int(substring(k, 1, 3))")
    }

    explain {
        sql("select sum(v) from compress group by substring(k, 1, 4);")
        contains("group by: encode_as_bigint(substring(k, 1, 4))")
    }

    order_qt_encodeexpr "select sum(v) from compress group by substring(k, 1, 3);"

    // TODO: RF targets on compressed_materialze column is broken
    // // verify that compressed materialization do not block runtime filter generation
    // sql """
    // set disable_join_reorder=true;
    // set runtime_filter_mode = GLOBAL;
    // set runtime_filter_type=2;
    // set enable_runtime_filter_prune=false;
    // """

    // qt_join """
    // explain shape plan 
    // select *
    // from (
    //     select k from compress group by k
    // ) T join[broadcast] cmt2 on T.k = cmt2.k2;
    // """


    sql """
    drop table if exists compressInt;
    CREATE TABLE `compressInt` (
    `k` varchar(3) NOT NULL,
    `v` int NOT NULL
    ) ENGINE=OLAP
    duplicate KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
    PROPERTIES (
    "replication_num" = "1"
    ); 


    insert into compressInt values ("a", 1), ("aa", 2), ("bb", 3), ("b", 4), ("b", 5);
    """
    explain{
        sql "select k from compressInt group by k"
        contains("encode_as_int")
    }

    sql """
    drop table if exists compressLargeInt;
    CREATE TABLE `compressLargeInt` (
    `k` varchar(10) NOT NULL,
    `v` int NOT NULL
    ) ENGINE=OLAP
    duplicate KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
    PROPERTIES (
    "replication_num" = "1"
    ); 


    insert into compressLargeInt values ("a", 1), ("aa", 2), ("bb", 3), ("b", 4), ("b", 5);
    """
    explain{
        sql "select k from compressLargeInt group by k"
        contains("group by: encode_as_largeint(k)")
    }


    sql """
    drop table if exists notcompress;
    CREATE TABLE `notcompress` (
    `k` varchar(16) NOT NULL,
    `v` int NOT NULL
    ) ENGINE=OLAP
    duplicate KEY(`k`)
    DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
    PROPERTIES (
    "replication_num" = "1"
    ); 


    insert into notcompress values ("a", 1), ("aa", 2), ("bb", 3), ("b", 4), ("b", 5);
    """
    explain{
        sql "select k from notcompress group by k"
        notContains("encode_as_")
    }

    sql """
        drop table if exists compressSort;
        CREATE TABLE `compressSort` (
        `k` varchar(3) NULL,
        `v` int NOT NULL
        ) ENGINE=OLAP
        duplicate KEY(`k`)
        DISTRIBUTED BY HASH(`k`) BUCKETS AUTO
        PROPERTIES (
        "replication_num" = "1"
        ); 


        insert into compressSort values ("a", 1), ("aa", 2), ("bb", 3), ("b", 4), ("b", 5);
        insert into compressSort(v) values (6);
        insert into compressSort values ("",7), ("中", 8), ("国", 9);
    """
    explain {
        sql "select v from compressSort order by k"
        contains("order by: encode_as_int(k)")
// expect plan fragment:
// 1:VSORT(140)                                                                                             |
//   order by: encode_as_int(k)[#5] ASC                                                                    |
//   algorithm: full sort                                                                                  |
//   offset: 0                                                                                             |
//   distribute expr lists:  
    }
    qt_sort "select * from compressSort order by k asc, v";
    qt_sort "select * from compressSort order by k desc, v";
    qt_sort "select * from compressSort order by k desc nulls last";
    qt_sort "select * from compressSort order by k desc nulls last, v limit 3";

}

