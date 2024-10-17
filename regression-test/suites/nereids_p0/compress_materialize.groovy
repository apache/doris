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
    """

//  expected explain contains partial_any_value(k)
// |   1:VAGGREGATE (update serialize)(162)                     |
// |   |  STREAMING                                             |
// |   |  output: partial_any_value(k[#3])[#5]                  |
// |   |  group by: encode_as_bigint(k)[#2]                     |
// |   |  sortByGroupKey:false                                  |
// |   |  cardinality=1                                         |
// |   |  distribute expr lists: k[#3]  
    explain{
        sql ("""
            select k from compress group by k;
            """)
        contains("any_value(partial_any_value(k)")
        contains("encode_as_bigint")
    }

    // 'substring(k, 1)' is in select list, not supported
    explain{
        sql ("""
            select k, substring(k, 1) from compress group by k;
            """)
        notContains("any_value")
        notContains("encode_as_bigint")
    }

    explain{
        sql ("""
            select k, substring(k, 1) from compress group by k;
            """)
        notContains("any_value")
        notContains("encode_as_bigint")

    }

    order_qt_agg_exec "select k from compress group by k;"
    order_qt_not_support """ select substring(k,1,3) from compress group by substring(k,1,3);"""
    order_qt_not_support """ select substring(k,1,3) from compress group by k;"""

    explain {
        sql("select sum(v) from compress group by substring(k, 1, 3);")
        contains("group by: encode_as_bigint(substring(k, 1, 3))")
    }
    order_qt_encodeexpr "select sum(v) from compress group by substring(k, 1, 3);"


    // verify that compressed materialization do not block runtime filter generation
    sql """
    set disable_join_reorder=true;
    set runtime_filter_mode = GLOBAL;
    set runtime_filter_type=2;
    set enable_runtime_filter_prune=false;
    """

    qt_join """
    explain shape plan 
    select *
    from (
        select k from compress group by k
    ) T join cmt2 on T.k = cmt2.k2;
    """
}

