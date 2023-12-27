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

suite("view") {
    sql """
        SET enable_nereids_planner=true
    """

    sql """
        SET enable_bucket_shuffle_join=false
    """

    sql """
        create view if not exists v1 as 
        select * 
        from customer
    """

    sql """
        create view if not exists v2 as
        select *
        from lineorder
    """

    sql """
        create view if not exists v3 as 
        select *
        from v1 join (
            select *
            from v2
            ) t 
        on v1.c_custkey = t.lo_custkey
    """

    sql "SET enable_fallback_to_original_planner=false"

    qt_select_1 """
        select * 
        from v1
        order by v1.c_custkey
    """

    qt_select_2 """
        select *
        from v2
        order by v2.lo_custkey
    """

    qt_select_3 """
        select *
        from v3
        order by v3.c_custkey, v3.lo_orderkey,lo_tax
    """

    qt_select_4 """
        select * 
        from customer c join (
            select * 
            from v1
            ) t 
        on c.c_custkey = t.c_custkey
        order by c.c_custkey, t.c_custkey
    """

    qt_select_5 """
        select * 
        from lineorder l join (
            select * 
            from v2
            ) t 
        on l.lo_custkey = t.lo_custkey
        order by l.lo_custkey, t.lo_custkey, l.lo_linenumber, t.lo_linenumber, t.lo_shipmode,t.lo_tax
    """

    qt_select_6 """
        select * from (
            select * 
            from part p 
            join v2 on p.p_partkey = v2.lo_partkey) t1 
        join (
            select * 
            from supplier s 
            join v3 on s.s_region = v3.c_region) t2 
        on t1.p_partkey = t2.lo_partkey
        order by t1.lo_custkey, t1.p_partkey, t2.s_suppkey, t2.c_custkey, t2.lo_orderkey
    """
}
