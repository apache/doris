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

suite("test_union_has_in_predicate") {
    sql """
        drop table if exists test_union_has_in_predicate_table;
    """
    
    sql """
        create table test_union_has_in_predicate_table ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_union_has_in_predicate_table values (1);
    """

    qt_select """
        with temp as(
        select 
            a as a 
        from 
            (
            (
                select 
                a 
                from 
                test_union_has_in_predicate_table 
                where 
                a in ('1')
            ) 
            union all 
                (
                select 
                    a 
                from 
                    test_union_has_in_predicate_table 
                where 
                    a in ('1')
                )
            ) tmp
        ) 
        select 
        a 
        from 
        temp 
        where 
        a in ('1');
    """

    sql """
        drop table if exists test_union_has_in_predicate_table;
    """
}
