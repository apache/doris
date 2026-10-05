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

suite("test_table_function_with_subquery") {
    sql """
        drop table if exists table_tf;
    """
    
    sql """
        create table table_tf ( a char(10) not null, b char(10) not null, c char(10) not null)
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into table_tf values( '1','1','1' );
    """

    qt_select """
        select
        a,
        multiples
        from
        (
            select
            t1.a,
            concat(
                '1:',
                ',',
                '2:',
                ',',
                '3:',
                ',',
                '4:',
                ',',
                '5:'
            ) multiple_list
            from
            (
                select
                h1.a
                
                from
                table_tf r1
                left join (
                    select
                    `b`,
                    `a`
                    from
                    table_tf
                ) h1 using (`b`)
                group by
                h1.a
            ) t1
            left join (
                select

                h1.a
                from
                table_tf r1
                left join (
                    select
                    `b`,
                    a
                    from
                    table_tf
                ) h1 using (b)
                group by
                h1.a
            ) t2 using (`a`)
        ) tmp1 lateral view explode_split(multiple_list, ',') multiples as multiples;
    """

    sql """
        drop table if exists table_tf;
    """

}
