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

suite("test_join_without_condition") {
    sql """
        drop table if exists test_join_without_condition_a;
    """

    sql """
        drop table if exists test_join_without_condition_b;
    """

    sql """
        create table if not exists test_join_without_condition_a ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        create table if not exists test_join_without_condition_b ( a int not null )
        ENGINE=OLAP
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2"
        );
    """

    sql """
        insert into test_join_without_condition_a values(1), (2), (3);
    """

    sql """
        insert into test_join_without_condition_b values(4), (5), (6);
    """

    // here condition 'b.a > 1' will be pushed down to scan node.
    qt_select """
        select a.a, b.a
        from
            test_join_without_condition_a a
            left join test_join_without_condition_b b on b.a > 1
        order by a.a, b.a;
    """

    explain {
        sql("select * from (select 1 id) t where (1 in (select a from test_join_without_condition_a))")
        notContains "CROSS JOIN"
        contains "LEFT SEMI JOIN"
    }

    sql """
        drop table if exists test_join_without_condition_a;
    """

    sql """
        drop table if exists test_join_without_condition_b;
    """
}
