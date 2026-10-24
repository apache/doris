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

suite("self_fk_limit") {
    try {
        sql "alter table self_fk_limit drop constraint self_fk_limit_fk"
    } catch (Exception ignored) {
    }
    try {
        sql "alter table self_fk_mixed_alias_foreign drop constraint self_fk_mixed_alias_fk"
    } catch (Exception ignored) {
    }
    sql "drop table if exists self_fk_limit"
    sql "drop table if exists self_fk_mixed_alias_foreign"
    sql "drop table if exists self_fk_mixed_alias_primary"
    sql """
        create table self_fk_limit (
            id int not null,
            parent_id int not null
        ) unique key(id)
        partition by range(id) (
            partition p1 values less than (2),
            partition p2 values less than (MAXVALUE)
        )
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into self_fk_limit values (1, 1), (2, 2)"
    sql "alter table self_fk_limit add constraint self_fk_limit_pk primary key (id)"
    sql """
        alter table self_fk_limit add constraint self_fk_limit_fk
        foreign key (parent_id) references self_fk_limit(id)
    """

    sql """
        create table self_fk_mixed_alias_primary (
            a int not null,
            b int not null
        ) unique key(a, b)
        distributed by hash(a) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table self_fk_mixed_alias_foreign (
            fa int not null,
            fb int not null
        ) duplicate key(fa, fb)
        distributed by hash(fa) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into self_fk_mixed_alias_primary values (1, 1), (2, 2)"
    sql "insert into self_fk_mixed_alias_foreign values (1, 1), (2, 2)"
    sql """
        alter table self_fk_mixed_alias_primary add constraint self_fk_mixed_alias_pk
        primary key (a, b)
    """
    sql """
        alter table self_fk_mixed_alias_foreign add constraint self_fk_mixed_alias_fk
        foreign key (fa, fb) references self_fk_mixed_alias_primary(a, b)
    """

    explain {
        sql """
            shape plan
            select f.parent_id
            from (select id from self_fk_limit order by id limit 1) p
            inner join self_fk_limit f on p.id = f.parent_id
        """
        contains "INNER_JOIN"
    }

    explain {
        sql """
            shape plan
            select f1.fa, f2.fb
            from self_fk_mixed_alias_primary p
            inner join (
                self_fk_mixed_alias_foreign f1
                cross join self_fk_mixed_alias_foreign f2
            ) on p.a = f1.fa and p.b = f2.fb
        """
        contains "INNER_JOIN"
    }

    explain {
        sql """
            shape plan
            select f.parent_id
            from self_fk_limit p
            inner join self_fk_limit f on p.id = f.parent_id
        """
        notContains "INNER_JOIN"
    }

    explain {
        sql """
            shape plan
            select f.parent_id
            from (select id as pk from self_fk_limit where parent_id = 1) p
            inner join self_fk_limit f on p.pk = f.parent_id
        """
        contains "INNER_JOIN"
    }

    explain {
        sql """
            shape plan
            select f.parent_id
            from self_fk_limit partition(p1) p
            inner join self_fk_limit f on p.id = f.parent_id
        """
        contains "INNER_JOIN"
    }

    explain {
        sql """
            shape plan
            select f.parent_id
            from self_fk_limit p tablesample(1 rows)
            inner join self_fk_limit f on p.id = f.parent_id
        """
        contains "INNER_JOIN"
    }

    try {
        sql "set skip_storage_engine_merge = true"
        explain {
            sql """
                shape plan
                select f.parent_id
                from self_fk_limit p
                inner join self_fk_limit f on p.id = f.parent_id
            """
            contains "INNER_JOIN"
        }
    } finally {
        sql "set skip_storage_engine_merge = false"
    }

    order_qt_alias_hidden_filter """
        select f.parent_id
        from (select id as pk from self_fk_limit where parent_id = 1) p
        inner join self_fk_limit f on p.pk = f.parent_id
        order by f.parent_id
    """

    order_qt_partition_primary """
        select f.parent_id
        from self_fk_limit partition(p1) p
        inner join self_fk_limit f on p.id = f.parent_id
        order by f.parent_id
    """

    order_qt_limited_primary """
        select f.parent_id
        from (select id from self_fk_limit order by id limit 1) p
        inner join self_fk_limit f on p.id = f.parent_id
        order by f.parent_id
    """

    order_qt_composite_fk_mixed_alias """
        select f1.fa, f2.fb
        from self_fk_mixed_alias_primary p
        inner join (
            self_fk_mixed_alias_foreign f1
            cross join self_fk_mixed_alias_foreign f2
        ) on p.a = f1.fa and p.b = f2.fb
        order by f1.fa, f2.fb
    """
}
