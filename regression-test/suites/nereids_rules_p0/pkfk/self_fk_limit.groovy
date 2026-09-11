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
    sql "drop table if exists self_fk_limit"
    sql """
        create table self_fk_limit (
            id int not null,
            parent_id int not null
        ) unique key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql "insert into self_fk_limit values (1, 1), (2, 2)"
    sql "alter table self_fk_limit add constraint self_fk_limit_pk primary key (id)"
    sql """
        alter table self_fk_limit add constraint self_fk_limit_fk
        foreign key (parent_id) references self_fk_limit(id)
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
            select f.parent_id
            from self_fk_limit p
            inner join self_fk_limit f on p.id = f.parent_id
        """
        notContains "INNER_JOIN"
    }

    order_qt_limited_primary """
        select f.parent_id
        from (select id from self_fk_limit order by id limit 1) p
        inner join self_fk_limit f on p.id = f.parent_id
        order by f.parent_id
    """
}
