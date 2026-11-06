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

suite("test_fk_versioned_foreign") {
    try {
        sql "alter table self_fk_versioned_foreign drop constraint versioned_foreign_fk"
    } catch (Exception ignored) {
    }
    sql "drop table if exists self_fk_versioned_foreign"
    sql """
        create table self_fk_versioned_foreign (
            id int not null,
            parent_id int not null
        ) unique key(id)
        distributed by hash(id) buckets 1
        properties(
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql "alter table self_fk_versioned_foreign add constraint versioned_foreign_pk primary key (id)"
    sql """
        alter table self_fk_versioned_foreign add constraint versioned_foreign_fk
        foreign key (parent_id) references self_fk_versioned_foreign(id)
    """

    sql "insert into self_fk_versioned_foreign values (1, 1), (2, 2)"
    sql "delete from self_fk_versioned_foreign where id = 1"
    sql "sync"

    // The change read still exposes the old (1, 1) row, while the current PK
    // scan has only (2, 2). A wrong FK proof would return 1 as well as 2.
    explain {
        sql """
            shape plan
            select f.parent_id
            from self_fk_versioned_foreign p
            inner join self_fk_versioned_foreign@incr('incrementType' = 'DETAIL') f
                on p.id = f.parent_id
        """
        contains "INNER_JOIN"
    }

    order_qt_change_read_foreign """
        select f.parent_id
        from self_fk_versioned_foreign p
        inner join self_fk_versioned_foreign@incr('incrementType' = 'DETAIL') f
            on p.id = f.parent_id
        order by f.parent_id
    """
}
