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

suite("fk_raw_version_foreign") {
    try {
        sql "alter table fk_raw_version_foreign_child drop constraint fk_raw_version_foreign_fk"
    } catch (Exception ignored) {
    }
    sql "drop table if exists fk_raw_version_foreign_child"
    sql "drop table if exists fk_raw_version_foreign_parent"

    sql """
        create table fk_raw_version_foreign_parent (
            id int not null
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        create table fk_raw_version_foreign_child (
            id int not null,
            parent_id int not null
        ) unique key(id)
        distributed by hash(id) buckets 1
        properties(
            "enable_unique_key_merge_on_write" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        )
    """
    sql """
        alter table fk_raw_version_foreign_parent add constraint fk_raw_version_foreign_pk
        primary key (id)
    """
    sql """
        alter table fk_raw_version_foreign_child add constraint fk_raw_version_foreign_fk
        foreign key (parent_id) references fk_raw_version_foreign_parent(id)
    """

    sql "insert into fk_raw_version_foreign_parent values (1), (2)"
    sql "insert into fk_raw_version_foreign_child values (10, 1)"
    sql "insert into fk_raw_version_foreign_child values (10, 2)"
    sql "delete from fk_raw_version_foreign_parent where id = 1"
    sql "sync"

    try {
        sql "set read_mor_as_dup_tables = 'fk_raw_version_foreign_child'"

        // The raw child scan exposes both physical versions, but only the current version has a
        // matching current parent. Eliminating the join would incorrectly return parent_id = 1.
        order_qt_raw_foreign_versions """
            select id, parent_id
            from fk_raw_version_foreign_child
            order by id, parent_id
        """

        explain {
            sql """
                shape plan
                select f.id, f.parent_id
                from fk_raw_version_foreign_parent p
                inner join fk_raw_version_foreign_child f on p.id = f.parent_id
                order by f.id, f.parent_id
            """
            contains "INNER_JOIN"
        }

        order_qt_raw_foreign_join """
            select f.id, f.parent_id
            from fk_raw_version_foreign_parent p
            inner join fk_raw_version_foreign_child f on p.id = f.parent_id
            order by f.id, f.parent_id
        """
    } finally {
        sql "set read_mor_as_dup_tables = ''"
    }
}
