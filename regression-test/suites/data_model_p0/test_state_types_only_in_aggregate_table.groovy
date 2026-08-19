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

suite("test_state_types_only_in_aggregate_table") {
    sql "set enable_agg_state=true"

    sql "drop table if exists state_type_dup_hll"
    test {
        sql """
            create table state_type_dup_hll (
                k int,
                v hll not null
            ) duplicate key(k)
            distributed by hash(k) buckets 1
            properties("replication_num" = "1")
        """
        exception "type is only supported in aggregate key tables"
    }

    sql "drop table if exists state_type_unique_quantile"
    test {
        sql """
            create table state_type_unique_quantile (
                k int,
                v quantile_state not null
            ) unique key(k)
            distributed by hash(k) buckets 1
            properties("replication_num" = "1")
        """
        exception "type is only supported in aggregate key tables"
    }

    sql "drop table if exists state_type_dup_agg_state"
    test {
        sql """
            create table state_type_dup_agg_state (
                k int,
                v agg_state<sum(int not null)> generic
            ) duplicate key(k)
            distributed by hash(k) buckets 1
            properties("replication_num" = "1")
        """
        exception "type is only supported in aggregate key tables"
    }

    sql "drop table if exists state_type_alter_dup"
    sql """
        create table state_type_alter_dup (
            k int,
            v int
        ) duplicate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1")
    """
    test {
        sql "alter table state_type_alter_dup add column h hll not null"
        exception "type is only supported in aggregate key tables"
    }
    test {
        sql "alter table state_type_alter_dup add column q quantile_state not null"
        exception "type is only supported in aggregate key tables"
    }
    test {
        sql "alter table state_type_alter_dup add column a agg_state<sum(int not null)> generic"
        exception "type is only supported in aggregate key tables"
    }

    sql "drop table if exists state_type_aggregate"
    sql """
        create table state_type_aggregate (
            k int,
            h hll hll_union not null,
            q quantile_state quantile_union not null,
            a agg_state<sum(int not null)> generic
        ) aggregate key(k)
        distributed by hash(k) buckets 1
        properties("replication_num" = "1")
    """

    setFeConfigTemporary([allow_non_aggregate_table_state_types: true]) {
        sql "drop table if exists state_type_compatibility_dup"
        sql """
            create table state_type_compatibility_dup (
                k int,
                h hll not null,
                q quantile_state not null,
                a agg_state<sum(int not null)> generic
            ) duplicate key(k)
            distributed by hash(k) buckets 1
            properties("replication_num" = "1")
        """
    }
}
