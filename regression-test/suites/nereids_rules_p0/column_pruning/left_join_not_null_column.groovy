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

suite("left_join_not_null_column") {
    sql "set enable_prune_nested_column = true"

    sql "drop table if exists left_join_not_null_dim"
    sql "drop table if exists left_join_not_null_fact"

    sql """
        create table left_join_not_null_dim (
            id int not null,
            segment varchar(32) not null
        )
        unique key(id)
        distributed by hash(id) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql """
        create table left_join_not_null_fact (
            fact_id int not null,
            dim_id int not null,
            quantity int not null,
            amount decimal(10, 2) not null
        )
        unique key(fact_id)
        distributed by hash(fact_id) buckets 1
        properties (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """

    sql "insert into left_join_not_null_dim values (1, 'segment-a'), (2, 'segment-b')"
    sql "insert into left_join_not_null_fact values (1, 1, 10, 100.00), (2, 2, 20, 200.00), (3, 3, 30, 300.00)"

    explain {
        sql """
            select count(*), sum(f.fact_id), sum(f.quantity), sum(f.amount),
                   sum(case when d.segment is null then 1 else 0 end)
            from left_join_not_null_fact f
            left join left_join_not_null_dim d on f.dim_id = d.id
        """
        notContains "segment.NULL"
    }

    qt_left_join_not_null_column """
        select count(*), sum(f.fact_id), sum(f.quantity), sum(f.amount),
               sum(case when d.segment is null then 1 else 0 end)
        from left_join_not_null_fact f
        left join left_join_not_null_dim d on f.dim_id = d.id
    """
}
