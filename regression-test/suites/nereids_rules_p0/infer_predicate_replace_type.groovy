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

suite("infer_predicate_replace_type") {
    sql "drop table if exists infer_replace_date_l"
    sql """
        create table infer_replace_date_l (id int not null, d date not null)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")
    """
    sql "drop table if exists infer_replace_date_r"
    sql """
        create table infer_replace_date_r (id int not null, ts datetime(0) not null)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")
    """
    sql "insert into infer_replace_date_l values (1,'2024-01-01')"
    sql "insert into infer_replace_date_r values (10,'2024-01-01 00:00:00')"

    order_qt_date_length """
        select l.id, r.id from infer_replace_date_l l join infer_replace_date_r r on l.d = r.ts
        where length(cast(l.d as string)) = 10
    """
    order_qt_datetime_length """
        select l.id, r.id from infer_replace_date_l l join infer_replace_date_r r on l.d = r.ts
        where length(cast(r.ts as string)) = 19
    """
    order_qt_date_same_type """
        select l.id, r.id from infer_replace_date_l l join infer_replace_date_l r on l.d = r.d
        where length(cast(l.d as string)) = 10
    """
    order_qt_date_comparison """
        select l.id, r.id from infer_replace_date_l l join infer_replace_date_r r on l.d = r.ts
        where l.d > cast('2023-12-31' as date)
    """

    sql "drop table if exists infer_replace_fp"
    sql """
        create table infer_replace_fp (id bigint not null, x double not null, y double not null)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")
    """
    sql """
        insert into infer_replace_fp values
        (1,cast('-0.0' as double),cast('0.0' as double)),
        (2,cast('0.0' as double),cast('-0.0' as double)),
        (3,2,2),(4,0,0),(5,-2,-2)
    """
    order_qt_signed_zero_facts """
        select id, x = y, signbit(x), signbit(y) from infer_replace_fp
    """
    order_qt_signed_zero_or """
        select id from infer_replace_fp where x = y and (signbit(x) or x > cast(1 as double))
    """
    order_qt_signed_zero_or_reversed """
        select id from infer_replace_fp where x = y and (signbit(y) or y > cast(1 as double))
    """
    order_qt_float_comparison """
        select id from infer_replace_fp where x = y and x > cast(1 as double)
    """

    sql "drop table if exists infer_replace_decimal_l"
    sql """
        create table infer_replace_decimal_l (id int not null, d decimal(9,2) not null)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")
    """
    sql "drop table if exists infer_replace_decimal_r"
    sql """
        create table infer_replace_decimal_r (id int not null, d decimal(9,3) not null)
        duplicate key(id) distributed by hash(id) buckets 1 properties("replication_num"="1")
    """
    sql "insert into infer_replace_decimal_l values (1,1.20)"
    sql "insert into infer_replace_decimal_r values (10,1.200)"
    order_qt_decimal_scale """
        select l.id, r.id from infer_replace_decimal_l l join infer_replace_decimal_r r on l.d = r.d
        where length(cast(l.d as string)) = 4
    """
}
