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

suite("fix-overflow") {
    sql "set check_overflow_for_decimal=true;"
    sql "drop table if exists fix_overflow_l;"
    sql """
        create table fix_overflow_l(k1 decimalv3(38,1)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql "drop table if exists fix_overflow_r;"
    sql """
        create table fix_overflow_r(k1 decimalv3(38,1)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into fix_overflow_l values(1.1);
    """
    qt_sql """
        select l.k1, r.k1, l.k1 * r.k1 from fix_overflow_l l left join fix_overflow_r r on l.k1=r.k1;
    """

    sql """
        drop TABLE if exists `fix_overflow_null1`;
    """
    sql """
    CREATE TABLE `fix_overflow_null1`
        (
            `country`             varchar(20),
            `financing_amount`    decimalv3(20, 2)
        ) 
        DISTRIBUTED BY HASH(`country`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    sql """
        insert into fix_overflow_null1 values("a", null), ("b", 976109703976856034.13);
    """

    sql """
        drop TABLE if exists `fix_overflow_null2`;
    """
    sql """
    CREATE TABLE `fix_overflow_null2`
        (
            `country`             varchar(20),
            `financing_amount`    decimalv3(20, 2)
        ) 
        DISTRIBUTED BY HASH(`country`) BUCKETS 10
        PROPERTIES (
        "replication_num" = "1"
        );
    """

    sql """
    insert into fix_overflow_null2
        select
            *
        from
            (
                with temp as (
                    select
                        sum(financing_amount) / 100000000 as amount,
                        country
                    from
                        fix_overflow_null1
                    group by
                        country
                )
                select
                    t1.country,
                    IF(
                        1 + (t1.amount - t2.amount) / ABS(t2.amount) > 0,
                        POW(
                            1 + (t1.amount - t2.amount) / ABS(t2.amount),
                            1 / 5
                        ) - 1,
                        - POW(
                            -(
                                1 + (t1.amount - t2.amount) / ABS(t2.amount)
                            ),
                            1 / 5
                        ) - 1
                    ) as past_5_cagr
                from
                    temp t1
                    left join temp t2 on
                    t2.country = t1.country
            ) as ret;
    """
    qt_select_insert """
        select * from fix_overflow_null2 order by 1,2;
    """

    sql """
        drop table if exists fix_overflow_null3;
    """
    sql """
        create table fix_overflow_null3(k1 decimalv3(38, 6), k2 double, k3 double) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into fix_overflow_null3 values (9.9, -1, null);
    """
    qt_select_fix_overflow_float_null1 """
        select cast(pow(k2+k3, 0.2) as decimalv3(38,6)) from fix_overflow_null3;
    """

    sql """
        drop table if exists fix_overflow_null4
    """
    sql """
        create table fix_overflow_null4(k1 int, k2 int, k3 decimalv3(38,6)) distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into fix_overflow_null4 values (1, null, 99999999999999999999999999999999.999999);
    """
    qt_select_fix_overflow_int_null1 """
        select k1 + k2 + k3 from fix_overflow_null4;
    """
    qt_select_fix_overflow_int_null2 """
        select cast( (k1 + k2) as decimalv3(3, 0) ) from fix_overflow_null4;
    """

    sql """
        drop table if exists fix_overflow_null5
    """
    sql """
        create table fix_overflow_null5(k1 int, k2 int, k3 decimalv3(38,6))
            distributed by hash(k1) properties("replication_num"="1");
    """
    sql """
        insert into fix_overflow_null5 values (-1, null, 99999999999999999999999999999999.999999);
    """
    qt_select_fix_overflow_bool_null1 """
        select (k1 < k2) + k3 from fix_overflow_null5;
    """
}
