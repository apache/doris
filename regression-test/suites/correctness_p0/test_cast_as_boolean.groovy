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

suite("test_cast_as_boolean") {
    sql """
        set enable_fold_constant_by_be = false;
    """
    sql """
        drop table if exists tbl_cast_as_boolean;
    """
    
    sql """
        create table if not exists tbl_cast_as_boolean (
            id int,
            val_float float,
            val_double double,
            val_tinyint tinyint,
            val_smallint smallint,
            val_int int,
            val_bigint bigint,
            val_decimal decimal(10, 2)
        ) DISTRIBUTED BY hash(id) PROPERTIES ('replication_num' = '1');
    """

    sql """
        insert into tbl_cast_as_boolean values 
            (0, 0.0, 0.0, 0, 0, 0, 0, 0.0),
            (1, 1.0, 1.0, 1, 1, 1, 1, 1.0),
            (2, 2.0, 2.0, 2, 2, 2, 2, 2.0);
    """

    sql """
        drop table if exists tbl_cast_as_boolean_tmp;
    """
    sql """
        create table tbl_cast_as_boolean_tmp properties("replication_num" = "1") 
        as select
            id, cast(val_float as boolean), cast(val_double as boolean), cast(val_tinyint as boolean),
            cast(val_smallint as boolean), cast(val_int as boolean), cast(val_bigint as boolean),
            cast(val_decimal as boolean) from tbl_cast_as_boolean;
    """
    qt_cast_as_boolean0 """
        select * from tbl_cast_as_boolean_tmp order by id;
    """

    // groovy seems will do convert to boolean automatically, so we need an additional cast to tinyint
    // to make sure the result is correct
    qt_cast_as_boolean1 """
        select id, val_float, cast(val_float as boolean), cast(cast(val_float as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean2 """
        select id, val_double, cast(val_double as boolean), cast(cast(val_double as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean3 """
        select id, val_tinyint, cast(val_tinyint as boolean), cast(cast(val_tinyint as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean4 """
        select id, val_smallint, cast(val_smallint as boolean), cast(cast(val_smallint as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean5 """
        select id, val_int, cast(val_int as boolean), cast(cast(val_int as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean6 """
        select id, val_bigint, cast(val_bigint as boolean), cast(cast(val_bigint as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """
    qt_cast_as_boolean7 """
        select id, val_decimal, cast(val_decimal as boolean), cast(cast(val_decimal as boolean) as tinyint) from tbl_cast_as_boolean order by id;
    """

    qt_implict_cast_as_boolean1 """
        select * from tbl_cast_as_boolean where val_float order by id;
    """
    qt_implict_cast_as_boolean2 """
        select * from tbl_cast_as_boolean where val_double order by id;
    """
    qt_implict_cast_as_boolean3 """
        select * from tbl_cast_as_boolean where val_tinyint order by id;
    """
    qt_implict_cast_as_boolean4 """
        select * from tbl_cast_as_boolean where val_smallint order by id;
    """
    qt_implict_cast_as_boolean5 """
        select * from tbl_cast_as_boolean where val_int order by id;
    """
    qt_implict_cast_as_boolean6 """
        select * from tbl_cast_as_boolean where val_bigint order by id;
    """
    qt_implict_cast_as_boolean7 """
        select * from tbl_cast_as_boolean where val_decimal order by id;
    """

    qt_is_true1 """
        select id, val_float, val_float is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true2 """
        select id, val_double, val_double is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true3 """
        select id, val_tinyint, val_tinyint is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true4 """
        select id, val_smallint, val_smallint is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true5 """
        select id, val_int, val_int is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true6 """
        select id, val_bigint, val_bigint is true from tbl_cast_as_boolean order by id;
    """
    qt_is_true7 """
        select id, val_decimal, val_decimal is true from tbl_cast_as_boolean order by id;
    """

    qt_equeal_true1 """
        select id, val_float, val_float = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true2 """
        select id, val_double, val_double = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true3 """
        select id, val_tinyint, val_tinyint = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true4 """
        select id, val_smallint, val_smallint = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true5 """
        select id, val_int, val_int = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true6 """
        select id, val_bigint, val_bigint = true from tbl_cast_as_boolean order by id;
    """
    qt_equeal_true7 """
        select id, val_decimal, val_decimal = true from tbl_cast_as_boolean order by id;
    """
}
