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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_variant_cast", "p0") {
    qt_sql1 """select cast(cast('{"a" : 1}' as variant) as jsonb);"""
    qt_sql2 """select json_type(cast(cast('{"a" : 1}' as variant) as jsonb), "\$.a");"""
    sql "DROP TABLE IF EXISTS var_cast"
    sql """
        CREATE TABLE `var_cast` (
            `k` int NULL,
            `var` variant NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into var_cast values (1, '{"aaa" : 1}')"""
    qt_sql3 "select cast(var as json) from var_cast"
    sql """insert into var_cast values (1, '[1]')"""
    qt_sql4 "select cast(var as json) from var_cast"
    sql """insert into var_cast values (1, '123')"""
    qt_sql5 "select cast(var as json) from var_cast"

    sql "DROP TABLE IF EXISTS var_not_null_cast"
    sql """
        CREATE TABLE `var_not_null_cast` (
            `k` int NULL,
            `var` variant NOT NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into var_not_null_cast values (1, '{"aaa" : 1}')"""
    //qt_sql6 "select cast(var as json) from var_not_null_cast"
    sql """insert into var_not_null_cast values (1, '[1]')"""
    //qt_sql7 "select cast(var as json) from var_not_null_cast"
    sql """insert into var_not_null_cast values (1, '123')"""
    //qt_sql8 "select cast(var as json) from var_not_null_cast"
    sql """insert into var_not_null_cast values (1, '{"aaa" : "aaa"}')"""
    qt_sql9 "select * from var_not_null_cast where cast(var['aaa'] as int) is null"

    sql "DROP TABLE IF EXISTS var_cast_decimal"
    sql """
        CREATE TABLE `var_cast_decimal` (
            `k` int NULL,
            `var` variant<'aaa': decimal(10, 2), properties("variant_enable_typed_paths_to_sparse" = "false")> NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """insert into var_cast_decimal values (1, '{"aaa" : 1.23}')"""
    qt_sql10 "select * from var_cast_decimal where cast(var['aaa'] as decimal(10, 1)) = 1.2"
}