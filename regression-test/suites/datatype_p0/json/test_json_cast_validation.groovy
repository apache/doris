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

suite("test_json_cast_validation", "p0") {
    sql "drop table if exists test_json_cast_validation_src"
    sql """
        create table test_json_cast_validation_src (
            id int,
            s varchar(30)
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_json_cast_validation_src values
            (1, 'null'),
            (2, 'nul'),
            (3, '[nul]'),
            (4, '{\"x\":nul}'),
            (5, '01'),
            (6, '-01'),
            (7, '1.'),
            (8, '18446744073709551616')
    """

    sql "set enable_strict_cast = false"
    order_qt_non_strict """
        select id, cast(s as json), cast(s as json) is null
        from test_json_cast_validation_src
        order by id
    """

    sql "drop table if exists test_json_cast_validation_sink"
    sql """
        create table test_json_cast_validation_sink (
            id int,
            j json
        ) duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_json_cast_validation_sink
        select id, cast(s as json) from test_json_cast_validation_src
    """
    order_qt_persisted """
        select id, j, j is null
        from test_json_cast_validation_sink
        order by id
    """

    sql "truncate table test_json_cast_validation_sink"
    sql "set enable_strict_cast = true"
    test {
        sql "select cast(s as json) from test_json_cast_validation_src where id = 2"
        exception "Failed to parse json string"
    }
    test {
        sql """
            insert into test_json_cast_validation_sink
            select id, cast(s as json) from test_json_cast_validation_src
        """
        exception "Failed to parse json string"
    }
    qt_strict_insert_atomic "select count(*) from test_json_cast_validation_sink"
}
