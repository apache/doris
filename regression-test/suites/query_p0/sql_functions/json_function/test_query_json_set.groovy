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

suite("test_query_json_set", "query") {
    qt_sql "select json_set('{\"a\": 1, \"b\": [2, 3]}', '\$', null);"
    qt_sql "select json_set('{\"k\": [1, 2]}', '\$.k[0]', null, '\$.[1]', null);"
    def tableName = "test_query_json_set"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `id` int(11) not null,
              `time` datetime,
              `k` int(11)
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`,`time`,`k`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """

    sql "insert into ${tableName} values(1,'2022-01-01 11:45:14',9);"
    sql "insert into ${tableName} values(2,'2022-01-01 11:45:14',null);"
    sql "insert into ${tableName} values(3,null,9);"
    sql "insert into ${tableName} values(4,null,null);"
    qt_sql1 "select json_set('{\"id\": 0, \"time\": \"1970-01-01 00:00:00\", \"a1\": [1, 2], \"a2\": [1, 2]}', '\$.id', id, '\$.time', time, '\$.a1[1]', k, '\$.a2[3]', k) from ${tableName} order by id;"
    sql "DROP TABLE ${tableName};"

    // test json_set with complex type
    // array
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array('"aaa"','"bbb"')); """
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array('aaa','bbb')); """
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array(1,2)); """
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array(1.1,2.2)); """
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array(1.1,2)); """
    qt_sql_array """ SELECT json_set('{"arr": null}', '\$.arr', array(cast(1 as decimal), cast(1.2 as decimal))); """
    // map
    qt_sql_map """ SELECT json_set('{"map": null}', '\$.map', map('a', 'b', 'c', 'd')); """
    qt_sql_map """ SELECT json_set('{"map": null}', '\$.map', map('a', 1, 'c', 2)); """
    qt_sql_map """ SELECT json_set('{"map": null}', '\$.map', map('a', 1.1, 'c', 2.2)); """
    qt_sql_map """ SELECT json_set('{"map": null}', '\$.map', map('a', 1.1, 'c', 2)); """
    qt_sql_map """ SELECT json_set('{"map": null}', '\$.map', map('a', cast(1 as decimal), 'c', cast(1.2 as decimal))); """
    // struct
    qt_sql_struct """ SELECT json_set('{"struct": null}', '\$.struct', named_struct('name', 'a', 'age', 1)); """
    qt_sql_struct """ SELECT json_set('{"struct": null}', '\$.struct', named_struct('name', 'a', 'age', 1.1)); """
    qt_sql_struct """ SELECT json_set('{"struct": null}', '\$.struct', named_struct('name', 'a', 'age', cast(1 as decimal))); """
    // json
    qt_sql_json """ SELECT json_set('{"json": null}', '\$.json', cast('{\"a\":\"b\"}' as JSON)); """
    qt_sql_json """ SELECT json_set('{"json": null}', '\$.json', cast('{\"a\":1}' as JSON)); """
    qt_sql_json """ SELECT json_set('{"json": null}', '\$.json', cast('{\"a\":1.1}' as JSON)); """

    // test with table
    tableName = "test_query_json_set_complex"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `k0` int(11) not null,
              `k1` array<string> NULL,
              `k2` map<string, string> NULL,
              `k3` struct<name:string, age:int> NULL,
              `k4` json NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k0`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    sql "insert into ${tableName} values(1,null,null,null,null);"
    sql "insert into ${tableName} values(2, array('a','b'), map('a','b'), named_struct('name','a','age',1), '{\"a\":\"b\"}');"
    sql """insert into ${tableName} values(3, array('"a"', '"b"'), map('"a"', '"b"', '"c"', '"d"'), named_struct('name','"a"','age', 1), '{\"c\":\"d\"}');"""
    sql """insert into ${tableName} values(4, array(1,2), map(1,2), named_struct('name', 2, 'age',1), '{\"a\":\"b\"}');"""
    sql """insert into ${tableName} values(5, array(1,2,3,3), map(1,2,3,4), named_struct('name',\"a\",'age',1), '{\"a\":\"b\"}');"""
    qt_sql2 """select json_set('{"data": {"array": null, "map": null, "struct": null, "json": null}}', 
                              '\$.data.array', k1, 
                              '\$.data.map', k2, 
                              '\$.data.struct', k3, 
                              '\$.data.json', k4) from ${tableName} order by k0;"""
    sql "DROP TABLE ${tableName};"
}
