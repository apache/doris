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

suite("test_query_json_object", "query") {
    def tableName = "test_query_json_object"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE ${tableName} (
              `k0` int(11) not null,
              `k1` int(11) NULL,
              `k2` boolean NULL,
              `k3` varchar(255),
              `k4` datetime
            ) ENGINE=OLAP
            DUPLICATE KEY(`k0`,`k1`,`k2`,`k3`,`k4`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`k0`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
            );
        """
    sql "insert into ${tableName} values(1,null,null,null,null);"
    sql "insert into ${tableName} values(2,1,null,null,null);"
    sql "insert into ${tableName} values(3,null,true,null,null);"
    sql "insert into ${tableName} values(4,null,null,'test','2022-01-01 11:11:11');"
    sql "insert into ${tableName} values(5,1,true,'test','2022-01-01 11:11:11');"
    qt_sql1 "select json_object('k0',k0,'k1',k1,'k2',k2,'k3',k3,'k4',cast(k4 as string),'k5', null,'k6','k6') from ${tableName} order by k0;"
    test {
        sql """select k0,json_object(k3,123) from ${tableName} order by k0;"""
        exception "JSON documents may not contain NULL member name"
    }

    qt_sql2 """select json_object ( CONCAT('k',t.number%30926%3000 + 0),CONCAT('k',t.number%30926%3000 + 0,t.number%1000000) ) v from numbers("number" = "2") t order by cast(v as string);"""
    sql "DROP TABLE ${tableName};"

    // test json_object with complex type
    // literal cases
    // array
    qt_sql_array """ SELECT json_object('id', 1, 'level', array('"aaa"','"bbb"')); """
    qt_sql_array """ SELECT json_object('id', 1, 'level', array('aaa','bbb')); """
    qt_sql_array """ SELECT json_object('id', 1, 'level', array(1,2)); """
    qt_sql_array """ SELECT json_object('id', 1, 'level', array(1.1,2.2)); """
    qt_sql_array """ SELECT json_object('id', 1, 'level', array(1.1,2)); """
    qt_sql_array_decimal """ SELECT /*+ set_var(enable_fold_constant_by_be=0) */ json_object('id', 1, 'level', array(cast(1 as decimal), cast(1.2 as decimal))); """
    qt_sql_array_decimal_fold """ SELECT /*+ set_var(enable_fold_constant_by_be=1) */ json_object('id', 1, 'level', array(cast(1 as decimal), cast(1.2 as decimal))); """
    // struct
    qt_sql_struct """ SELECT json_object('id', 1, 'level', named_struct('name', 'a', 'age', 1)); """
    qt_sql_struct """ SELECT json_object('id', 1, 'level', named_struct('name', 'a', 'age', 1.1)); """
    qt_sql_struct """ SELECT json_object('id', 1, 'level', named_struct('name', 'a', 'age', 1)); """
    qt_sql_struct """ SELECT json_object('id', 1, 'level', named_struct('name', 'a', 'age', 1.1)); """
    // json
    qt_sql_json """ SELECT json_object('id', 1, 'level', cast('{\"a\":\"b\"}' as JSON)); """
    qt_sql_json """ SELECT json_object('id', 1, 'level', cast('{\"a\":1}' as JSON)); """
    qt_sql_json """ SELECT json_object('id', 1, 'level', cast('{\"a\":1.1}' as JSON)); """
    qt_sql_json """ SELECT json_object('id', 1, 'level', cast('{\"a\":1.1}' as JSON)); """
    qt_sql_json """ SELECT json_object('id', 1, 'level', cast('{\"a\":1.1}' as JSON)); """



    tableName = "test_query_json_object_complex"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
            CREATE TABLE test_query_json_object_complex (
              `k0` int(11) not null,
              `k1` array<string> NULL,
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
    sql "insert into ${tableName} values(1,null,null,null);"
    sql "insert into ${tableName} values(2, array('a','b'), named_struct('name','a','age',1), '{\"a\":\"b\"}');"
    sql """insert into ${tableName} values(3, array('"a"', '"b"'),named_struct('name','"a"','age', 1), '{\"c\":\"d\"}');"""
    sql """insert into ${tableName} values(4, array(1,2), named_struct('name', 2, 'age',1), '{\"a\":\"b\"}');"""
    sql """insert into ${tableName} values(5, array(1,2,3,3), named_struct('name',\"a\",'age',1), '{\"a\":\"b\"}');"""
    qt_sql2 "select json_object('k0',k0,'k1',k1,'k3',k3,'k4',k4) from ${tableName} order by k0;"

    qt_json_object1 """ select json_object('k0', null, 'k1', json_parse('{"a":1}'), 'k2', '{"a":1}'); """
    qt_json_object2 """ select json_object('k0', null); """


    test {
        sql """select json_object(null, 123); """
        exception "json_object key can't be NULL"
    }
}
