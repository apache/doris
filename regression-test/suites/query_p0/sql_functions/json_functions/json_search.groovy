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

suite("test_json_search") {
    def dbName = "test_json_search_db"
    List<List<Object>> db = sql """show databases like '${dbName}'"""
    if (db.size() == 0) {
        sql """CREATE DATABASE ${dbName}"""
    }
    sql """use ${dbName}"""

    def testTable = "test_json_search"
    sql """ drop table if exists test_json_search;"""
    sql """
            CREATE TABLE `${testTable}` (
              `id` int NULL,
              `j`  varchar(1000) NULL,
              `jb` json NULL,
              `o`  varchar(1000) NULL,
              `p`  varchar(1000) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
    """
    def jsonValue = """'["A",[{"B":"1"}],{"C":"AB"},{"D":"BC"}]'"""

    sql """insert into ${testTable} values(1, $jsonValue, $jsonValue, NULL, '_%')"""
    sql """insert into ${testTable} values(2, $jsonValue, $jsonValue, 'one', '_%')"""
    sql """insert into ${testTable} values(3, $jsonValue, $jsonValue, 'One', '_%')"""
    sql """insert into ${testTable} values(4, $jsonValue, $jsonValue, 'all', '_%')"""
    sql """insert into ${testTable} values(5, $jsonValue, $jsonValue, 'All', '_%')"""
    sql """insert into ${testTable} values(6, $jsonValue, $jsonValue, 'invalid_one_or_all', '_%')"""
    sql """insert into ${testTable} values(7, NULL, NULL, 'one', '_%')"""
    sql """insert into ${testTable} values(8, $jsonValue, $jsonValue, 'all', NULL)"""
    sql """insert into ${testTable} values(9, $jsonValue, $jsonValue, 'all', 'X')"""

    qt_one_is_valid_or_null """ SELECT id, j, o, p, JSON_SEARCH(j, o, p), JSON_SEARCH(jb, o, p) 
                FROM ${testTable} WHERE o <> 'invalid_one_or_all' ORDER BY id;"""
    test {
        sql """SELECT id, j, o, p, JSON_SEARCH(j, o, p), JSON_SEARCH(jb, o, p) 
                   FROM ${testTable} WHERE o = 'invalid_one_or_all' ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }

    qt_all_const1 """ SELECT JSON_SEARCH($jsonValue, 'one', '__')"""
    qt_all_const1 """ SELECT JSON_SEARCH($jsonValue, 'One', '__')"""
    qt_all_const2 """ SELECT JSON_SEARCH($jsonValue, 'all', '__')"""
    qt_all_const2 """ SELECT JSON_SEARCH($jsonValue, 'All', '__')"""
    qt_all_const3 """ SELECT JSON_SEARCH($jsonValue, 'one', 'A')"""
    qt_all_const4 """ SELECT JSON_SEARCH($jsonValue, 'all', 'A')"""
    qt_all_const5 """ SELECT JSON_SEARCH($jsonValue, 'one', 'A%')"""
    qt_all_const6 """ SELECT JSON_SEARCH($jsonValue, 'one', 'A_')"""
    qt_all_const7 """ SELECT JSON_SEARCH($jsonValue, 'one', 'X')"""
    qt_all_const8 """ SELECT JSON_SEARCH($jsonValue, 'all', 'X')"""

    qt_one_is_one_const """ SELECT id, j, 'one', p, JSON_SEARCH(j, 'one', p), JSON_SEARCH(jb, 'one', p) 
                         FROM ${testTable} ORDER BY id; """
    qt_one_is_all_const """ SELECT id, j, 'all', p, JSON_SEARCH(j, 'all', p), JSON_SEARCH(jb, 'all', p) 
                         FROM ${testTable} ORDER BY id; """
    test {
        sql """SELECT id, JSON_SEARCH(j, 'invalid_one_or_all', p), JSON_SEARCH(jb, 'invalid_one_or_all', p) 
                         FROM ${testTable} ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }

    test {
        sql """SELECT id, JSON_SEARCH(j, o, 'A'), JSON_SEARCH(jb, o, 'A') 
                         FROM ${testTable} WHERE o = 'invalid_one_or_all'  ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }

    test {
        sql """SELECT id, j, o, p, JSON_SEARCH(j, o, NULL), JSON_SEARCH(jb, o, NULL) 
                         FROM ${testTable} WHERE o = 'invalid_one_or_all'  ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }

    qt_one_and_pattern_is_const1 """ SELECT id, j, 'one', 'A', JSON_SEARCH(j, 'one', 'A'), JSON_SEARCH(jb, 'one', 'A') 
                         FROM ${testTable} ORDER BY id; """
    qt_one_and_pattern_is_const2 """ SELECT id, j, 'all', 'A', JSON_SEARCH(j, 'all', 'A'), JSON_SEARCH(jb, 'all', 'A') 
                         FROM ${testTable} ORDER BY id; """

    qt_one_and_pattern_is_nullconst """ SELECT id, j, NULL, NULL, JSON_SEARCH(j, NULL, NULL), JSON_SEARCH(jb, NULL, NULL) 
                         FROM ${testTable} ORDER BY id; """

    test {
        sql """ SELECT id, $jsonValue, o, p, JSON_SEARCH($jsonValue, o, p) FROM ${testTable} 
                     WHERE o = 'invalid_one_or_all' ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }
    qt_json_const1 """ SELECT id, $jsonValue, 'one', p, JSON_SEARCH($jsonValue, 'one', p) FROM ${testTable} ORDER BY id; """
    qt_json_const2 """ SELECT id, $jsonValue, 'all', p, JSON_SEARCH($jsonValue, 'all', p) FROM ${testTable} ORDER BY id; """

    test {
        sql """ SELECT id, JSON_SEARCH($jsonValue, o, 'A') FROM ${testTable} 
                     WHERE o = 'invalid_one_or_all' ORDER BY id;"""
        exception "[INVALID_ARGUMENT]the one_or_all argument invalid_one_or_all is not 'one' not 'all'"
    }

    qt_one_case1 """ SELECT id, $jsonValue, 'One', p, JSON_SEARCH($jsonValue, 'One', p) FROM ${testTable} ORDER BY id; """
    qt_one_case2 """ SELECT id, $jsonValue, 'All', p, JSON_SEARCH($jsonValue, 'One', p) FROM ${testTable} ORDER BY id; """

    sql "drop table ${testTable}"
}
