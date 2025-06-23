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

suite("test_ddl") {
    sql "drop database if exists test_dictionary_ddl"
    sql "create database test_dictionary_ddl"
    sql "use test_dictionary_ddl"

    sql """
        create table dc(
            k0 datetime(6) not null,
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """

    test { // wrong grammar. no using
        sql """
        create dictionary dic1
        (
            col1 KEY,
            col2 VALUE,
            col3 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "mismatched input"
    }

    test { // wrong grammar. no properties keyword
        sql """
        create dictionary dic1 using dc
        (
            col1 KEY,
            col2 VALUE,
            col3 VALUE
        )LAYOUT(HASH_MAP)
        ('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "mismatched input"
    }

    test { // no source table
        sql """
        create dictionary dic1 using dcxxx
        (
            col1 KEY,
            col2 VALUE,
            col3 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "Unknown table"
    }

    test { // wrong column name
        sql """
        create dictionary dic1 using dc
        (
            col1 KEY,
            col2 VALUE,
            col3 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "Column col1 not found in source table dc"
    }

    test { // wrong column type
        sql """
        create dictionary dic1 using dc
        (
            k0 KEY,
            k1 VALUE,
            k1 VARCHAR
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "mismatched input 'VARCHAR'"
    }

    test { // duplicate columns
        sql """
        create dictionary dic1 using dc
        (
            k1 KEY,
            k0 VALUE,
            k0 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "Column k0 is used more than once"
    }

    // complex type
    sql """
        create table ctype(
            k0 int null,
            k1 MAP<STRING, INT>
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    test {
        sql """
        create dictionary dic1 using ctype
        (
            k1 KEY,
            k0 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
        """
        exception "Key column k1 cannot be complex type"
    }

    // nullable column test base table
    sql """
        create table nullable_table(
            k1 varchar(32) null,
            k2 varchar(32) not null,
            v1 varchar(64) null,
            v2 varchar(64) not null
        )
        DISTRIBUTED BY HASH(`k2`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // test IP_TRIE layout requires exactly one key column
    test {
        sql """
        create dictionary dic_null using nullable_table
        (
            k1 KEY,
            k2 KEY,
            v1 VALUE,
            v2 VALUE
        )LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
        """
        exception "IP_TRIE layout requires exactly one key column"
    }

    // test right nullable
    sql """
    create dictionary dic_not_null using nullable_table
    (
        k2 KEY,
        v2 VALUE
    )LAYOUT(IP_TRIE)
    properties('data_lifetime'='600');
    """
    sql "drop dictionary dic_not_null"

    test { // wrong type
        sql """
        create dictionary dic1 using dc
        (
            k1 KEY, 
            k0 VALUE
        )LAYOUT(xxx)
        properties('data_lifetime'='600');
        """
        exception "Unknown layout type: xxx. must be IP_TRIE or HASH_MAP"
    }

    test { // wrong type for ip_trie
        sql """
        create dictionary dic_trie2 using dc
        (
            k0 KEY,
            k1 VALUE
        )LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
        """
        exception "Key column k0 must be String type for IP_TRIE layout"
    }

    test { // no data_lifetime
        sql """
        create dictionary dic_trie using dc
        (
            k1 KEY,
            k0 VALUE
        )LAYOUT(IP_TRIE)
        properties('x' = '1');
        """
        exception "Property 'data_lifetime' is required"
    }

    // normal
    sql """
        create dictionary dic1 using dc
        (
            k1 KEY, 
            k0 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
    """
    waitAllDictionariesReady()

    // normal ip_trie
    sql """
        create dictionary dic_trie using dc
        (
            k1 KEY, 
            k0 VALUE
        )LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """

    test { // duplicate dictionary
        sql """
        create dictionary dic1 using dc
        (
            k1 KEY, 
            k0 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600', "x"="x", "y"="y");
        """
        exception "Dictionary dic1 already exists in database test"
    }

    qt_sql1 "explain dictionary dic1"
    qt_sql2 "explain dictionary dic_trie"

    // test drop
    sql "drop dictionary dic1"
    def origin_res = sql "show dictionaries"
    assertEquals(origin_res.size(), 1)

    // drop databases
    sql "use mysql"
    sql "drop database test_dictionary_ddl"
    sql "create database test_dictionary_ddl"
    sql "use test_dictionary_ddl"
    origin_res = sql "show dictionaries"
    log.info(origin_res.toString())
    assertEquals(origin_res.size(), 0) // should also be removed

    // test multiple key columns
    sql """
        create table multi_key_table(
            k1 varchar(64) not null,
            k2 decimal(15, 9) not null,
            k3 int not null,
            v1 datetime not null
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql "insert into multi_key_table values ('1', 1.1, 1, '2020-01-01 00:00:00'), ('2', 2.2, 2, '2020-02-02 00:00:00')"

    sql """
        create dictionary dic_multi_key using multi_key_table
        (
            k1 KEY,
            v1 VALUE,
            k2 KEY,
            k3 KEY
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    // test refresh result for multiple key columns dictionary
    waitAllDictionariesReady()
    def refresh_res = (sql "show dictionaries")[0]
    assertTrue(refresh_res[1] == "dic_multi_key" && refresh_res[4] == "NORMAL")

    // test no key columns error
    test {
        sql """
        create dictionary dic_no_key using multi_key_table
        (
            v1 VALUE
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
        """
        exception "Need at least one key column"
    }

    // test no value columns error
    test {
        sql """
        create dictionary dic_no_value using multi_key_table
        (
            k1 KEY,
            k2 KEY
        )LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
        """
        exception "Need at least one value column"
    }

    // test show with conditions
    def like_result = sql "show dictionaries like '%_multi_ke%' "
    assertEquals(like_result.size(), 1)
    like_result = sql "show dictionaries like '%no_key%' "
    assertEquals(like_result.size(), 0)
    like_result = sql "show dictionaries where dic_multi_key "
    assertEquals(like_result.size(), 1)
    like_result = sql "show dictionaries where 1 "
    assertEquals(like_result.size(), 0)
}