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

suite('load', 'p0,restart_fe') {
    sql "drop database if exists test_dictionary_upgrade"
    sql "create database test_dictionary_upgrade"
    sql "use test_dictionary_upgrade"

    // IP_TRIE base table
    sql """
        create table ip_base(
            ip varchar(32) not null,
            region varchar(64) not null,
            isp varchar(32) not null
        )
        DISTRIBUTED BY HASH(`ip`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // HASH_MAP base table
    sql """
        create table user_base(
            user_id varchar(32) not null,
            user_name varchar(64) not null,
            age int not null
        )
        DISTRIBUTED BY HASH(`user_id`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // IP_TRIE dictionary
    sql """
        create dictionary ip_dict using ip_base
        (
            ip KEY,
            region VALUE,
            isp VALUE
        )LAYOUT(IP_TRIE)
            properties('data_lifetime'='600');
    """

    // HASH_MAP dictionary
    sql """
        create dictionary user_dict using user_base
        (
            user_id KEY,
            user_name VALUE,
            age VALUE
        )LAYOUT(HASH_MAP)
            properties('data_lifetime'='600');
    """

    // third base table for IP_TRIE
    sql """
        create table area_base(
            area_ip varchar(128) not null,
            city varchar(64) not null,
            country varchar(32) not null
        )
        DISTRIBUTED BY HASH(`area_ip`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql "insert into area_base values('2001:0db8:85a3:0000:0000:8a2e:0370:7334/128', 'Beijing', 'CN')"

    // forth base table for HASH_MAP
    sql """
        create table product_base(
            product_id varchar(32) not null,
            product_name varchar(64) not null,
            price decimal(10,2) not null
        )
        DISTRIBUTED BY HASH(`product_id`) BUCKETS auto
        properties("replication_num" = "1");
    """

    // third dictionary with IP_TRIE layout
    sql """
        create dictionary area_dict using area_base
        (
            area_ip KEY,
            city VALUE,
            country VALUE
        )LAYOUT(IP_TRIE)
            properties('data_lifetime'='600');
    """

    // forth dictionary with HASH_MAP layout
    sql """
        create dictionary product_dict using product_base
        (
            product_id KEY,
            product_name VALUE,
            price VALUE
        )LAYOUT(HASH_MAP)
            properties('data_lifetime'='600');
    """

    // check dictionaries number
    def dict_res = sql "show dictionaries"
    log.info("After creating all dictionaries: " + dict_res.toString())
    assertTrue(dict_res.size() == 4)

    // delete and validate dictionaries number
    sql "drop dictionary ip_dict"
    sql "drop dictionary product_dict"

    dict_res = sql "show dictionaries"
    log.info("After dropping dictionaries: " + dict_res.toString())
    assertTrue(dict_res.size() == 2)

    // validate dict names
    def remaining_dicts = dict_res.collect { it[1] }
    assertTrue(remaining_dicts.contains("user_dict"))
    assertTrue(remaining_dicts.contains("area_dict"))
}