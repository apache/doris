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

suite("test_dict_get_nullable") {
    sql "drop database if exists test_dictionary_function"
    sql "create database test_dictionary_function"
    sql "use test_dictionary_function"

    sql """
        create table if not exists multi_key_table(
            k0 int not null,
            k1 varchar not null,
            k2 int  null,
            k3 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into multi_key_table values(1, 'abc', null, 'def');""" 
    sql """insert into multi_key_table values(2, 'ABC', 2, 'DEF');"""    
    sql """
        create dictionary single_key_dict using multi_key_table
        (
            k0 KEY,
            k2 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    sql """
        create dictionary multi_key_dict using multi_key_table
        (
            k0 KEY,
            k1 KEY,
            k2 VALUE,
            k3 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    sql """
        create table if not exists ip_trie_table(
            id int not null,    
            k0 string not null,
            k1 string null
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into ip_trie_table values(1,"192.168.1.0/24" , 'A');"""      
    sql """insert into ip_trie_table values(2,"192.168.1.128/25" , null);"""   
    sql """
        create dictionary ip_trie_dict using ip_trie_table
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """
    waitAllDictionariesReady()

    qt_sql1 """ select dict_get("test_dictionary_function.single_key_dict", "k2", null) """  
    qt_sql2 """ select dict_get("test_dictionary_function.single_key_dict", "k2", 1) """ 
    qt_sql3 """ select dict_get("test_dictionary_function.single_key_dict", "k2", 3) """  
    qt_sql4 """ select dict_get("test_dictionary_function.single_key_dict", "k2", 2) """ 

    qt_sql5 """ select dict_get("test_dictionary_function.ip_trie_dict", "k1", cast("avc" as ipv4)) """  
    qt_sql6 """ select dict_get("test_dictionary_function.ip_trie_dict", "k1", cast("192.168.1.128" as ipv4)) """ 
    qt_sql7 """ select dict_get("test_dictionary_function.ip_trie_dict", "k1", cast("192.168.1.0" as ipv4)) """  
    qt_sql8 """ select dict_get("test_dictionary_function.ip_trie_dict", "k1", cast("125.168.1.0" as ipv4))""" 

    qt_sql9 """ select dict_get_many("test_dictionary_function.multi_key_dict", ["k2","k3"], struct(null,null)) """  
    qt_sql10 """ select dict_get_many("test_dictionary_function.multi_key_dict", ["k2","k3"], struct(null,'abc')) """  
    qt_sql11 """ select dict_get_many("test_dictionary_function.multi_key_dict", ["k2","k3"], struct(1,'abc')) """  
    qt_sql12 """ select dict_get_many("test_dictionary_function.multi_key_dict", ["k2","k3"], struct(3,'abc')) """  
    qt_sql13 """ select dict_get_many("test_dictionary_function.multi_key_dict", ["k2","k3"], struct(2,'ABC')) """  
}