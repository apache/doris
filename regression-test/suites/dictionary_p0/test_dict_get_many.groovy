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

suite("test_dict_get_many") {
    sql "drop database if exists test_dict_get_many"
    sql "create database test_dict_get_many"
    sql "use test_dict_get_many"

    sql """
        create table if not exists multi_key_table(
            k0 int not null,
            k1 varchar not null,
            k2 float not null,
            k3 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into multi_key_table values(1, 'abc', 1.1, 'def');""" 
    sql """insert into multi_key_table values(2, 'ABC', 2.1, 'DEF');"""    

    sql """
        create dictionary single_key_dict using multi_key_table
        (
            k0 KEY,
            k1 VALUE,
            k3 VALUE
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
    waitAllDictionariesReady()

    qt_sql1 """ select dict_get("test_dict_get_many.single_key_dict", "k1", 1)  ,  dict_get("test_dict_get_many.single_key_dict", "k1", 2),dict_get("test_dict_get_many.single_key_dict", "k1", 3) """  
    qt_sql2 """ select dict_get("test_dict_get_many.single_key_dict", "k3", 1)  ,  dict_get("test_dict_get_many.single_key_dict", "k3", 2),dict_get("test_dict_get_many.single_key_dict", "k3", 3) """  
    qt_sql3 """ select dict_get_many("test_dict_get_many.single_key_dict", ["k1","k3"], struct(1)) , dict_get_many("test_dict_get_many.single_key_dict", ["k1","k3"], struct(2)) ,  dict_get_many("test_dict_get_many.single_key_dict", ["k1","k3"], struct(3))  """  
    qt_sql4 """ select dict_get_many("test_dict_get_many.multi_key_dict", ["k2","k3"], struct(2,'ABC')); """  
}
