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

suite("test_dict_get") {
    sql "drop database if exists test_dict_get"
    sql "create database test_dict_get"
    sql "use test_dict_get"

    sql """
        create table dc(
            k0 datetime(6) not null,
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """
    sql """insert into dc values('2020-12-12', 'abc');"""

    sql """
        create dictionary dic1 using dc
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    explain {
        sql """select dict_get("test_dict_get.dic1", "k1", "2020-12-12")"""
        verbose true
        notContains "type=datetimev2(6)"
        contains "type=varchar(65533)"
    }
    test {
        sql """select dict_get("test_dict_get.dic1", "k0", "2020-12-12")"""
        exception "Can't ask for key k0 by dict_get()"
    }

    sql """
        create dictionary dic2 using dc
        (
            k1 KEY,
            k0 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    explain {
        sql """select dict_get("test_dict_get.dic2", "k0", "abc")"""
        verbose true
        contains "type=datetimev2(6)"
        notContains "type=varchar(65533)"
    }


    sql """
        create dictionary dic_ip_trie using dc
        (
            k1 KEY,
            k0 VALUE
        )
        LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """
    explain {
        sql """select dict_get("test_dict_get.dic_ip_trie", "k0", cast("127.0.0.1" as ipv4))"""
        verbose true
        contains "type=datetimev2(6)"
        notContains "type=varchar(65533)"
    }
}