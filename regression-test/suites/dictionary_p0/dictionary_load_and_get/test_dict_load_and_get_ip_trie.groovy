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

import static java.util.concurrent.TimeUnit.SECONDS

import org.awaitility.Awaitility

suite("test_dict_load_and_get_ip_trie") {
    def dbName  = "test_dict_load_and_get_ip_trie_db"  

    sql "drop database if exists ${dbName}"
    sql "create database ${dbName}"
    sql "use ${dbName}"


     sql """
        create table single_key_with_duplicate(
            k0 int not null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into single_key_with_duplicate values(1, '192.168.1.0/24');"""  

    sql """insert into single_key_with_duplicate values(2, '192.168.1.0/24');"""  


    sql """
        create dictionary dc_single_key_with_duplicate using single_key_with_duplicate
        (
            k1 KEY,
            k0 VALUE
        )
        LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """
    
    for (int _ = 0; _ < 30 ; _++)
    {
        try {
            sql "refresh dictionary dc_single_key_with_duplicate"
            assert false
        } catch (Exception e) {
            if (e.getMessage().contains("The CIDR has duplicate data in IpAddressDictionary")) {
                break;
            } else {
                logger.info("refresh dictionary dc_single_key_with_duplicate failed: " + e.getMessage())
            }
        }
        assertTrue(_ < 30, "refresh dictionary dc_single_key_with_duplicate failed")
        sleep(1000)
    }

    sql """
        create table ip_trie_table(
            id int not null,    
            cidr string not null , 
            int_not_null int not null ,
            int_null int null   
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS auto
        properties("replication_num" = "1");
    """     

    sql """insert into ip_trie_table values(1,"192.168.1.0/24", 1000, 1);"""       
    sql """insert into ip_trie_table values(2,"192.168.1.128/25", 100, null);"""      

    sql """
        create dictionary ip_trie_dict using ip_trie_table
        (
            cidr KEY,
            int_not_null VALUE
        )
        LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """
    waitDictionaryReady("ip_trie_dict")

    sql """ refresh dictionary ip_trie_dict; """        

    qt_sql """ select dict_get("${dbName}.ip_trie_dict", "int_not_null", cast("192.168.1.128" as ipv4)) """    
    qt_sql """ select dict_get("${dbName}.ip_trie_dict", "int_not_null", cast("192.168.1.128" as ipv4)) """    
    qt_sql """ select dict_get("${dbName}.ip_trie_dict", "int_not_null", cast("192.168.1.0" as ipv4))"""
    qt_sql """ select dict_get("${dbName}.ip_trie_dict", "int_not_null", cast("125.168.1.0" as ipv4))"""   
}