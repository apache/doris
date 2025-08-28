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

suite("test_dict_load_and_get_hash_map") {
    sql "drop database if exists test_dict_load_and_get_hash_map_db"
    sql "create database test_dict_load_and_get_hash_map_db"
    sql "use test_dict_load_and_get_hash_map_db"

    sql """
        create table single_key_with_duplicate(
            k0 int not null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into single_key_with_duplicate values(1, 'abc');"""  

    sql """insert into single_key_with_duplicate values(1, 'def');"""  


    sql """
        create dictionary dc_single_key_with_duplicate using single_key_with_duplicate
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """ 

    for (int _ = 0; _ < 30 ; _++)
    {
        try {
            sql "refresh dictionary dc_single_key_with_duplicate"
            assert false
        } catch (Exception e) {
            if (e.getMessage().contains("The key has duplicate data in HashMapDictionary")) {
                break;
            } else {
                logger.info("refresh dictionary dc_single_key_with_duplicate failed: " + e.getMessage())
            }
        }
        assertTrue(_ < 30, "refresh dictionary dc_single_key_with_duplicate failed")
        sleep(1000)
    }

    sql """
        create table single_key_without_duplicate(
            k0 int not null,    
            str_not_null string not null,
            str_null string null,
            int_not_null int not null ,
            int_null int null,      
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 


    sql """insert into single_key_without_duplicate values(1, 'abc', 'def', 100, 10000);"""   
    sql """insert into single_key_without_duplicate values(2, 'ABC', null, 200, null);"""    

    sql """
        create dictionary dc_single_key_without_duplicate using single_key_without_duplicate
        (
            k0 KEY,
            str_not_null VALUE,
            str_null VALUE,
            int_not_null VALUE,
            int_null VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitDictionaryReady("dc_single_key_without_duplicate")
    
    qt_sql_constant"""
        select dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_not_null", 1)  ,  
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_not_null", 2),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_not_null", 3),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_not_null", null)         
    """

    qt_sql_constant"""
        select dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_null", 1)  ,  
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_null", 2),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_null", 3),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "str_null", null) 
    """         

    qt_sql_constant"""
        select dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_not_null", 1)  ,  
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_not_null", 2),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_not_null", 3),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_not_null", null)        
    """     

    qt_sql_constant"""
        select dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_null", 1)  ,  
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_null", 2),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_null", 3),
               dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_null", null)        
    """     


    sql """
        create table query_table(
            int_not_null int not null,    
            int_null int  null
        )       
         DISTRIBUTED BY HASH(`int_not_null`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into query_table values(1, 1);"""  // 1 can found value not null  
    sql """insert into query_table values(2, null);"""  // input null can found value null  
    sql """insert into query_table values(3, 2);"""  // 2 can found value not null  
     sql """insert into query_table values(3, 3);"""  // 3 can not found return null

    qt_sql_not_constant"""
        select int_not_null , dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_not_null", int_not_null) from query_table order by int_not_null       
    """     
    qt_sql_not_constant"""
        select int_null ,  dict_get("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", "int_null", int_null) from query_table order by int_null       
    """     


    qt_sql_many """
        select int_not_null , dict_get_many("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(int_not_null)) from query_table order by int_not_null          
    """     

    qt_sql_many """
        select int_null , dict_get_many("test_dict_load_and_get_hash_map_db.dc_single_key_without_duplicate", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(int_null)) from query_table order by int_null          
    """     


    sql """
        create table multi_key_table(
            k0 int not null,
            k1 float not null,
            k2 varchar not null,
            int_not_null int not null ,
            int_null int null   ,
            str_not_null string not null,   
            str_null string null    
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """    

    // insert data
    sql """insert into multi_key_table values(1, 1.0, 'abc', 100, 10000, 'abc', 'def');"""  
    sql """insert into multi_key_table values(2, 2.0, 'ABC', 200, null, 'ABC', null);"""    

    // create dictionary

    sql """
        create dictionary dc_multi_key_table using multi_key_table
        (
            k0 KEY,
            k1 KEY,
            k2 KEY,
            int_not_null VALUE,
            int_null VALUE,
            str_not_null VALUE,
            str_null VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """
    waitDictionaryReady("dc_multi_key_table")  

    qt_sql_constant"""
        select dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(1, 1.0, 'abc'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(2, 2.0, 'ABC'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(3, 3.0, 'ABC'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(1, 1.0, 'ABC'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(2, 2.0, 'abc'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(3, 3.0, 'abc'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(1, 1.0, 'def'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(2, 2.0, 'def'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(3, 3.0, 'def'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(1, 1.0, 'DEF'))  ,  
               dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(2, 2.0, 'DEF'));  
    """     


    // qurery data  

    sql """
        create table query_table_multi_key(
            k0 int not null,
            k1 float not null,
            k2 varchar not null
        )       
         DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """     

    sql """insert into query_table_multi_key values(1, 1.0, 'abc');"""  
    sql """insert into query_table_multi_key values(2, 2.0, 'ABC');"""  
    sql """insert into query_table_multi_key values(3, 3.0, 'ABC');"""      

    qt_sql_not_constant"""
        select k0, k1, k2, dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(k0, k1, k2)) from query_table_multi_key order by k0, k1, k2       
    """     


    sql """
        create table query_table_multi_key_null(
            k0 int not null,
            k1 float null,
            k2 varchar null
        )       
         DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into query_table_multi_key_null values(1, 1.0, 'abc');""" 
    sql """insert into query_table_multi_key_null values(2, 2.0, 'ABC');"""     
    sql """insert into query_table_multi_key_null values(2, null, 'ABC');"""
    sql """insert into query_table_multi_key_null values(3, 3.0, 'ABC');"""   
    sql """ insert into query_table_multi_key_null values(3, 3.0, null);"""          

    qt_sql_not_constant"""
        select k0, k1, k2, dict_get_many("test_dict_load_and_get_hash_map_db.dc_multi_key_table", ["int_not_null" , "int_null" , "str_not_null" , "str_null"], struct(k0, k1, k2)) from query_table_multi_key_null order by k0, k1, k2       
    """     
}