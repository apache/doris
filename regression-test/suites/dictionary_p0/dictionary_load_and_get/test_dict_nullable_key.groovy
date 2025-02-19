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

suite("test_dict_nullable_key") {
    def dbName  = "test_dict_nullable_key_db"    

    sql "drop database if exists ${dbName}"
    sql "create database ${dbName}"
    sql "use ${dbName}"

    sql """
        create table tmp_table_no_null(
            k0 int null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into tmp_table_no_null values(1, 'abc');"""  

    sql """insert into tmp_table_no_null values(2, 'def');"""  


    sql """
        create dictionary dc_tmp_table_no_null using tmp_table_no_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """ 

    sleep(1000);


    sql """
        refresh dictionary dc_tmp_table_no_null
    """
   
    qt_sql_constant"""
        select  dict_get("${dbName}.dc_tmp_table_no_null", "k1", 1)  , dict_get("${dbName}.dc_tmp_table_no_null", "k1", 2)  ;
    """     
  



     sql """
        create table tmp_table_null(
            k0 int null,    
            k1 varchar not null
        )
        DISTRIBUTED BY HASH(`k0`) BUCKETS auto
        properties("replication_num" = "1");
    """ 

    sql """insert into tmp_table_null values(1, 'abc');"""  

    sql """insert into tmp_table_null values(null, 'def');"""  


    sql """
        create dictionary tmp_table_null using tmp_table_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """ 

    sleep(5000);

    test {
        sql """
            refresh dictionary tmp_table_null
        """
        exception "key column k0 has null value"
    }


    sql """
        drop dictionary tmp_table_null
    """     
   

    sql """
        create dictionary tmp_table_null using tmp_table_null
        (
            k0 KEY,
            k1 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600','skip_null_key'='true');   
    """ 

    sleep(5000);

    sql """
        refresh dictionary tmp_table_null
    """    


    qt_sql_constant"""
        select  dict_get("${dbName}.tmp_table_null", "k1", 1)  , dict_get("${dbName}.tmp_table_null", "k1", 2)  ;
    """     
  
    
}