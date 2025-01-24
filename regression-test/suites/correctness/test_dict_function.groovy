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

suite("test_dict_function") {
     sql """ DROP TABLE IF EXISTS test_dict_function """
     sql """
        CREATE TABLE test_dict_function(
        id  bigint  not null
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """
        insert into test_dict_function values(1),(2),(3),(4),(5),(6),(7),(8);
    """

    qt_select2 """
        select id, dict_get_string("dict_key_int64","str" ,id) from test_dict_function order by id;
    """ 


    sql """ DROP TABLE IF EXISTS test_dict_str_function """
     sql """
         CREATE TABLE test_dict_str_function(
        id  bigint  not null,
        name string not null
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    sql """
         insert into test_dict_str_function values(1,"abc"),(2,"1"),(3,"2"),(4,"3"),(5,"some null"),(6,"4"),(7,"def"),(8,"5");
    """

    qt_select2 """
        select id, name,  dict_get_string("dict_key_string","str" ,name) from test_dict_str_function order by id;

    """ 
   

    qt_select1 """
        select  dict_get_string("ip_map","ATT" ,cast(cast("192.168.1.130" as ipv4) as ipv6));
    """
    qt_select2 """
        select  dict_get_string("ip_map","ATT" ,cast(cast("192.168.1.00" as ipv4) as ipv6));
    """ 
    qt_select3 """
        select  dict_get_string("ip_map","att" ,cast(cast("172.16.1.130" as ipv4) as ipv6));
    """


    qt_select1 """
        select  dict_get_string("ip_map","ATT" ,(cast("192.168.1.130" as ipv4)));
    """
    qt_select2 """
        select  dict_get_string("ip_map","ATT" ,(cast("192.168.1.00" as ipv4)));
    """ 
    qt_select3 """
        select  dict_get_string("ip_map","att" ,(cast("172.16.1.130" as ipv4)));
    """

}   
