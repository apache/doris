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

suite("test_url_functions") {
    sql " drop table if exists test_url_functions"
    sql """
        create table test_url_functions (
            id int,
            s1 string not null,
            s2 string null
        )
        DISTRIBUTED BY HASH(id)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    //empty table
    order_qt_empty_nullable1 "select top_level_domain(s2) from test_url_functions"
    order_qt_empty_nullable2 "select first_significant_subdomain(s2) from test_url_functions"
    order_qt_empty_nullable3 "select cut_to_first_significant_subdomain(s2) from test_url_functions"
    order_qt_empty_not_nullable1 "select top_level_domain(s1) from test_url_functions"
    order_qt_empty_not_nullable2 "select first_significant_subdomain(s1) from test_url_functions"
    order_qt_empty_not_nullable3 "select cut_to_first_significant_subdomain(s1) from test_url_functions"

    //null / const
    order_qt_empty_null1 "select top_level_domain(NULL)"
    order_qt_empty_null2 "select first_significant_subdomain(NULL)"
    order_qt_empty_null3 "select cut_to_first_significant_subdomain(NULL)"
    
    //vaild url
    order_qt_empty_const1 "select top_level_domain('www.baidu.com')"
    order_qt_empty_const2 "select first_significant_subdomain('www.baidu.com')"
    order_qt_empty_const3 "select cut_to_first_significant_subdomain('www.baidu.com')"
    order_qt_empty_const4 "select top_level_domain('www.google.com.cn')"
    order_qt_empty_const5 "select first_significant_subdomain('www.google.com.cn')"
    order_qt_empty_const6 "select cut_to_first_significant_subdomain('www.google.com.cn')"
    
    //invaild url
    order_qt_empty_const7 "select top_level_domain('I am invaild url')"
    order_qt_empty_const8 "select first_significant_subdomain('I am invaild url')"
    order_qt_empty_const9 "select cut_to_first_significant_subdomain('I am invaild url')"
    

    sql """ insert into test_url_functions values (1, 'www.baidu.com', 'www.baidu.com'); """
    sql """ insert into test_url_functions values (2, 'www.google.com.cn', 'www.google.com.cn'); """
    sql """ insert into test_url_functions values (3, 'invalid url', 'invalid url'); """
    sql """ insert into test_url_functions values (4, '', ''); """
    sql """ insert into test_url_functions values (5, ' ', ' '); """
    sql """ insert into test_url_functions values (6, ' ', NULL); """
    sql """ insert into test_url_functions values (7, 'xxxxxxxx', 'xxxxxxxx'); """
    sql """ insert into test_url_functions values (8, 'http://www.example.com/a/b/c?a=b', 'http://www.example.com/a/b/c?a=b'); """
    sql """ insert into test_url_functions values (9, 'https://news.clickhouse.com/', 'https://news.clickhouse.com/'); """
    sql """ insert into test_url_functions values (10, 'https://news.clickhouse.com.tr/', 'https://news.clickhouse.com.tr/'); """

    order_qt_nullable1 "select id,s2,top_level_domain(s2) from test_url_functions order by id"
    order_qt_nullable2 "select id,s2,first_significant_subdomain(s2) from test_url_functions order by id"
    order_qt_nullable3 "select id,s2,cut_to_first_significant_subdomain(s2) from test_url_functions order by id"

    order_qt_not_nullable1 "select id,s1,top_level_domain(s1) from test_url_functions order by id"
    order_qt_not_nullable2 "select id,s1,first_significant_subdomain(s1) from test_url_functions order by id"
    order_qt_not_nullable3 "select id,s1,cut_to_first_significant_subdomain(s1) from test_url_functions order by id"

}
