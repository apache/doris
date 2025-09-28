package infer_expr_name.test
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

suite("set_operation") {
    // test union all
    def table_prefix = "set_operation";

    def checkColNames = { String tableName, List<String> expectedNames ->
        def colNames = sql "desc ${tableName}";
        List<String> firstColNames = colNames.collect { it[0] }
        assertEquals(firstColNames.size(), expectedNames.size())
        for (int i = 0; i < firstColNames.size(); i++) {
            assertEquals(firstColNames[i], expectedNames[i])
        }
    }

    sql """
    CREATE TABLE IF NOT EXISTS set_operation_table_test1
    PROPERTIES (  "replication_num" = "1")
    AS
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    UNION ALL
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales
    UNION ALL
    SELECT 'CATALOG' as channel,
           cs_sold_date_sk as date_sk,
           cs_item_sk as item_sk,
           cs_sales_price as sales_price,
           ''
    FROM catalog_sales;
    """

    checkColNames("set_operation_table_test1",
            ["__cast_0", "__cast_1", "item_sk", "sales_price", "__literal_4"])


    sql """
    CREATE TABLE IF NOT EXISTS set_operation_table_test2
    PROPERTIES (  "replication_num" = "1")
    AS
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    UNION ALL
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales;
    """

    checkColNames("set_operation_table_test2",
            ["__literal_0", "__cast_1", "item_sk", "sales_price", "__literal_4"])


    sql """
    CREATE TABLE IF NOT EXISTS set_operation_table_test3
    PROPERTIES (  "replication_num" = "1")
    AS
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    INTERSECT
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales;
    """

    checkColNames("set_operation_table_test2",
            ["__literal_0", "__cast_1", "item_sk", "sales_price", "__literal_4"])


    sql """
    CREATE TABLE IF NOT EXISTS set_operation_table_test4
    PROPERTIES (  "replication_num" = "1")
    AS
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    EXCEPT
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales;
    """

    checkColNames("set_operation_table_test2",
            ["__literal_0", "__cast_1", "item_sk", "sales_price", "__literal_4"])


    sql """
    CREATE TABLE IF NOT EXISTS set_operation_table_test5
    PROPERTIES (  "replication_num" = "1")
    AS
    SELECT 'STORE', 
           date_add(ss_sold_date_sk, INTERVAL 2 DAY),
           ss_item_sk as item_sk,
           ss_sales_price as sales_price,
           'STORE()##'
    FROM store_sales
    MINUS
    SELECT 'WEB',
           ws_sold_date_sk as date_sk,
           ws_item_sk as item_sk,
           ws_sales_price as sales_price,
           ''
    FROM web_sales;
    """

    checkColNames("set_operation_table_test2",
            ["__literal_0", "__cast_1", "item_sk", "sales_price", "__literal_4"])

}

