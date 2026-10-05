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

suite("test_refresh_dict") {
    // Clean up and create test database
    sql "drop database if exists test_refresh_dict"
    sql "create database test_refresh_dict"
    sql "use test_refresh_dict"

    // Create different types of source tables
    sql """
        create table product_info(
            product_id INT NOT NULL,
            product_name VARCHAR(32) NOT NULL,
            price DECIMAL(10,2) NOT NULL
        )
        DISTRIBUTED BY HASH(product_id) BUCKETS 1
        properties("replication_num" = "1");
    """

    sql """
        create table ip_info(
            ip VARCHAR(32) NOT NULL,
            location VARCHAR(64) NOT NULL,
            update_time DATETIME NOT NULL
        )
        DISTRIBUTED BY HASH(ip) BUCKETS auto
        properties("replication_num" = "1");
    """

    sql """
        create table user_info(
            user_id BIGINT NOT NULL,
            username VARCHAR(32) NOT NULL,
            email VARCHAR(64) NULL,
            status TINYINT NOT NULL
        )
        DISTRIBUTED BY HASH(user_id) BUCKETS 3
        properties("replication_num" = "1");
    """

    sql """
        create table precision_test(
            id BIGINT NOT NULL,
            dt1 DATETIME NOT NULL,
            dt2 DATETIME(3) NOT NULL,
            dt3 DATETIME(6) NOT NULL,
            dec1 DECIMAL(10,2) NOT NULL,
            dec2 DECIMAL(27,9) NOT NULL,
            info VARCHAR(32) NOT NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        properties("replication_num" = "1");
    """

    sql """
        create table column_order_test(
            col1 INT NOT NULL,
            col2 VARCHAR(32) NOT NULL,
            col3 DECIMAL(10,2) NOT NULL,
            col4 DATETIME NOT NULL,
            col5 BIGINT NOT NULL,
            col6 VARCHAR(64) NOT NULL
        )
        DISTRIBUTED BY HASH(col1) BUCKETS 1
        properties("replication_num" = "1");
    """

    // Insert initial data
    sql """
        insert into product_info values
        (1, 'Apple', 5.99),
        (2, 'Banana', 3.99),
        (3, 'Orange', 4.50);
    """

    sql """
        insert into ip_info values
        ('192.168.1.0/24', 'Beijing', '2024-01-14 10:00:00'),
        ('192.168.2.0/24', 'Shanghai', '2024-01-14 10:00:00');
    """

    sql """
        insert into user_info values
        (1001, 'user1', 'user1@email.com', 1),
        (1002, 'user2', 'user2@email.com', 1);
    """

    sql """
        insert into precision_test values
        (1, '2024-01-14 10:00:00', '2024-01-14 10:00:00.123', '2024-01-14 10:00:00.123456', 123.45, 123456.789123456, 'test1'),
        (2, '2024-01-14 11:00:00', '2024-01-14 11:00:00.456', '2024-01-14 11:00:00.456789', 456.78, 789123.456789123, 'test2');
    """

    sql """
        insert into column_order_test values
        (1, 'value1', 10.5, '2024-01-14 10:00:00', 1001, 'info1'),
        (2, 'value2', 20.5, '2024-01-14 11:00:00', 1002, 'info2');
    """

    // Create dictionaries with different layouts
    sql """
        create dictionary product_dict using product_info
        (
            product_id KEY,
            product_name VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    sql """
        create dictionary ip_dict using ip_info
        (
            ip KEY,
            location VALUE
        )
        LAYOUT(IP_TRIE)
        properties('data_lifetime'='600');
    """

    sql """
        create dictionary user_dict using user_info
        (
            username VALUE,
            user_id KEY
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    // Create dictionary for precision test
    sql """
        create dictionary precision_dict using precision_test
        (
            id KEY,
            dt2 VALUE,
            dt3 VALUE,
            dec1 VALUE,
            dec2 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    // Create dictionary with different column order
    sql """
        create dictionary order_dict using column_order_test
        (
            col6 VALUE,
            col1 KEY,
            col4 VALUE,
            col2 VALUE
        )
        LAYOUT(HASH_MAP)
        properties('data_lifetime'='600');
    """

    // Wait for dictionary to be loaded
    waitAllDictionariesReady()

    // Test initial dict_get queries
    qt_sql1 """select dict_get("test_refresh_dict.product_dict", "product_name", 1);"""
    qt_sql2 """select dict_get("test_refresh_dict.ip_dict", "location", cast("192.168.1.1" as ipv4));"""
    qt_sql3 """select dict_get("test_refresh_dict.user_dict", "username", 1001);"""

    // Test precision types
    qt_sql_p1 """select dict_get("test_refresh_dict.precision_dict", "dt2", 1);"""
    qt_sql_p2 """select dict_get("test_refresh_dict.precision_dict", "dt3", 1);"""
    qt_sql_p3 """select dict_get("test_refresh_dict.precision_dict", "dec1", 1);"""
    qt_sql_p4 """select dict_get("test_refresh_dict.precision_dict", "dec2", 1);"""

    // Test column order dictionary
    qt_sql_o1 """select dict_get("test_refresh_dict.order_dict", "col6", 1);"""
    qt_sql_o2 """select dict_get("test_refresh_dict.order_dict", "col4", 1);"""
    qt_sql_o3 """select dict_get("test_refresh_dict.order_dict", "col2", 1);"""

    // Insert new data
    sql """
        insert into product_info values
        (4, 'Grape', 6.99),
        (5, 'Mango', 7.99);
    """

    sql """
        insert into ip_info values
        ('192.168.3.3/24', 'Guangzhou', '2024-01-14 11:00:00');
    """

    sql """
        insert into user_info values
        (1003, 'user3', 'user3@email.com', 1);
    """

    sql """
        insert into precision_test values
        (3, '2024-01-14 12:00:00', '2024-01-14 12:00:00.789', '2024-01-14 12:00:00.789123', 789.12, 456789.123456789, 'test3');
    """

    sql """
        insert into column_order_test values
        (3, 'value3', 30.5, '2024-01-14 12:00:00', 1003, 'info3');
    """

    // Refresh dictionaries
    sql "refresh dictionary test_refresh_dict.product_dict"
    sql "refresh dictionary test_refresh_dict.ip_dict"
    sql "refresh dictionary test_refresh_dict.user_dict"
    sql "refresh dictionary test_refresh_dict.precision_dict"
    sql "refresh dictionary test_refresh_dict.order_dict"

    waitAllDictionariesReady()

    // Test dict_get after refresh
    qt_sql4 """select dict_get("test_refresh_dict.product_dict", "product_name", 4);"""
    qt_sql5 """select dict_get("test_refresh_dict.ip_dict", "location", cast("192.168.1.100" as ipv4));"""
    qt_sql6 """select dict_get("test_refresh_dict.user_dict", "username", 1003);"""

    // Test precision types after refresh
    qt_sql_p5 """select dict_get("test_refresh_dict.precision_dict", "dt2", 3);"""
    qt_sql_p6 """select dict_get("test_refresh_dict.precision_dict", "dt3", 3);"""
    qt_sql_p7 """select dict_get("test_refresh_dict.precision_dict", "dec1", 3);"""
    qt_sql_p8 """select dict_get("test_refresh_dict.precision_dict", "dec2", 3);"""

    // Test column order dictionary after refresh
    qt_sql_o4 """select dict_get("test_refresh_dict.order_dict", "col6", 3);"""
    qt_sql_o5 """select dict_get("test_refresh_dict.order_dict", "col4", 3);"""
    qt_sql_o6 """select dict_get("test_refresh_dict.order_dict", "col2", 3);"""

    // Test non-existent keys
    qt_sql7 """select dict_get("test_refresh_dict.product_dict", "product_name", 10);"""
    qt_sql8 """select dict_get("test_refresh_dict.ip_dict", "location", cast("192.168.5.100" as ipv4));"""
    qt_sql9 """select dict_get("test_refresh_dict.user_dict", "username", 9999);"""
    qt_sql_p9 """select dict_get("test_refresh_dict.precision_dict", "dt2", 999);"""
    qt_sql_o7 """select dict_get("test_refresh_dict.order_dict", "col6", 999);"""

    // Unnormal test
    test {
        sql """select dict_get("test_refresh_dict.ip_dict", "location", "192.168.2.100");"""
        exception "dict_get() only support IP type for IP_TRIE"
    }
}
