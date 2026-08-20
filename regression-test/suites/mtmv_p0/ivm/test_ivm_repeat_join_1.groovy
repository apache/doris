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

suite("test_ivm_repeat_join_1") {
    sql """drop materialized view if exists test_ivm_repeat_join_1_mv;"""
    sql """drop table if exists test_ivm_repeat_join_1_sales;"""
    sql """drop table if exists test_ivm_repeat_join_1_product;"""

    sql """
        CREATE TABLE test_ivm_repeat_join_1_sales (
            sale_id INT,
            product_id INT,
            region VARCHAR(16),
            amount INT
        )
        UNIQUE KEY(sale_id)
        DISTRIBUTED BY HASH(sale_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE test_ivm_repeat_join_1_product (
            product_id INT,
            category VARCHAR(16)
        )
        UNIQUE KEY(product_id)
        DISTRIBUTED BY HASH(product_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        INSERT INTO test_ivm_repeat_join_1_product VALUES
            (1, 'book'),
            (2, 'toy');
    """
    sql """
        INSERT INTO test_ivm_repeat_join_1_sales VALUES
            (1, 1, 'east', 10),
            (2, 1, 'west', 20),
            (3, 2, 'east', 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_repeat_join_1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        );
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_join_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_join_1_mv")
    order_qt_repeat_join_complete """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
    """
    order_qt_repeat_join_complete_source """
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        )
    """
    order_qt_repeat_join_complete_empty_group """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
        WHERE g_category = 1 AND g_region = 1
    """

    sql """INSERT INTO test_ivm_repeat_join_1_sales VALUES (4, 2, 'west', 40);"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_join_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_join_1_mv")
    order_qt_repeat_join_sales_incr """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
    """
    order_qt_repeat_join_sales_incr_source """
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        )
    """
    order_qt_repeat_join_sales_incr_empty_group """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
        WHERE g_category = 1 AND g_region = 1
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_join_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_join_1_mv")
    order_qt_repeat_join_complete2 """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
    """
    order_qt_repeat_join_complete2_source """
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        )
    """
    order_qt_repeat_join_complete2_empty_group """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
        WHERE g_category = 1 AND g_region = 1
    """

    sql """INSERT INTO test_ivm_repeat_join_1_product VALUES (3, 'game');"""
    sql """INSERT INTO test_ivm_repeat_join_1_sales VALUES (5, 3, 'east', 50);"""
    Thread.sleep(1000)
    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_join_1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_join_1_mv")
    order_qt_repeat_join_product_incr """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
    """
    order_qt_repeat_join_product_incr_source """
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        )
    """
    order_qt_repeat_join_product_incr_empty_group """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
        WHERE g_category = 1 AND g_region = 1
    """

    sql """REFRESH MATERIALIZED VIEW test_ivm_repeat_join_1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_repeat_join_1_mv")
    order_qt_repeat_join_complete3 """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
    """
    order_qt_repeat_join_complete3_source """
        SELECT test_ivm_repeat_join_1_product.category AS category,
               test_ivm_repeat_join_1_sales.region AS region,
               GROUPING(test_ivm_repeat_join_1_product.category) AS g_category,
               GROUPING(test_ivm_repeat_join_1_sales.region) AS g_region,
               GROUPING_ID(test_ivm_repeat_join_1_product.category,
                           test_ivm_repeat_join_1_sales.region) AS gid,
               COUNT(*) AS cnt,
               SUM(test_ivm_repeat_join_1_sales.amount) AS total_amount
        FROM test_ivm_repeat_join_1_sales
        INNER JOIN test_ivm_repeat_join_1_product
            ON test_ivm_repeat_join_1_sales.product_id = test_ivm_repeat_join_1_product.product_id
        GROUP BY GROUPING SETS (
            (test_ivm_repeat_join_1_product.category, test_ivm_repeat_join_1_sales.region),
            (test_ivm_repeat_join_1_product.category),
            ()
        )
    """
    order_qt_repeat_join_complete3_empty_group """
        SELECT category, region, g_category, g_region, gid, cnt, total_amount
        FROM test_ivm_repeat_join_1_mv
        WHERE g_category = 1 AND g_region = 1
    """
}
