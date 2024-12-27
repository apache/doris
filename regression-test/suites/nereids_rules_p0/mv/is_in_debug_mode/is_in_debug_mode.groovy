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

import org.junit.Assert

suite("is_in_debug_mode") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders;
    """

    sql """
     CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    UNIQUE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi'); 
    """

    create_async_mv(db, "basic_mv", """
    select * from orders where o_orderkey > 1;
    """)

    sql """set skip_delete_sign = true;"""
    mv_not_part_in("""select * from orders where o_orderkey > 1;""", "basic_mv")
    try {
        sql """
        CREATE MATERIALIZED VIEW test_create_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS select * from orders where o_orderkey > 2;
        """
    } catch (Exception e) {
        def message = e.getMessage()
        logger.info("test_create_mv1" + message)
        Assert.assertTrue(message.contains("because is in debug mode"))
    }
    sql """set skip_delete_sign = false;"""


    sql """set skip_storage_engine_merge = true;"""
    mv_not_part_in("""select * from orders where o_orderkey > 1;""", "basic_mv")
    try {
        sql """
        CREATE MATERIALIZED VIEW test_create_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS select * from orders where o_orderkey > 2;
        """
    } catch (Exception e) {
        def message = e.getMessage()
        logger.info("test_create_mv2" + message)
        Assert.assertTrue(message.contains("because is in debug mode"))
    }
    sql """set skip_storage_engine_merge = false;"""


    sql """set skip_delete_bitmap = true;"""
    mv_not_part_in("""select * from orders where o_orderkey > 1;""", "basic_mv")
    try {
        sql """
        CREATE MATERIALIZED VIEW test_create_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS select * from orders where o_orderkey > 2;
        """
    } catch (Exception e) {
        def message = e.getMessage()
        logger.info("test_create_mv3: " + message)
        Assert.assertTrue(message.contains("because is in debug mode"))
    }
    sql """set skip_delete_bitmap = false;"""


    sql """set skip_delete_predicate = true;"""
    mv_not_part_in("""select * from orders where o_orderkey > 1;""", "basic_mv")
    try {
        sql """
        CREATE MATERIALIZED VIEW test_create_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS select * from orders where o_orderkey > 2;
        """
    } catch (Exception e) {
        def message = e.getMessage()
        logger.info("test_create_mv4" + message)
        Assert.assertTrue(message.contains("because is in debug mode"))
    }
    sql """set skip_delete_predicate = false;"""


    sql """set show_hidden_columns = true;"""
    mv_not_part_in("""select * from orders where o_orderkey > 1;""", "basic_mv")
    try {
        sql """
        CREATE MATERIALIZED VIEW test_create_mv
        BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1') 
        AS select * from orders where o_orderkey > 2;
        """
    } catch (Exception e) {
        def message = e.getMessage()
        logger.info("test_create_mv5" + message)
        Assert.assertTrue(message.contains("because is in debug mode"))
    }
    sql """set show_hidden_columns = false;"""

    sql """drop materialized view if exists basic_mv"""
}

