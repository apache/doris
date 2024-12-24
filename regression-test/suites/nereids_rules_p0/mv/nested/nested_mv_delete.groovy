package mv.nested
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

suite("nested_mv_delete") {

    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders_1
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders_1  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      o_comment        VARCHAR(79) NOT NULL,
      public_col       INT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders_1 values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy', 1),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy', null),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy', 2),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy', null),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy', 3),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm', null),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi', 4),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi', null);  
    """

    sql """alter table orders_1 modify column o_comment set stats ('row_count'='8');"""


    create_async_mv(db, "mv_level_1", """
    select * from orders_1;
    """)

    create_async_mv(db, "mv_level_2", """
    select * from mv_level_1;
    """)

    sql """drop materialized view mv_level_1;"""

    order_qt_query_after_delete "select * from mv_level_2"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv_level_2"""
}
