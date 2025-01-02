package mv.unsafe_equals
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

suite("null_unsafe_equals") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NULL,
      o_custkey        INTEGER NULL,
      o_orderstatus    CHAR(1) NULL,
      o_totalprice     DECIMALV3(15,2) NULL,
      o_orderdate      DATE NULL,
      o_orderpriority  CHAR(15) NULL,  
      o_clerk          CHAR(15) NULL, 
      o_shippriority   INTEGER NULL,
      O_COMMENT        VARCHAR(79) NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders values
    (null, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, null, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, null, 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', null, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, null, 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', null,'d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c',null, 2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d', null, 'mi');  
    """

    sql """alter table orders modify column o_comment set stats ('row_count'='8');"""

    def mv1_0 =
            """
            select count(*), o_orderstatus, o_comment
            from orders
            group by
            o_orderstatus, o_comment;
            """
    // query contains the filter which is 'o_orderstatus = o_orderstatus' should reject null
    def query1_0 =
            """
            select count(*), o_orderstatus, o_comment
            from orders
            where o_orderstatus = o_orderstatus
            group by
            o_orderstatus, o_comment;
            """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "mv1_0")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS mv1_0"""
}
