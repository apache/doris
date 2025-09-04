package mv.derive_mv_operative
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

suite("derive_mv_oprative") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set pre_materialized_view_rewrite_strategy = NOT_IN_RBO"

    sql """
    drop table if exists part
    """

    sql """
    CREATE TABLE `part` (
      `p_partkey` INT(11) NOT NULL COMMENT '',
      `p_name` VARCHAR(255) NOT NULL COMMENT '',
      `p_size` INT(11) NOT NULL COMMENT ''
    )
    ENGINE=OLAP
    DUPLICATE KEY(`p_partkey`)
    COMMENT '部件信息表'
    DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    INSERT INTO `part` (p_partkey, p_name, p_size)
    VALUES
    (1, 'a', 12),
    (2, 'b', 15),
    (3, 'c', 19),
    (4, 'd', 11),
    (5, 'e', 18),
    (6, 'f', 5), 
    (7, 'g', 25),
    (8, 'h', 16),
    (9, 'i', 10),
    (10, 'j', 14); 
    """

    sql """alter table part modify column p_name set stats ('row_count'='10');"""

    def mv1_0 = """
        SELECT p_name, p_size FROM part WHERE p_size > 10 AND p_size < 20 ORDER BY p_name; 
    """
    def query1_0 = """
        SELECT p_name, p_size FROM part WHERE p_size > 10 AND p_size < 20 ORDER BY p_name limit 5;
    """
    order_qt_query1_0_before "${query1_0}"
    async_mv_rewrite_success(db, mv1_0, query1_0, "operative_mv")
    order_qt_query1_0_after "${query1_0}"
    sql """ DROP MATERIALIZED VIEW IF EXISTS operative_mv"""
}
