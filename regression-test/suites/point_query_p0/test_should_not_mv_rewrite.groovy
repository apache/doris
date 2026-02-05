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

suite("test_should_not_mv_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists tbl_point_query_when_mv;
    """

    sql """
    CREATE TABLE `tbl_point_query_when_mv` (
    `k1` int(11) NULL,
    `v1` decimal(27, 9) NULL,
    `v2` varchar(30) NULL,
    `v3` varchar(30) NULL,
    `v4` date NULL,
    `v5` datetime NULL,
    `v6` float NULL,
    `v7` datev2 NULL
) ENGINE=OLAP
UNIQUE KEY(`k1`)
COMMENT 'OLAP'
DISTRIBUTED BY HASH(`k1`) BUCKETS 1
PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "enable_unique_key_merge_on_write" = "true",
    "light_schema_change" = "true",
    "store_row_column" = "true"
);
    """

    sql """
INSERT INTO tbl_point_query_when_mv (
    `k1`, `v1`, `v2`, `v3`, `v4`, 
    `v5`, `v6`, `v7`
) VALUES
(1, 123456789.123456789, '测试数据1', '示例文本1', '2025-01-01', 
 '2025-01-01 08:30:45', 123.456, '2025-01-01'),

(2, 987654321.987654321, '测试数据2', '示例文本2', '2025-02-15', 
 '2025-02-15 14:25:30', 789.012, '2025-02-15'),

(3, 555555555.555555555, '测试数据3', '示例文本3', '2025-03-20', 
 '2025-03-20 09:15:20', 345.678, '2025-03-20'),

(4, 111111111.222222222, '测试数据4', '示例文本4', '2025-04-10', 
 '2025-04-10 18:45:10', 901.234, '2025-04-10'),

(5, 999999999.888888888, '测试数据5', '示例文本5', '2025-05-25', 
 '2025-05-25 11:30:15', 567.890, '2025-05-25'),

(6, 333333333.444444444, '测试数据6', '示例文本6', '2025-06-30', 
 '2025-06-30 16:20:40', 234.567, '2025-06-30'),

(7, 777777777.666666666, '测试数据7', '示例文本7', '2025-07-15', 
 '2025-07-15 10:10:10', 890.123, '2025-07-15'),

(8, 222222222.333333333, '测试数据8', '示例文本8', '2025-08-20', 
 '2025-08-20 13:45:25', 456.789, '2025-08-20'),

(9, 888888888.999999999, '测试数据9', '示例文本9', '2025-09-05', 
 '2025-09-05 20:30:50', 678.901, '2025-09-05'),

(10, 444444444.555555555, '测试数据10', '示例文本10', '2025-10-31', 
 '2025-10-31 23:59:59', 123.456, '2025-10-31');
    """

    // crate materialized view
    create_async_mv(db, "mv_tbl_point_query", "SELECT k1, v1, v2 FROM tbl_point_query_when_mv;")

    // mv should not be rewritten and should not part in rewrite when point query SHORT-CIRCUIT
    mv_not_part_in("SELECT k1, v1, v2 FROM tbl_point_query_when_mv WHERE k1 = 1", "mv_tbl_point_query")

    sql """set enable_short_circuit_query = false;"""
    // mv should  be rewritten and part in rewrite when disable point query SHORT-CIRCUIT
    mv_rewrite_success_without_check_chosen("SELECT k1, v1, v2 FROM tbl_point_query_when_mv WHERE k1 = 1", "mv_tbl_point_query")
}
