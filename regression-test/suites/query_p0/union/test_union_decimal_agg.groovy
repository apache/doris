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

suite("test_union_decimal_agg") {
    def db = "test_query_db"
    sql "use ${db}"
    
    sql """ DROP TABLE IF EXISTS `test_union_decimal_agg_1` """
    sql """ CREATE TABLE `test_union_decimal_agg_1` (
        `cost` decimalv3(27, 2) NULL,
        `active` bigint(20) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`cost`, `active`)
    DISTRIBUTED BY HASH(`cost`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """ DROP TABLE IF EXISTS `test_union_decimal_agg_2` """
    sql """ CREATE TABLE `test_union_decimal_agg_2` (
      `cost` bigint(20) NULL,
      `activated_count` bigint(20) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`cost`, `activated_count`)
    DISTRIBUTED BY HASH(`cost`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """ DROP TABLE IF EXISTS `test_union_decimal_agg_3` """
    sql """ CREATE TABLE `test_union_decimal_agg_3` (
      `cost` decimalv3(27, 2) NULL,
      `ocpcconversionsdetail4` decimalv3(27, 2) NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`cost`, `ocpcconversionsdetail4`)
    DISTRIBUTED BY HASH(`cost`) BUCKETS 10
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """ insert into test_union_decimal_agg_1 values(0, 3); """
    sql """ insert into test_union_decimal_agg_1 values(0, 4); """
    sql """ insert into test_union_decimal_agg_1 values(249.97, 45); """

    qt_decimal_union_agg1 """ SELECT
        sum(basic_data.cost)  / sum(basic_data.active)
    FROM
            ( 
                SELECT cost, active FROM test_union_decimal_agg_1
             UNION ALL      
                SELECT cost  / 100.0  AS cost,activated_count as active FROM test_union_decimal_agg_2
            UNION ALL
                SELECT cast(cost as decimalv3(38, 2)) AS cost,  ocpcconversionsdetail4  AS active FROM  test_union_decimal_agg_3
            ) AS `basic_data`;
    """

    qt_decimal_union_agg2 """ SELECT
        sum(basic_data.cost)  / sum(basic_data.active)
    FROM
            ( 
                SELECT cost, active FROM test_union_decimal_agg_1
             UNION ALL      
                SELECT cost  / 100  AS cost,activated_count as active FROM test_union_decimal_agg_2
            UNION ALL
                SELECT cast(cost as decimalv3(38, 2)) AS cost,  ocpcconversionsdetail4  AS active FROM  test_union_decimal_agg_3
            ) AS `basic_data`;
    """
}
