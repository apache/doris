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

suite("test_collect_list_distinct") {
    def table1 = "bu_delivery"
    def table2 = "bu_delivery_product"
    def table3 = "bu_trans_transfer"

    sql "DROP TABLE IF EXISTS ${table1}"
    sql "DROP TABLE IF EXISTS ${table2}"
    sql "DROP TABLE IF EXISTS ${table3}"

    sql """
            CREATE TABLE `${table1}` (
              `delivery_id` bigint(20) NOT NULL,
              `val_status` varchar(6) NULL,
              `delivery_type` int(11) NULL,
              `catalog_name` varchar(96) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`delivery_id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`delivery_id`) BUCKETS 9
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
        """
    sql """
            CREATE TABLE `${table2}` (
              `delivery_product_id` bigint(20) NOT NULL,
              `delivery_id` bigint(20) NULL,
              `nc_num` text NULL,
              `material_qty` text NULL,
              `catalog_name` text NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`delivery_product_id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`delivery_product_id`) BUCKETS 9
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
        """
    sql """
        CREATE TABLE `${table3}` (
          `transfer_id` bigint(20) NOT NULL,
          `delivery_product_id` bigint(20) NULL,
          `nc_num` text NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`transfer_id`)
        COMMENT '涓浆杩愯緭鍗曚俊鎭�'
        DISTRIBUTED BY HASH(`transfer_id`) BUCKETS 9
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """insert into ${table3} values (2, 1, "12");"""
    sql """insert into ${table2} values (1, 7, "12", "car", "amory is cute"); """
    sql """insert into ${table1} values (7, 30, 1, "amory clever"); """
    sql """insert into ${table1} values (1, 90, NULL, "car");"""
    sql """insert into ${table3} values (1, 2, "12");"""

    qt_select_tt  """ select * from ${table3} order by transfer_id;"""
    qt_select_bd  """ select * from ${table1} order by delivery_id;"""
    qt_select_bdp  """ select * from ${table2} order by delivery_product_id;"""

    // this select stmt can trigger column array call update_hashes_with_value
    qt_select """ select * from (
                    select  bdp.nc_num,
                            collect_list(distinct(bd.catalog_name)) as catalog_name,
                            material_qty
                            from ${table2} bdp
                            left join ${table3} btt on bdp.delivery_product_id = btt.delivery_product_id
                            left join ${table1} bd on bdp.delivery_id = bd.delivery_id
                     where  bd.val_status in ('10', '20', '30', '90')  and bd.delivery_type in (0, 1, 2)
                     group by nc_num, material_qty
                     union all
                     select bdp.nc_num,
                            collect_list(distinct(bd.catalog_name)) as catalog_name,
                            material_qty
                            from ${table3} btt
                            left join ${table2} bdp on bdp.delivery_product_id = btt.delivery_product_id
                            left join ${table1} bd on bdp.delivery_id = bd.delivery_id
                     where  bd.val_status in ('10', '20', '30', '90') and bd.delivery_type in (0, 1, 2)
                     group by nc_num, material_qty ) aa;
              """

    sql "DROP TABLE IF EXISTS ${table1};"
    sql "DROP TABLE IF EXISTS ${table2};"
    sql "DROP DATABASE IF EXISTS ${table3};"
}
