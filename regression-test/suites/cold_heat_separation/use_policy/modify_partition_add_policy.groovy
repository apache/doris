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

import java.text.SimpleDateFormat;
import java.util.Date;

suite("add_table_policy_by_modify_partition") {
    sql """ADMIN SET FRONTEND CONFIG ("enable_storage_policy" = "true");"""

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Date date = new Date(System.currentTimeMillis() + 3600000)
    def cooldownTime = format.format(date)

    sql """DROP TABLE IF EXISTS create_table_partition"""
    def create_table_partition_not_have_policy_result = try_sql """
        CREATE TABLE IF NOT EXISTS `create_table_partition` (
        `lo_orderkey` bigint(20) NOT NULL COMMENT "",
        `lo_linenumber` bigint(20) NOT NULL COMMENT "",
        `lo_custkey` int(11) NOT NULL COMMENT "",
        `lo_partkey` int(11) NOT NULL COMMENT "",
        `lo_suppkey` int(11) NOT NULL COMMENT "",
        `lo_orderdate` int(11) NOT NULL COMMENT "",
        `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
        `lo_shippriority` int(11) NOT NULL COMMENT "",
        `lo_quantity` bigint(20) NOT NULL COMMENT "",
        `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
        `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
        `lo_discount` bigint(20) NOT NULL COMMENT "",
        `lo_revenue` bigint(20) NOT NULL COMMENT "",
        `lo_supplycost` bigint(20) NOT NULL COMMENT "",
        `lo_tax` bigint(20) NOT NULL COMMENT "",
        `lo_commitdate` bigint(20) NOT NULL COMMENT "",
        `lo_shipmode` varchar(11) NOT NULL COMMENT ""
        )
        PARTITION BY RANGE(`lo_orderdate`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")) )
        DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 4
        PROPERTIES (
        "replication_num" = "1"
        );
    """
    assertEquals(create_table_partition_not_have_policy_result.size(), 1);

    // support
    // 1. ALTER TABLE create_table_partition MODIFY PARTITION p1992 SET("storage_policy"="not_exist_policy");
    // 2. ALTER TABLE create_table_partition MODIFY PARTITION (p1992, p1998) SET("storage_policy"="not_exist_policy");
    // 3. ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="not_exist_policy");
    def alter_table_partition_use_not_exist_policy_result = try_sql """
        ALTER TABLE create_table_partition MODIFY PARTITION p1992 SET("storage_policy"="not_exist_policy");
    """
    // errCode = 2, detailMessage = Resource does not exist. name: not_exist_policy
    assertEquals(alter_table_partition_use_not_exist_policy_result, null);

    def storage_exist = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for(iter in show_storage_policy){
            if(name == iter[0]){
                return true;
            }
        }
        return false;
    }

    def create_s3_resource = try_sql """
        CREATE RESOURCE IF NOT EXISTS "test_modify_partition_table_use_resource"
        PROPERTIES(
            "type"="s3",
            "s3_region" = "bj",
            "s3_endpoint" = "http://bj.s3.comaaaa",
            "s3_root_path" = "path/to/rootaaaa",
            "s3_secret_key" = "aaaa",
            "s3_access_key" = "bbba",
            "s3_bucket" = "test-bucket"
        );
    """
    def create_succ_1 = try_sql """
        CREATE STORAGE POLICY IF NOT EXISTS created_create_table_partition_alter_policy
        PROPERTIES(
        "storage_resource" = "test_modify_partition_table_use_resource",
        "cooldown_datetime" = "$cooldownTime"
        );
    """
    sql """ALTER STORAGE POLICY created_create_table_partition_alter_policy PROPERTIES("cooldown_datetime" = "$cooldownTime")"""
    assertEquals(storage_exist.call("created_create_table_partition_alter_policy"), true)

    def alter_table_partition_try_again_result = try_sql """
        ALTER TABLE create_table_partition MODIFY PARTITION (*) SET("storage_policy"="created_create_table_partition_alter_policy");
    """
    // OK
    assertEquals(alter_table_partition_try_again_result.size(), 1);

    def alter_table_when_table_partition_has_storage_policy_result = try_sql """
        ALTER TABLE create_table_partition MODIFY PARTITION p1992 SET("storage_policy"="created_create_table_partition_alter_policy");
    """
    // errCode = 2, detailMessage = Do not support alter table's partition storage policy , this table [create_table_partition] and partition [p1992] has storage policy created_create_table_partition_alter_policy
    assertEquals(alter_table_when_table_partition_has_storage_policy_result, null);

    sql """
    DROP TABLE IF EXISTS create_table_partition;
    """
}
