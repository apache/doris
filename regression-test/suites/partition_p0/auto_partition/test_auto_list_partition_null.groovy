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

suite("test_auto_list_partition_null") {

    sql "DROP TABLE IF EXISTS list_table_null"

    sql """
        CREATE TABLE list_table_null (
            id int null,
            k largeint null
        )
        AUTO PARTITION BY LIST (`id`, `k`)
        (
        )
        DISTRIBUTED BY HASH(`k`) BUCKETS 16
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p1` VALUES IN ((NULL, "1")) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p2` VALUES IN (("1", NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p3` VALUES IN ((NULL, NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p4` VALUES IN ((NULL, MAXVALUE)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p5` VALUES IN ((MAXVALUE, NULL)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p6` VALUES IN (("1", MAXVALUE)) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """
    sql """ ALTER TABLE `list_table_null` ADD PARTITION `p7` VALUES IN ((MAXVALUE, "1")) ("version_info" = "1") DISTRIBUTED BY HASH(`k`) BUCKETS 16; """

    def res = sql "show create table list_table_null"

    assertTrue(res[0][1].contains("PARTITION p3 VALUES IN ((NULL, NULL))"))
    assertTrue(res[0][1].contains("PARTITION p1 VALUES IN ((NULL, \"1\"))"))
    assertTrue(res[0][1].contains("PARTITION p4 VALUES IN ((NULL, MAXVALUE))"))
    assertTrue(res[0][1].contains("PARTITION p2 VALUES IN ((\"1\", NULL))"))
    assertTrue(res[0][1].contains("PARTITION p6 VALUES IN ((\"1\", MAXVALUE))"))
    assertTrue(res[0][1].contains("PARTITION p5 VALUES IN ((MAXVALUE, NULL))"))
    assertTrue(res[0][1].contains("PARTITION p7 VALUES IN ((MAXVALUE, \"1\"))"))
}
