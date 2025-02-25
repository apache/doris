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

import org.junit.jupiter.api.Assertions;

suite("docs/table-design/row-store.md") {
    try {
        sql "DROP TABLE IF EXISTS `tbl_point_query`"
        multi_sql """
        CREATE TABLE `tbl_point_query` (
            `key` int(11) NULL,
            `v1` decimal(27, 9) NULL,
            `v2` varchar(30) NULL,
            `v3` varchar(30) NULL,
            `v4` date NULL,
            `v5` datetime NULL,
            `v6` float NULL,
            `v7` datev2 NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`key`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`key`) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "row_store_columns" = "key,v1,v3,v5,v7",
            "row_store_page_size" = "4096",
            "replication_num" = "1"
        );
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/table-design/row-store.md failed to exec, please fix it", t)
    }
}
