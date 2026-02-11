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

suite("test_table_name_with_dollar") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "DROP TABLE IF EXISTS `t\$partitions`"
    sql """
        CREATE TABLE `t\$partitions` (
            `k1` int,
            `v1` varchar(20)
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql "INSERT INTO `t\$partitions` VALUES (1, 'a'), (2, 'b')"

    qt_select "SELECT k1, v1 FROM `t\$partitions` ORDER BY k1"

    qt_desc "DESC `t\$partitions`"

    sql "DROP TABLE IF EXISTS `t\$partitions`"
}
