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

suite("filter_to_select") {
    sql "SET enable_nereids_planner=true;"

    sql "drop table if exists t_filter_to_select;"
    sql """
        CREATE TABLE `t_filter_to_select` (
        `k1` int(11) NULL, 
        `k2` int(11) NULL
        ) ENGINE = OLAP DUPLICATE KEY(`k1`, `k2`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`k1`) BUCKETS 1 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1", 
        "in_memory" = "false", "storage_format" = "V2", 
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        insert into t_filter_to_select values (5,4), (6,2), (7,0);
    """

    qt_select """ 
        select * from (select * from t_filter_to_select order by k1 desc limit 2) a where k1 > 5;
    """
}
