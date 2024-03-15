/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("agg_distinct_case_when") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "DROP TABLE IF EXISTS agg_test_table_t;"
    sql """
        CREATE TABLE `agg_test_table_t` (
        `k1` varchar(65533) NULL,
        `k2` text NULL,
        `k3` text null,
        `k4` text null
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """insert into agg_test_table_t(`k1`,`k2`,`k3`) values('20231026221524','PA','adigu1bububud');"""
    sql """
        select 
        count(distinct case when t.k2='PA' and loan_date=to_date(substr(t.k1,1,8)) then t.k2 end )
        from (
        select substr(k1,1,8) loan_date,k3,k2,k1 from agg_test_table_t) t
        group by
        substr(t.k1,1,8);"""

    sql "DROP TABLE IF EXISTS agg_test_table_t;"
}
