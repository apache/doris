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

suite("test_outerjoin_jsonb") {
    sql """
        drop table if exists test_outerjoin_jsonb_t1;
    """
    sql """
        drop table if exists test_outerjoin_jsonb_t2;
    """
    
    sql """
        CREATE TABLE `test_outerjoin_jsonb_t1` (
        `id` bigint(20) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        CREATE TABLE `test_outerjoin_jsonb_t2` (
        `serverid` bigint(20) NULL,
        `json_data` jsonb NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`serverid`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`serverid`) BUCKETS 16
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "in_memory" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false"
        );
    """

    sql """
        SELECT 
            tav.json_data
        FROM test_outerjoin_jsonb_t1 k
        left JOIN (SELECT json_data, serverid FROM test_outerjoin_jsonb_t2) tav
        ON k.id = tav.serverid; 
    """

    sql """
        drop table if exists test_outerjoin_jsonb_t1;
    """
    sql """
        drop table if exists test_outerjoin_jsonb_t2;
    """
}
