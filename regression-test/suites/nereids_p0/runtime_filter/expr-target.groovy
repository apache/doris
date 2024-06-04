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

suite("expr-target") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "drop table if exists rfexpA"
    sql """
        CREATE TABLE rfexpA (
        a1 INT NOT NULL,
        a2 VARCHAR(25) NOT NULL,
        a3 VARCHAR(152) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(a1)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(a1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        ); 
        """

    sql "drop table if exists rfexpB"
    sql """
        CREATE TABLE rfexpB (
        b1 INT NOT NULL,
        b2 VARCHAR(25) NOT NULL,
        b3 VARCHAR(152) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(b1)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(b1) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "min_load_replica_num" = "-1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false",
        "group_commit_interval_ms" = "10000",
        "group_commit_data_bytes" = "134217728"
        ); 
        """
    sql """
        insert into rfexpA values (1, 1, 1), (2, 2, 2),(-1,-1, -1);
    """
    sql """
        insert into rfexpB values (1,1,1), (4,4,4);
    """
    
    sql "set enable_nereids_planner=true";
    sql "set forbid_unknown_col_stats=false"
    sql "set disable_join_reorder=true"
    sql "set enable_runtime_filter_prune=false"

    explain {
        sql "select * from rfexpA join rfexpB on abs(rfexpA.a1) = rfexpB.b1;"
        contains("RF000")
    }

    explain {
        sql "select * from rfexpA join rfexpB on abs(rfexpA.a1 + rfexpA.a2) = rfexpB.b1;"
        notContains("RF000")
    }

    explain {
        sql "select * from rfexpA join rfexpB on abs(rfexpA.a1 + rfexpA.a1) = rfexpB.b1;"
        contains("RF000")
    }

    explain {
        sql """
            select * 
            from 
                (select a1 + 1 as a1
                from rfexpA
                ) T
                join rfexpB on T.a1 = rfexpB.b1;
            """
        contains("RF000")
    }

    qt_abs "select * from rfexpA join rfexpB on abs(rfexpA.a1) = rfexpB.b1"
}