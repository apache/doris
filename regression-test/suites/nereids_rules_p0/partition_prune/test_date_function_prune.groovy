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

suite("test_date_function_prune") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set partition_pruning_expand_threshold=10;"
    sql "drop table if exists dp"
    sql """
        CREATE TABLE `dp` (
        `node_name` varchar(100) NULL COMMENT '',
        `date_time` datetime NULL COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`node_name`)
        COMMENT ''
        PARTITION BY RANGE(`date_time`)(
        partition p1 values less than ('2020-01-02 00:00:00'),
        partition p2 values less than ('2020-01-03 00:00:00'),
        partition p3 values less than ('2020-01-04 00:00:00')
        )
        DISTRIBUTED BY HASH(`node_name`) BUCKETS 1
        PROPERTIES (
        "replication_num" = "1"
        );"""
    sql "insert into dp values('a', '2020-01-01 11:00:00'), ('b', '2020-01-02 11:00:00'), ('c', '2020-01-03 11:00:00');"
    
    explain {
        sql "select * from dp where Date(date_time) > '2020-01-02'"
        contains("partitions=1/3 (p3)")
    }

    explain {
        sql "select * from dp where '2020-01-02' < Date(date_time);"
        contains("partitions=1/3 (p3)")
    }

    explain {
        sql "select * from dp where Date(date_time) >= '2020-01-02'"
        contains("partitions=2/3 (p2,p3)")
    }

    explain {
        sql "select * from dp where Date(date_time) < '2020-01-02'"
        contains("partitions=1/3 (p1)")
    }

    explain {
        sql "select * from dp where Date(date_time) <= '2020-01-02'"
        contains("partitions=2/3 (p1,p2)")
    }
    
    explain {
        sql "select * from dp where Date(date_time) between '2020-01-01' and '2020-01-02'"
        contains("partitions=2/3 (p1,p2)")
    }

    explain {
        sql "select * from dp where Date(date_time) in ('2020-01-01', '2020-01-03')"
        contains("partitions=2/3 (p1,p3)")
    }

    explain {
        sql "select * from dp where (date(date_time) = null and node_name = 'no sense1') or (date(date_time) = '2020-01-01' and node_name = 'no sense2')"
        contains("partitions=1/3 (p1)")
    }

    explain {
        sql "select * from dp where date(date_time) = null or date(date_time) = '2020-01-01'"
        contains("partitions=1/3 (p1)")
    }

    explain {
        sql "select * from dp where date_time > str_to_date('2020-01-02','%Y-%m-%d')"
        contains("partitions=2/3 (p2,p3)")
    }

    sql "drop table if exists test_to_date_trunc"
    sql """
        CREATE TABLE test_to_date_trunc(
            event_day DATETIME NOT NULL
        )
        DUPLICATE KEY(event_day)
        AUTO PARTITION BY range (date_trunc(event_day, "day")) (
            PARTITION `p20230807` values [(20230807 ), (20230808 )),
            PARTITION `p20020106` values [(20020106 ), (20020107 ))
        )
        DISTRIBUTED BY HASH(event_day) BUCKETS 4
        PROPERTIES("replication_num" = "1");
    """
    explain {
        sql """ select /*+ SET_VAR(enable_nereids_planner=false) */ * from test_to_date_trunc where date_trunc(event_day, "day")= "2023-08-07 11:00:00" """
        contains("partitions=0/2")
    }
    explain {
        sql """ select * from test_to_date_trunc where date_trunc(event_day, "day")= "2023-08-07 11:00:00" """
        contains("VEMPTYSET")
    }
    sql """ insert into test_to_date_trunc values ("20230807000000"); """
    explain {
        sql """ select /*+ SET_VAR(enable_nereids_planner=false) */ * from test_to_date_trunc where date_trunc(event_day, "day")= "2023-08-07 11:00:00" """
        contains("partitions=1/2 (p20230807)")
    }
    explain {
        sql """ select * from test_to_date_trunc where date_trunc(event_day, "day")= "2023-08-07 11:00:00" """
        contains("partitions=1/2 (p20230807)")
    }
}