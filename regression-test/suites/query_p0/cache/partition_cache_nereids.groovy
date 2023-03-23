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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("partition_cache_nereids") {
    def tableName = "test_partition_cache_nereids"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` date NOT NULL COMMENT "",
              `k2` int(11) NOT NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`, `k2`)
            COMMENT "OLAP"
            PARTITION BY RANGE(`k1`)
            (PARTITION p202305 VALUES [('2023-05-01'), ('2023-06-01')),
            PARTITION p202306 VALUES [('2023-06-01'), ('2023-07-01')))
            DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 32
            PROPERTIES (
    	      "replication_allocation" = "tag.location.default: 1",
       	      "in_memory" = "false",
              "storage_format" = "V2"
            )
        """

    sql "sync"

    sql """ INSERT INTO ${tableName} VALUES 
                    ("2023-05-27",0),
                    ("2023-05-28",0),
                    ("2023-05-29",0),
                    ("2023-05-30",0),
                    ("2023-05-31",0)
        """
    sql """ INSERT INTO ${tableName} VALUES 
                    ("2023-06-01",0),
                    ("2023-06-02",0)
        """

    sql "set enable_nereids_trace = false;" // issues#16747
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"

    qt_partition_cache """ select * from  ${tableName} order by k1; """

    sql "set enable_partition_cache=true;"

    // 1. test cache and explain
    def sql1 = """
                 select
                   k1,
                   sum(k2) as total_pv 
                 from
                   ${tableName} 
                 where
                   k1 between '2023-05-01' and '2023-05-31' 
                 group by
                   k1
                 order by
                   k1;
              """

    qt_partition_cache """ ${sql1} """
    explain {
	sql """ plan ${sql1} """
	contains """Cache"""
        contains """>= 2023-05-01"""
        contains """<= 2023-05-26"""
    }
    qt_partition_cache """ ${sql1} """


    // 2. test view
    def viewName = "${tableName}_view"

    sql """ DROP VIEW IF EXISTS ${viewName} """
    sql """ 
	    CREATE VIEW ${viewName} as 
            select
                k1 as kk1, k2 as kk2
            from 
                ${tableName} 
	    where 
                k1 between '2023-05-11' and '2023-05-27'
	""" 

    def sql2 = """
                 select
                   kk1,
                   sum(kk2) as total_pv
                 from
                   ${viewName} 
                 where
                   kk1 between '2023-05-01' and '2023-05-31'
                 group by
                   kk1
                 order by
                   kk1;
              """
    qt_partition_cache """ ${sql2} """
    explain {
	sql """ plan ${sql2} """
	contains """Cache"""
        contains """>= 2023-05-11"""
        contains """<= 2023-05-26"""
    }
    qt_partition_cache """ ${sql2} """
    
   sql "set enable_partition_cache=false;"
}
