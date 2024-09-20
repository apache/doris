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

suite("test_union_instance") {
        multi_sql """
	    drop table if exists t1;
	    drop table if exists t2;
	    drop table if exists t3;
	    drop table if exists t4;
	    CREATE TABLE `t1` (
              `c1` int NULL,
              `c2` int NULL,
              `c3` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`c1`, `c2`, `c3`)
            DISTRIBUTED BY HASH(`c1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
	  
	    insert into t1 values (1,1,1);
	    insert into t1 values (2,2,2);
	    insert into t1 values (3,3,3);

            CREATE TABLE `t2` (
              `year_week` varchar(45) NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`year_week`)
            DISTRIBUTED BY HASH(`year_week`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
            
            CREATE TABLE `t3` (
              `unique_key` varchar(2999) NULL,
              `brand_name` varchar(96) NULL,
              `skc` varchar(150) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`unique_key`)
            DISTRIBUTED BY HASH(`unique_key`) BUCKETS 20
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
            
            CREATE TABLE `t4` (
              `skc_code` varchar(150) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`skc_code`)
            DISTRIBUTED BY HASH(`skc_code`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1");
            
            set parallel_pipeline_task_num=1;
            set disable_nereids_rules='PRUNE_EMPTY_PARTITION';
    	"""
	explain {
                sql """
                SELECT `t`.`year_week` AS `year_week`
                ,'' AS `brand_name`
                , '' AS `skc_code`
                FROM `t1` a
                CROSS JOIN `t2` t
                union all
                SELECT '1' AS `year_week`
                ,`a`.`brand_name` AS `brand_name`
                ,`a`.`skc` AS `skc_code`
                FROM `t3` a
                INNER JOIN[shuffle] `t4` b ON `a`.`skc` = `b`.`skc_code`
                GROUP BY 1, 2, 3;
                """
    	        contains "3:VNESTED LOOP JOIN"
		contains "4:VEXCHANGE"
		contains "8:VEXCHANGE"
		contains "6:VEXCHANGE"
		contains "11:VUNION"
        }
}
