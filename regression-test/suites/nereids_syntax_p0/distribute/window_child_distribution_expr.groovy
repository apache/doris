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

suite("window_child_distribution_expr") {
        multi_sql """
	    drop table if exists baseall;
	    drop table if exists test;
	    CREATE TABLE IF NOT EXISTS `baseall` (
                `k1` tinyint(4) null comment "",
	        `k2` smallint(6) null comment "",
                `k3` int(11) null comment "",
                `k4` bigint(20) null comment ""
            ) engine=olap
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3 properties("replication_num" = "1");

	    CREATE TABLE IF NOT EXISTS `test` (
                `k1` tinyint(4) null comment "",
                `k2` smallint(6) null comment "",
                `k3` int(11) null comment ""
            ) engine=olap
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3 properties("replication_num" = "1");
	    
	    insert into baseall values (1,1,1,1);
	    insert into baseall values (2,2,2,2);
	    insert into baseall values (3,3,3,3);
	    insert into test values (1,1,1);
	    insert into test values (2,2,2);
	    insert into test values (3,3,3);

            set enable_nereids_distribute_planner=true;
            set enable_pipeline_x_engine=true;
            set disable_join_reorder=true;
            set enable_local_shuffle=true;
            set force_to_local_shuffle=true;
    	"""
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k1, t1.k2 order by t1.k3) rn from baseall t1 join test t2 on t1.k1=t2.k1) tmp;
		"""
    	        contains "distribute expr lists: k1[#13], k2[#14]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k1, t1.k2 order by t1.k3) rn from baseall t1 join test t2 on t1.k1=t2.k1) tmp where rn <=1;
		"""
    	        contains "distribute expr lists: k1[#17], k2[#18]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k2, t1.k3 order by t1.k4) rn from baseall t1 join test t2 on t1.k1=t2.k1) tmp;
		"""
    	        contains "distribute expr lists: k2[#14], k3[#15]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k2, t1.k3 order by t1.k4) rn from baseall t1 join test t2 on t1.k1=t2.k1) tmp where rn <=1;
		"""
    	        contains "distribute expr lists: k2[#18], k3[#19]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k2, t1.k3 order by t1.k4) rn from baseall t1 join test t2 on t1.k2=t2.k2) tmp;
		"""
    	        contains "distribute expr lists: k2[#11], k3[#12]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k2, t1.k3 order by t1.k4) rn from baseall t1 join test t2 on t1.k2=t2.k2) tmp where rn <=1;
		"""
    	        contains "distribute expr lists: k2[#15], k3[#16]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k1 order by t1.k3) rn from baseall t1 join test t2 on t1.k2=t2.k2) tmp;
		"""
    	        contains "distribute expr lists: k1[#12]"
        }
	explain {
                sql """
                select * from (select t2.k2, row_number() over (partition by t1.k1 order by t1.k3) rn from baseall t1 join test t2 on t1.k2=t2.k2) tmp where rn <=1;
		"""
    	        contains "distribute expr lists: k1[#15]"
        }
}
