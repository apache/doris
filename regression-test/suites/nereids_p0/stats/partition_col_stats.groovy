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

suite("partition_col_stats") {
    multi_sql """
        set global enable_partition_analyze=true;
        drop table if exists pt;
        CREATE TABLE `pt` (
                `k1` int(11) NULL COMMENT "",
                `k2` int(11) NULL COMMENT "",
                `k3` int(11) NULL COMMENT ""
                ) 
                PARTITION BY RANGE(`k1`)
                (PARTITION p1 VALUES LESS THAN ("3"),
                PARTITION p2 VALUES [("3"), ("7")),
                PARTITION p3 VALUES [("7"), ("10")))
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                PROPERTIES ('replication_num' = '1');

        insert into pt values (1, 2, 2), (1, 3, 3), (1, 4, 1), (1, 4, 1), (4, 4, 4), (5,5,5),(6,6,6);
        analyze table pt with sync;
    """
    //run this sql to make stats be cached
    sql "select * from pt where k1<3;"
    def pt_data = sql "show data from pt;"
    def retry= 0;
    while (pt_data[0][4] != '7' && retry < 20) {
        pt_data = sql "show data from pt;"
        sleep(10000);
        retry ++;
        print("wait partition row count, retry " + retry +" times\n");
    }
    
    explain{
        sql "physical plan select * from pt where k1<3;"
        contains("stats=4")
    }
    sql "set global enable_partition_analyze=false;"
}
