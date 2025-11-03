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

suite("test_mv_select") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    sql "DROP TABLE IF EXISTS mv_test_table_t"
    sql """
	    CREATE TABLE `mv_test_table_t` (
        `Uid` bigint(20) NOT NULL,
        `DateCode` int(11) NOT NULL,
        `ProductId` bigint(20) NOT NULL,
        `LiveSales` int(11) REPLACE NULL
        ) ENGINE=OLAP
        AGGREGATE KEY(`Uid`, `DateCode`, `ProductId`)
        DISTRIBUTED BY HASH(`Uid`, `ProductId`) BUCKETS 8
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql "ALTER TABLE mv_test_table_t ADD ROLLUP rollup_mv_test_table_t(ProductId,DateCode,Uid);"

    explain {
        sql ("""select Uid
                        from mv_test_table_t  
                where ProductId = 3570093298674738221  and DateCode >=20230919 and DateCode <=20231018
                        group by Uid;""")
        contains "mv_test_table_t"
    }

    sql """drop table if exists SkuUniqDailyCounter"""
    sql """CREATE TABLE `SkuUniqDailyCounter` (
            `Pd` bigint(20) NOT NULL,
            `Dc` int(11) NOT NULL,
            `Bc` bigint(20) NOT NULL,
            `Fs` int(11) REPLACE NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`Pd`, `Dc`, `Bc`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`Dc`) BUCKETS 8
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );"""

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }
}