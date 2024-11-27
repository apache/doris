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
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

suite("test_create_mv") {
    def tableName = "test_mv_10010"

    def getJobState = { table ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${table}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
        CREATE TABLE ${tableName} (
          `load_time` datetime NOT NULL COMMENT '事件发生时间',
          `id` varchar(192) NOT NULL COMMENT '',
          `class` varchar(192) NOT NULL COMMENT '',
          `result` int NOT NULL COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`load_time`,`id`, `class`)
        COMMENT ''
        PARTITION BY RANGE(`load_time`)(
            PARTITION p1 VALUES LESS THAN ("2025-01-01 00:00:00")
        )
        DISTRIBUTED BY HASH(`load_time`,`id`, `class`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """ insert into ${tableName} values ('2024-03-20 10:00:00', 'a', 'b', 1) """

    sql """
        create materialized view mv_1 as
        select 
          date_trunc(load_time, 'minute'),
          id,
          class,
          count(id) as total,
          min(result) as min_result,
          sum(result) as max_result
        from 
          ${tableName}
        group by date_trunc(load_time, 'minute'), id, class;
    """

    sql """ SHOW ALTER TABLE MATERIALIZED VIEW """

    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tableName)
        if (res == "FINISHED" || res == "CANCELLED") {
            assertEquals("FINISHED", res)
            sleep(3000)
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }
}
