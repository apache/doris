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

suite("partition_prune") {
    // String db = context.config.getDbNameByFile(context.file)
    // sql "use ${db}"
    // sql "set runtime_filter_mode=OFF";
    // sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    // sql """
    // drop table if exists test_duplicate;
    // """

    // sql """
    //     CREATE TABLE IF NOT EXISTS test_duplicate (
    //     `app_name` VARCHAR(64) NULL COMMENT '标识', 
    //     `event_id` VARCHAR(128) NULL COMMENT '标识', 
    //     `decision` VARCHAR(32) NULL COMMENT '枚举值', 
    //     `time` DATETIME NULL COMMENT '查询时间', 
    //     `id` VARCHAR(35) NOT NULL COMMENT 'od', 
    //     `code` VARCHAR(64) NULL COMMENT '标识', 
    //     `event_type` VARCHAR(32) NULL COMMENT '事件类型' 
    //     )
    //     DUPLICATE KEY(app_name, event_id)
    //     PARTITION BY RANGE(time)                                                                                                                                                                                                                
    //     (                                                                                                                                                                                                                                      
    //      FROM ("2024-07-01 00:00:00") TO ("2024-07-15 00:00:00") INTERVAL 1 HOUR                                                                                                                                                                              
    //     )     
    //     DISTRIBUTED BY HASH(event_id)
    //     BUCKETS 3 PROPERTIES ("replication_num" = "1");
    // """

    // sql """
    // insert into test_duplicate values
    // ('aa', 'bc', 'cc', '2024-07-03 01:15:30', 'dd', 'ee', 'ff'),
    // ('as', 'bd', 'cd', '2024-07-03 01:15:30', 'dd', 'ee', 'ff'),
    // ('ad', 'be', 'cc', '2024-07-03 07:06:30', 'dd', 'ee', 'ff'),
    // ('af', 'bf', 'ce', '2024-07-04 10:01:30', 'dd', 'ee', 'ff'),
    // ('ag', 'bc', 'cc', '2024-07-04 10:01:35', 'dd', 'ee', 'ff'),
    // ('aa', 'bc', 'cc', '2024-07-05 01:15:30', 'dd', 'ee', 'ff'),
    // ('as', 'bd', 'cd', '2024-07-05 06:09:30', 'dd', 'ee', 'ff'),
    // ('ad', 'be', 'cc', '2024-07-06 07:06:30', 'dd', 'ee', 'ff'),
    // ('af', 'bf', 'ce', '2024-07-07 10:01:30', 'dd', 'ee', 'ff'),
    // ('ag', 'bc', 'cc', '2024-07-08 12:55:30', 'dd', 'ee', 'ff');
    // """

    // sql """
    // drop table if exists test_unique;
    // """

    // sql """
    //     CREATE TABLE IF NOT EXISTS test_unique (
    //     `time` DATETIME NULL COMMENT '查询时间', 
    //     `app_name` VARCHAR(64) NULL COMMENT '标识', 
    //     `event_id` VARCHAR(128) NULL COMMENT '标识', 
    //     `decision` VARCHAR(32) NULL COMMENT '枚举值', 
    //     `id` VARCHAR(35) NOT NULL COMMENT 'od', 
    //     `code` VARCHAR(64) NULL COMMENT '标识', 
    //     `event_type` VARCHAR(32) NULL COMMENT '事件类型' 
    //     )
    //     UNIQUE KEY(time)
    //     PARTITION BY RANGE(time)                                                                                                                                                                                                                
    //     (                                                                                                                                                                                                                                      
    //      FROM ("2024-07-01 00:00:00") TO ("2024-07-15 00:00:00") INTERVAL 1 HOUR                                                                                                                                                                              
    //     )     
    //     DISTRIBUTED BY HASH(time)
    //     BUCKETS 3 PROPERTIES ("replication_num" = "1");
    // """

    // sql """
    // insert into test_unique values
    // ('2024-07-03 01:00:00', 'aa', 'bc', 'cc', 'dd', 'ee', 'ff'),
    // ('2024-07-03 06:00:00', 'as', 'bd', 'cd', 'dd', 'ee', 'ff'),
    // ('2024-07-03 07:00:00', 'ad', 'be', 'cc', 'dd', 'ee', 'ff'),
    // ('2024-07-04 10:00:00', 'af', 'bf', 'ce', 'dd', 'ee', 'ff'),
    // ('2024-07-04 12:00:00', 'ag', 'bc', 'cc', 'dd', 'ee', 'ff'),
    // ('2024-07-05 01:00:00', 'aa', 'bc', 'cc', 'dd', 'ee', 'ff'),
    // ('2024-07-05 06:00:00', 'as', 'bd', 'cd', 'dd', 'ee', 'ff'),
    // ('2024-07-06 07:00:00', 'ad', 'be', 'cc', 'dd', 'ee', 'ff'),
    // ('2024-07-07 10:00:00', 'af', 'bf', 'ce', 'dd', 'ee', 'ff'),
    // ('2024-07-08 12:00:00', 'ag', 'bc', 'cc', 'dd', 'ee', 'ff');
    // """

    // sql """
    // drop table if exists test_aggregate;
    // """

    // sql """
    //     CREATE TABLE IF NOT EXISTS test_aggregate (
    //     `app_name` VARCHAR(64) NULL COMMENT '标识', 
    //     `event_id` VARCHAR(128) NULL COMMENT '标识', 
    //     `time` DATETIME NULL COMMENT '查询时间',
    //     `price` DOUBLE SUM DEFAULT '0' COMMENT '价格'
    //     )
    //     AGGREGATE KEY(app_name, event_id, time)
    //     PARTITION BY RANGE(time)                                                                                                                                                                                                                
    //     (                                                                                                                                                                                                                                      
    //      FROM ("2024-07-01 00:00:00") TO ("2024-07-15 00:00:00") INTERVAL 1 HOUR                                                                                                                                                                              
    //     )     
    //     DISTRIBUTED BY HASH(event_id)
    //     BUCKETS 3 PROPERTIES ("replication_num" = "1");
    // """

    // sql """
    // insert into test_aggregate values
    // ('aa', 'bc', '2024-07-03 01:00:00', 2.1),
    // ('as', 'bd', '2024-07-03 06:00:00', 1.1),
    // ('ad', 'be', '2024-07-03 07:00:00', 3.1),
    // ('af', 'bf', '2024-07-04 10:00:00', 4.1),
    // ('ag', 'bc', '2024-07-04 12:00:00', 5.1),
    // ('aa', 'bc', '2024-07-05 01:00:00', 6.1),
    // ('as', 'bd', '2024-07-05 06:00:00', 7.1),
    // ('ad', 'be', '2024-07-06 07:00:00', 8.1),
    // ('af', 'bf', '2024-07-07 10:00:00', 9.1),
    // ('ag', 'bc', '2024-07-08 12:00:00', 10.1);
    // """

    // // test partition prune in duplicate table

    // def mv1 = """
    // select
    // app_name,
    // event_id,
    // time,
    // count(*)
    // from 
    // test_duplicate
    // group by
    // app_name,
    // event_id,
    // time;
    // """

    // def query1 = """
    // select
    // app_name,
    // event_id,
    // time,
    // count(*)
    // from 
    // test_duplicate
    // where time < '2024-07-05 01:00:00'
    // group by
    // app_name,
    // time,
    // event_id;
    // """

    // order_qt_query1_before "${query1}"
    // createMV("""
    // CREATE MATERIALIZED VIEW mv1
    // AS
    // ${mv1}
    // """)
    // // wait partition row count report
    // sleep(10000)
    // sql "analyze table test_duplicate with sync;"


    // explain {
    //     sql("""${query1}""")
    //     check {result ->
    //         result.contains("(mv1)") && result.contains("partitions=3")
    //     }
    // }
    // order_qt_query1_after "${query1}"

    // // test partition prune in unique table
    // def mv2 = """
    // select
    // time,
    // app_name,
    // event_id
    // from 
    // test_unique;
    // """

    // def query2 = """
    // select
    // time,
    // app_name,
    // event_id
    // from 
    // test_unique
    // where time < '2024-07-05 01:00:00';
    // """

    // order_qt_query2_before "${query2}"
    // createMV("""
    // CREATE MATERIALIZED VIEW mv2
    // AS
    // ${mv2}
    // """)
    // // wait partition row count report
    // sleep(10000)
    // sql "analyze table test_unique with sync;"

    // explain {
    //     sql("""${query2}""")
    //     check {result ->
    //         result.contains("(mv2)") && result.contains("partitions=5")
    //     }
    // }
    // order_qt_query2_after "${query2}"

    // // test partition prune in aggregate table
    // def mv3 = """
    // select
    // app_name,
    // event_id,
    // time,
    // sum(price)
    // from 
    // test_aggregate
    // where time < '2024-07-11 01:00:00'
    // group by
    // app_name,
    // event_id,
    // time;
    // """

    // def query3 = """
    // select
    // app_name,
    // event_id,
    // time,
    // sum(price)
    // from 
    // test_aggregate
    // where time < '2024-07-05 01:00:00'
    // group by
    // app_name,
    // time,
    // event_id;
    // """


    // order_qt_query3_before "${query3}"
    // createMV("""
    // CREATE MATERIALIZED VIEW mv3
    // AS
    // ${mv3}
    // """)
    // // wait partition row count report
    // sleep(10000)
    // sql "analyze table test_aggregate with sync;"
    // def memo3=sql "explain memo plan ${query3}"
    // print(memo3)
    // explain {
    //     sql("""${query3}""")
    //     check {result ->
    //         result.contains("(mv3)") && result.contains("partitions=5")
    //     }
    // }
    // order_qt_query3_after "${query3}"

}
