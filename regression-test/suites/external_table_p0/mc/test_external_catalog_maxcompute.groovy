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

suite("test_external_catalog_maxcompute", "p2,external,maxcompute,external_remote,external_remote_maxcompute") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk");
        String mc_db = "mc_datalake"
        String mc_catalog_name = "test_external_mc_catalog"

        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.region" = "cn-beijing",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.public_access" = "true"
            );
        """
        
        // query data test
        def q01 = {
            order_qt_q1 """ select count(*) from web_site """
        }
        // data type test
        def q02 = {
            order_qt_q2 """ select * from web_site where web_site_id>='WS0003' order by web_site_id; """ // test char,date,varchar,double,decimal
            order_qt_q3 """ select * from int_types order by mc_boolean limit 10 """ // test bool,tinyint,int,bigint
        }
        // test partition table filter
        def q03 = {
            order_qt_q4 """ select * from mc_parts where dt = '2023-08-03' """
            order_qt_q5 """ select * from mc_parts where dt > '2023-08-03' """
            order_qt_q6 """ select * from mc_parts where dt > '2023-08-03' and mc_bigint > 1002 """
            order_qt_q7 """ select * from mc_parts where dt < '2023-08-03' or (mc_bigint > 1003 and dt > '2023-08-04') order by mc_bigint, dt; """
        }

        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        q01()
        q02()
        q03()

        // replay test
        sql """drop catalog if exists ${mc_catalog_name};"""
        sql """
            create catalog if not exists ${mc_catalog_name} properties (
                "type" = "max_compute",
                "mc.region" = "cn-beijing",
                "mc.default.project" = "${mc_db}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.public_access" = "true"
            );
        """
        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        order_qt_replay_q6 """ select * from mc_parts where dt >= '2023-08-03' and mc_bigint > 1001 """
        
        // test multi partitions prune
        sql """ refresh catalog ${mc_catalog_name} """
        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        order_qt_multi_partition_q1 """ show partitions from multi_partitions limit 2,3; """
        order_qt_multi_partition_q2 """ select pt, create_time, yy, mm, dd from multi_partitions where pt>-1 and yy > '' and mm > '' and dd >'' order by pt desc, dd desc limit 3; """
        order_qt_multi_partition_q3 """ select sum(pt), create_time, yy, mm, dd from multi_partitions where yy > '' and mm > '' and dd >'' group by create_time, yy, mm, dd order by dd limit 3; """
        order_qt_multi_partition_q4 """ select count(*) from multi_partitions where pt>-1 and yy > '' and mm > '' and dd <= '30'; """
        order_qt_multi_partition_q5 """ select create_time, yy, mm, dd from multi_partitions where yy = '2023' and mm='08' and dd='04' order by pt limit 3; """
        order_qt_multi_partition_q6 """ select max(pt), yy, mm from multi_partitions where yy = '2023' and mm='08' group by yy, mm order by yy, mm; """
        order_qt_multi_partition_q7 """ select count(*) from multi_partitions where yy < '2023' or dd < '03'; """
        order_qt_multi_partition_q8 """ select count(*) from multi_partitions where pt>=3; """
        order_qt_multi_partition_q9 """ select city,mnt,gender,finished_time,order_rate,cut_date,create_time,pt, yy, mm, dd from multi_partitions where pt >= 2 and pt < 4 and finished_time is not null; """
        order_qt_multi_partition_q10 """ select pt, yy, mm, dd from multi_partitions where pt >= 2 and create_time > '2023-08-03 03:11:00' order by pt, yy, mm, dd limit 3; """
    }
}
