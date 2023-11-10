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
        String mc_db = "jz_datalake"
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
            qt_q1 """ select count(*) from store_sales """
        }
        // data type test
        def q02 = {
            qt_q2 """ select * from web_site where web_site_id=2 order by web_site_id """ // test char,date,varchar,double,decimal
            qt_q3 """ select * from int_types order by mc_boolean limit 2 """ // test bool,tinyint,int,bigint
        }
        // test partition table filter
        def q03 = {
            qt_q4 """ select * from mc_parts where dt = '2020-09-21' """
            qt_q5 """ select * from mc_parts where dt = '2021-08-21' """
            qt_q6 """ select * from mc_parts where dt = '2020-09-21' and mc_bigint > 6223 """
            qt_q7 """ select * from mc_parts where dt = '2020-09-21' or mc_bigint > 0 """
        }
        sql """ switch `${mc_catalog_name}`; """
        sql """ use `${mc_db}`; """
        q01()
        q02()
        q03()
    }
}
