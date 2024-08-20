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

suite("test_max_compute_complex_type", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk")
        String mc_catalog_name = "test_max_compute_complex_type"
        sql """drop catalog if exists ${mc_catalog_name} """
        sql """
        CREATE CATALOG IF NOT EXISTS ${mc_catalog_name} PROPERTIES (
                "type" = "max_compute",
                "mc.default.project" = "mc_datalake",
                "mc.region" = "cn-beijing",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.public_access" = "true"
        );
        """

        logger.info("catalog " + mc_catalog_name + " created")
        sql """switch ${mc_catalog_name};"""
        logger.info("switched to catalog " + mc_catalog_name)
        sql """ use mc_datalake """

        qt_mc_q1 """ select id,arr3,arr1,arr5,arr2 from array_table order by id desc """
        qt_mc_q2 """ select arr2,arr1 from map_table order by id limit 2 """
        qt_mc_q3 """ select contact_info,user_info from struct_table order by id limit 2 """
        qt_mc_q4 """ select user_id,activity_log from nested_complex_table order by user_id limit 2 """

        sql """drop catalog ${mc_catalog_name};"""
    }
}
