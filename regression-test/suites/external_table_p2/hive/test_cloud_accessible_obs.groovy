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

suite("test_cloud_accessible_obs", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableObjStorageTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String ak = context.config.otherConfigs.get("hwYunAk")
        String sk = context.config.otherConfigs.get("hwYunSk")
        String hms_catalog_name = "test_cloud_accessible_obs"
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'obs.endpoint' = 'obs.cn-north-4.myhuaweicloud.com',
                'obs.access_key' = '${ak}',
                'obs.secret_key' = '${sk}'
            );
        """

        logger.info("catalog " + hms_catalog_name + " created")
        sql """switch ${hms_catalog_name};"""
        logger.info("switched to catalog " + hms_catalog_name)
        sql """ use cloud_accessible """
        qt_hms_q1 """ select * from types_obs order by hms_int """
        qt_hms_q2 """ select * from types_one_part_obs order by hms_int """

        sql """drop catalog ${hms_catalog_name};"""
    }
}
