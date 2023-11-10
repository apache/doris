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

suite("test_cloud_accessible_oss", "p2,external,hive,external_remote,external_remote_hive") {
    String enabled = context.config.otherConfigs.get("enableObjStorageTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String ak = context.config.otherConfigs.get("aliYunAk")
        String sk = context.config.otherConfigs.get("aliYunSk")
        String hms_catalog_name = "test_cloud_accessible_oss"
        sql """drop catalog if exists ${hms_catalog_name};"""
        sql """
            CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
            PROPERTIES ( 
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'oss.endpoint' = 'oss-cn-beijing.aliyuncs.com',
                'oss.access_key' = '${ak}',
                'oss.secret_key' = '${sk}'
            );
        """
        
        logger.info("catalog " + hms_catalog_name + " created")
        sql """switch ${hms_catalog_name};"""
        logger.info("switched to catalog " + hms_catalog_name)
        sql """ use cloud_accessible """
        qt_hms_q1 """ select * from types_oss order by hms_int """
        qt_hms_q2 """ select * from types_one_part_oss order by hms_int """
        
        sql """drop catalog ${hms_catalog_name};"""
                
        // dlf case
        String dlf_catalog_name = "test_cloud_accessible_dlf"
        String dlf_uid = context.config.otherConfigs.get("dlfUid")
        sql """drop catalog if exists ${dlf_catalog_name};"""
        sql """
            CREATE CATALOG  IF NOT EXISTS ${dlf_catalog_name}
            PROPERTIES (
                "type"="hms",
                "hive.metastore.type" = "dlf",
                "dlf.endpoint" = "dlf.cn-beijing.aliyuncs.com",
                "dlf.region" = "cn-beijing",
                "dlf.proxy.mode" = "DLF_ONLY",
                "dlf.uid" = "${dlf_uid}",
                "dlf.access_key" = "${ak}",
                "dlf.secret_key" = "${sk}",
                "dlf.access.public" = "true"
            );
        """
        logger.info("catalog " + dlf_catalog_name + " created")
        sql """switch ${dlf_catalog_name};"""
        logger.info("switched to catalog " + dlf_catalog_name)
        sql """ use jz_datalake """
        qt_dlf_q1 """ select * from web_site where web_site_id='AAAAAAAAKBAAAAAA' order by web_site_sk,web_site_id limit 1; """ // test char,date,varchar,double,decimal
                
        sql """drop catalog ${dlf_catalog_name};"""
    }
}
