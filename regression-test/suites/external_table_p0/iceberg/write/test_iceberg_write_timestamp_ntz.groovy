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

suite("test_iceberg_write_timestamp_ntz", "p0,external,iceberg,external_docker,external_docker_iceberg") { 
    
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

  
    try {

        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "iceberg_timestamp_ntz_test"

        sql """drop catalog if exists ${catalog_name}"""
        sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
               'type'='iceberg',
               'iceberg.catalog.type'='rest',
               'uri' = 'http://${externalEnvIp}:${rest_port}',
               "s3.access_key" = "admin",
               "s3.secret_key" = "password",
               "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
               "s3.region" = "us-east-1"
         );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)
        sql """ use test_db;""" 

        sql """INSERT INTO t_ntz_doris VALUES ('2025-02-07 20:12:00');"""
        sql """INSERT INTO t_tz_doris VALUES ('2025-02-07 20:12:01');"""

     
        sql "set time_zone = 'Asia/Shanghai'"
        qt_timestamp_ntz """select * from t_ntz_doris;"""
        qt_timestamp_tz  """select * from t_tz_doris;"""
        // test Extra column in desc result
        qt_desc01 """desc t_ntz_doris"""
        qt_desc02 """desc t_tz_doris"""

        sql "set time_zone = 'Europe/Tirane'"
        qt_timestamp_ntz2 """select * from t_ntz_doris;"""
        qt_timestamp_tz2  """select * from t_tz_doris;"""
      
        // sql """drop catalog if exists ${catalog_name}"""

    } finally {

    }
}
