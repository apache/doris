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

suite("test_show_catalogs_error_msg", "p0,external,iceberg,external_docker,external_docker_iceberg") { 
    
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    try {
        String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port");
        String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalog_name = "test_show_catalogs_error_msg"

        sql """drop catalog if exists ${catalog_name}"""
        // use wrong port 181812
        sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
               'type'='iceberg',
               'iceberg.catalog.type'='rest',
               'uri' = 'http://${externalEnvIp}:181812',
               "s3.access_key" = "admin",
               "s3.secret_key" = "password",
               "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
               "s3.region" = "us-east-1"
         );"""

        test {
            sql """show databases from ${catalog_name}"""
            exception "errCode = 2, detailMessage"
        }

        boolean found = false;
        List<List<Object>> res = sql """show catalogs"""
        for (List<Object> line : res) {
            logger.info("get show catalogs line: " + line + ", name: " + line[1] + ", msg: " + line[7]);
            if (line[1].equals("test_show_catalogs_error_msg")) {
                if (line[7].contains("181812 is out of range")) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found, "failed to find invalid catalog") 

        // change to right port, the error msg will be removed
        sql """alter catalog test_show_catalogs_error_msg set properties('uri' = 'http://${externalEnvIp}:${rest_port}');"""
        sql """show databases from test_show_catalogs_error_msg"""
        res = sql """show catalogs"""
        for (List<Object> line : res) {
            logger.info("get show catalogs line: " + line + ", name: " + line[1] + ", msg: " + line[7]);
            if (line[1].equals("test_show_catalogs_error_msg")) {
                if (line[7].isEmpty()) {
                    found = true;
                    break;
                }
            }
        }
        assertTrue(found, "failed to find valid catalog") 

        // sql """drop catalog if exists ${catalog_name}"""

    } finally {

    }
}
