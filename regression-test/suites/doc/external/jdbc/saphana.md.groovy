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

suite("saphana.md", "p0,external,saphana,external_docker,external_docker_saphana") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "saphana_catalog_md";
        String saphana_port = context.config.otherConfigs.get("saphana_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ngdbc-2.4.51.jar"


// Not testing yet because there is no docker environment for saphana

//         sql """drop catalog if exists ${catalog_name} """
//
//         sql """create catalog if not exists ${catalog_name} properties(
//                 "type"="jdbc",
//                 "user"="USERNAME",
//                 "password"="PASSWORD",
//                 "jdbc_url" = "jdbc:sap://Hostname:Port/?optionalparameters",
//                 "driver_url" = "ngdbc-2.4.51.jar",
//                 "driver_class" = "com.sap.db.jdbc.Driver"
//         );"""
//
//         sql """drop catalog if exists ${catalog_name} """
    }
}
