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

suite("test_mixed_par_locations", "p2,external,hive,external_remote,external_remote_hive") {

    def formats = ["_parquet", "_orc"]
    def q1 = """select * from test_mixed_par_locationsSUFFIX order by id;"""
    def q2 = """select count(id) from test_mixed_par_locationsSUFFIX;"""
    def q3 = """select city, count(*) from test_mixed_par_locations_parquet where sex = 'male' group by city order by city;"""

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
            String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
            String extAk = context.config.otherConfigs.get("extAk");
            String extSk = context.config.otherConfigs.get("extSk");
            String extS3Endpoint = context.config.otherConfigs.get("extS3Endpoint");
            String extS3Region = context.config.otherConfigs.get("extS3Region");
            String catalog_name = "test_mixed_par_locations"

            sql """drop catalog if exists ${catalog_name};"""
            sql """
                create catalog if not exists ${catalog_name} properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                    'cos.access_key' = '${extAk}',
                    'cos.secret_key' = '${extSk}',
                    'cos.endpoint' = '${extS3Endpoint}'
                );
            """
            logger.info("catalog " + catalog_name + " created")
            sql """switch ${catalog_name};"""
            logger.info("switched to catalog " + catalog_name)
            sql """use multi_catalog;"""
            logger.info("use multi_catalog")

            for (String format in formats) {
                logger.info("Process format " + format)
                qt_01 q1.replace("SUFFIX", format)
                qt_02 q2.replace("SUFFIX", format)
                qt_03 q3.replace("SUFFIX", format)
            }
            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
