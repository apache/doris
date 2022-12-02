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

suite("test_external_catalog_hive", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        try {
            String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
            String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")

            sql """admin set frontend config ("enable_multi_catalog" = "true")"""

            sql """drop catalog if exists hive;"""

            sql """
                create catalog if not exists hive properties (
                    'type'='hms',
                    'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
                );
            """

            sql """switch hive;"""

            sql """use test;"""

            def res = sql """select count(*) from test.hive_test limit 10;"""
            logger.info("recoding select: " + res.toString())

            sql """switch internal"""

            def res1 = sql """show databases;"""
            logger.info("recoding select: " + res1.toString())

        }finally {
            sql """admin set frontend config ("enable_multi_catalog" = "false")"""
        }
    }
}
