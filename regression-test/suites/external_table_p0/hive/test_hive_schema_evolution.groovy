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

suite("test_hive_schema_evolution", "p0,external,hive,external_docker,external_docker_hive") {
    def q_text = {
        qt_q01 """
        select * from schema_evo_test_text order by id;
        """
        qt_q02 """
        select id, name, ts from schema_evo_test_text order by id;
        """
        qt_q03 """
        select ts from schema_evo_test_text order by id;
        """
    }

    def q_parquet = {
        qt_q01 """
        select * from schema_evo_test_parquet order by id;
        """
        qt_q02 """
        select id, name, ts from schema_evo_test_parquet order by id;
        """
        qt_q03 """
        select ts from schema_evo_test_parquet order by id;
        """
    }

    def q_orc = {
        qt_q01 """
        select * from schema_evo_test_orc order by id;
        """
        qt_q02 """
        select id, name, ts from schema_evo_test_orc order by id;
        """
        qt_q03 """
        select ts from schema_evo_test_orc order by id;
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("diable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "test_${hivePrefix}_schema_evolution"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """switch ${catalog_name}"""
            sql """use `${catalog_name}`.`default`"""

            q_text()
            q_parquet()
            q_orc()

            sql """drop catalog if exists ${catalog_name}"""
        } finally {
        }
    }
}
