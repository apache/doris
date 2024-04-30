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

suite("test_es_mtmv", "p0,external,es,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    logger.info("enabled: " + enabled)
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    logger.info("externalEnvIp: " + externalEnvIp)
    String es_7_port = context.config.otherConfigs.get("es_7_port")
    logger.info("es_7_port: " + es_7_port)
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "es_mtmv_catalog";
        String mvName = "test_es_mtmv"
        String dbName = "regression_test_mtmv_p0"
        String esDb = "default_db"
        String esTable = "test1"
        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:${es_7_port}",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );"""

        order_qt_catalog """select * from ${catalog_name}.${esDb}.${esTable}"""
        sql """drop materialized view if exists ${mvName};"""

        sql """
            CREATE MATERIALIZED VIEW ${mvName}
                BUILD DEFERRED REFRESH AUTO ON MANUAL
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS
                SELECT * FROM ${catalog_name}.${esDb}.${esTable};
            """

        sql """
                REFRESH MATERIALIZED VIEW ${mvName} complete
            """
        def jobName = getJobName(dbName, mvName);
        waitingMTMVTaskFinished(jobName)
        order_qt_mtmv "SELECT * FROM ${mvName}"

        sql """drop materialized view if exists ${mvName};"""
        sql """ drop catalog if exists ${catalog_name} """
    }
}

