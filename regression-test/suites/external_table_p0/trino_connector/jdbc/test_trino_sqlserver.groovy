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

suite("test_trino_sqlserver", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String enabled_trino_connector = context.config.otherConfigs.get("enableTrinoConnectorTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    if (enabled != null && enabled.equalsIgnoreCase("true")
            && enabled_trino_connector!= null && enabled_trino_connector.equalsIgnoreCase("true")) {
        
        def host_ips = new ArrayList()
        String[][] backends = sql """ show backends """
        for (def b in backends) {
            host_ips.add(b[1])
        }
        String [][] frontends = sql """ show frontends """
        for (def f in frontends) {
            host_ips.add(f[1])
        }
        dispatchTrinoConnectors(host_ips.unique())

        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");

        sql """drop catalog if exists trino_sqlserver_test """
        sql """create catalog if not exists trino_sqlserver_test properties(
                "type"="trino-connector",
                "trino.connector.name" = "sqlserver",
                "trino.connection-user" = "sa",
                "trino.connection-url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                "trino.connection-password" = "Doris123456"
        );"""

        sql """use trino_sqlserver_test.dbo"""

        qt_desc_all_types_null """desc dbo.extreme_test;"""

        qt_select_all_types_null """select * from dbo.extreme_test order by 1;"""

        qt_select_all_types_multi_block """select count(*) from dbo.extreme_test_multi_block;"""

        sql """drop catalog if exists trino_sqlserver_test """
    }
}
