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

suite("test_trino_oracle", "p0,external,oracle,external_docker,external_docker_oracle") {
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
        
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";

        sql """drop catalog if exists trino_oracle_test """
        sql """create catalog if not exists trino_oracle_test properties(
                "type"="trino-connector",
                "trino.connector.name" = "oracle",
                "trino.connection-user" = "doris_test",
                "trino.connection-url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                "trino.connection-password" = "123456"
        );"""

        sql """use trino_oracle_test.doris_test"""

        qt_desc_all_types_null """desc doris_test.extreme_test;"""
        qt_select_all_types_null """select * from doris_test.extreme_test order by 1;"""

        qt_select_all_types_multi_block """select count(*) from doris_test.extreme_test_multi_block;"""

        sql """drop catalog if exists trino_oracle_test """
    }
}
