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

suite("test_trino_mysql", "p0,external,mysql,external_docker,external_docker_mysql") {
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
        
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists trino_mysql_test """
        sql """create catalog if not exists trino_mysql_test properties(
                "type"="trino-connector",
                "trino.connector.name" = "mysql",
                "trino.connection-user" = "root",
                "trino.connection-url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}",
                "trino.connection-password" = "123456",
                "trino.jdbc-types-mapped-to-varchar" = "JSON"
        );"""

        sql """use trino_mysql_test.doris_test"""

        qt_desc_all_types_null """desc all_types_nullable;"""
        qt_select_all_types_null """select * except(time1,time2,time3) from all_types_nullable order by 1;"""

        qt_desc_all_types_non_null """desc all_types_non_nullable;"""
        qt_select_all_types_non_null """select * except(time1,time2,time3) from all_types_non_nullable order by 1;"""

        qt_select_varchar """select * from t_varchar order by 1;"""
        qt_select_char """select * from t_char order by 1;"""

        qt_select_all_types_multi_block """select count(`int`),count(`varchar`) from all_types_multi_block;"""

        sql """drop catalog if exists trino_mysql_test """
    }
}

