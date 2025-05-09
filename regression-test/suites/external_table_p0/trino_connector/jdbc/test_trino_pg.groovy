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

suite("test_trino_pg", "p0,external,pg,external_docker,external_docker_pg") {
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
        
        String pg_port = context.config.otherConfigs.get("pg_14_port");

        sql """drop catalog if exists trino_pg_test """
        sql """create catalog if not exists trino_pg_test properties(
                "type"="trino-connector",
                "trino.connector.name" = "postgresql",
                "trino.connection-user" = "postgres",
                "trino.connection-url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
                "trino.connection-password" = "123456",
                "trino.jdbc-types-mapped-to-varchar" = "uuid,interval,inet,cidr,macaddr,point,line,circle,date,timestamp,timestamptz,json,jsonb"
        );"""

        sql """use trino_pg_test.catalog_pg_test"""

        qt_desc_all_types_null """desc catalog_pg_test.extreme_test;"""

        qt_select_all_types_null """SELECT 
                                    id,
                                    smallint_val,
                                    int_val,
                                    bigint_val,
                                    decimal_val,
                                    real_val,
                                    double_val,
                                    char_val,
                                    LENGTH(varchar_val) AS varchar_val_length,
                                    LENGTH(text_val) AS text_val_length,
                                    date_val,
                                    timestamp_val,
                                    timestamptz_val,
                                    interval_val,
                                    bool_val,
                                    bytea_val,
                                    inet_val,
                                    cidr_val,
                                    macaddr_val,
                                    json_val,
                                    jsonb_val,
                                    point_val,
                                    line_val,
                                    circle_val,
                                    uuid_val
                                FROM 
                                    catalog_pg_test.extreme_test
                                ORDER BY 
                                    1;"""

        qt_select_all_types_multi_block """select count(*) from catalog_pg_test.extreme_test_multi_block;"""

        sql """drop catalog if exists trino_pg_test """
    }
}
