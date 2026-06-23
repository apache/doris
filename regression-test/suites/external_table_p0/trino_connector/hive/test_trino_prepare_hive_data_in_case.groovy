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

suite("test_trino_prepare_hive_data_in_case", "p0,external") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    def catalog_name = "test_trino_prepare_hive_data_in_case"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def host_ips = new ArrayList()
        def tableName = catalog_name
        def trinoCatalogName = catalog_name
        String[][] backends = sql """ show backends """
        for (def b in backends) {
            host_ips.add(b[1])
        }
        String [][] frontends = sql """ show frontends """
        for (def f in frontends) {
            host_ips.add(f[1])
        }
        dispatchTrinoConnectors(host_ips.unique())
        try {
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
            String hms_port = context.config.otherConfigs.get("hive2HmsPort")

            hive_docker """show databases;"""
            hive_docker """drop table if exists default.${tableName};"""
            hive_docker """
                            create table default.${tableName} (k1 String, k2 String);
                        """
            hive_docker """insert into default.${tableName} values ('aaa','bbb'),('ccc','ddd'),('eee','fff')"""
            def values = hive_docker """select count(*) from `default`.${tableName};"""

            log.info(values.toString())

            sql """drop catalog if exists ${trinoCatalogName};"""
            sql """
                create catalog if not exists ${trinoCatalogName} properties (
                    "type"="trino-connector",
                    "trino.connector.name"="hive",
                    'trino.hive.metastore.uri' = 'thrift://${externalEnvIp}:${hms_port}'
                );
            """
            def values2 = sql """select count(*) from ${trinoCatalogName}.`default`.${tableName};"""
            log.info(values2.toString())
            assertEquals(values[0][0],values2[0][0])

            qt_hive_docker_01 """select * from default.${tableName} order by k1 desc  ;"""

            qt_sql_02 """ select * from ${trinoCatalogName}.`default`.${tableName} order by k1 desc;"""

        } finally {
            try_sql """drop catalog if exists ${trinoCatalogName};"""
            try_hive_docker """drop table if exists default.${tableName};"""
        }
    }
}
