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
//import org.postgresql.Driver
suite("test_external_catalog_es", "p2") {

    String enabled = context.config.otherConfigs.get("enableExternalEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extEsHost = context.config.otherConfigs.get("extEsHost")
        String extEsPort = context.config.otherConfigs.get("extEsPort")
        String extEsUser = context.config.otherConfigs.get("extEsUser")
        String extEsPassword = context.config.otherConfigs.get("extEsPassword")
        String esCatalogName ="es7_catalog_name"

        String jdbcPg14Database1 = "jdbc_es_14_database1"
        String jdbcPg14Table1 = "accounts"

        sql """drop catalog if exists ${esCatalogName}"""

        sql """
            CREATE CATALOG ${esCatalogName} PROPERTIES (
                    "type"="es",
                    "elasticsearch.hosts"="http://${extEsHost}:${extEsPort}",
                    "elasticsearch.nodes_discovery"="false",
                    "elasticsearch.username"="${extEsUser}",
                    "elasticsearch.username"="${extEsPassword}",
            );
            """

        sql """
            SWITCH ${esCatalogName};
            """
        sql """
            SHOW DATABASES;
            """

        def res1=sql "select * from ${jdbcPg14Table1} limit 10;"
        logger.info("recoding all: " + res1.toString())

        sql """drop table if exists ${jdbcPg14Table1};"""
        sql """drop database if exists ${jdbcPg14Database1};"""

        sql """switch internal;"""

    }
}
