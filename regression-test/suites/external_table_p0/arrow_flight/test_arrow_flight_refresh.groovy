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

suite("test_arrow_flight_refresh", "p0,external,doris,external_docker,external_docker_doris") {
    String arrow_flight_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String arrow_flight_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String arrow_flight_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String arrow_flight_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String arrow_flight_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    String arrow_flight_thrift_port = context.config.otherConfigs.get("extFeThriftPort")

    def showres = sql "show frontends";
    arrow_flight_arrow_port = showres[0][6]
    arrow_flight_http_port = showres[0][3]
    arrow_flight_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${arrow_flight_arrow_port}, http: ${arrow_flight_http_port}, thrift: ${arrow_flight_thrift_port}")

    def showres2 = sql "show backends";
    log.info("show backends log = ${showres2}")

    def db_name = "test_arrow_flight_refresh_db"
    def catalog_name = "test_arrow_flight_refresh_catalog"

    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """

    sql """DROP DATABASE IF EXISTS ${db_name}"""

    sql """
     CREATE CATALOG `${catalog_name}` PROPERTIES (
                'type' = 'arrow',
                'hosts' = '${arrow_flight_host}:${arrow_flight_arrow_port}',
                'user' = '${arrow_flight_user}',
                'password' = '${arrow_flight_psw}',
                'flight_sql_catalog_name' = 'internal'
        );
    """

    def databases_init = sql """
        SHOW DATABASES FROM `${catalog_name}`
    """

    def checkName = { ArrayList result, String name ->
        for (item in result) {
            println item
            if (item.toString() == "[" + name + "]") {
                println "success"
                return
            }
        }
        println "fail"
    }

    checkName(databases_init, "${db_name}")

    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""

    def databases_before = sql """
        SHOW DATABASES FROM `${catalog_name}`
    """

    checkName(databases_before, "${db_name}")

    sql """REFRESH CATALOG ${catalog_name}"""

    def databases_after = sql """
        SHOW DATABASES FROM `${catalog_name}`
    """

    checkName(databases_after, "${db_name}")

    qt_sql """
        SHOW TABLES FROM `${catalog_name}`.`${db_name}`
    """

    sql """
        CREATE TABLE `${db_name}`.`test_arrow_flight_catalog_t` (
              `id` datetime NOT NULL,
              `c_date` date NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    def tables_before = sql """
        SHOW TABLES FROM `${catalog_name}`.`${db_name}`
    """

    checkName(tables_before, "test_arrow_flight_catalog_t")

    sql """REFRESH CATALOG ${catalog_name}"""

    def tables_after = sql """
        SHOW TABLES FROM `${catalog_name}`.`${db_name}`
    """

    checkName(tables_after, "test_arrow_flight_catalog_t")

    qt_sql """ DESC `${catalog_name}`.`${db_name}`.`test_arrow_flight_catalog_t`"""

    sql """ ALTER TABLE `${db_name}`.`test_arrow_flight_catalog_t` ADD COLUMN ( c_new int NULL) """

    qt_sql """ DESC `${catalog_name}`.`${db_name}`.`test_arrow_flight_catalog_t`"""

    sql """ REFRESH CATALOG ${catalog_name} """

    qt_sql """ DESC `${catalog_name}`.`${db_name}`.`test_arrow_flight_catalog_t` """

    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """
    sql """DROP DATABASE IF EXISTS ${db_name}"""
}


