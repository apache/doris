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

suite("test_remote_doris_refresh", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_refresh_catalog`
    """

    sql """DROP DATABASE IF EXISTS test_remote_doris_refresh_db"""

    sql """
    CREATE CATALOG `test_remote_doris_all_types_select_catalog` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}'
        );
    """

    def databases_init = sql """
        SHOW DATABASES FROM `test_remote_doris_refresh_catalog`
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

    checkName(databases_init, "test_remote_doris_refresh_db")

    sql """CREATE DATABASE IF NOT EXISTS test_remote_doris_refresh_db"""

    def databases_before = sql """
        SHOW DATABASES FROM `test_remote_doris_refresh_catalog`
    """

    checkName(databases_before, "test_remote_doris_refresh_db")

    sql """REFRESH CATALOG test_remote_doris_refresh_catalog"""

    def databases_after = sql """
        SHOW DATABASES FROM `test_remote_doris_refresh_catalog`
    """

    checkName(databases_after, "test_remote_doris_refresh_db")

    qt_sql """
        SHOW TABLES FROM `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`
    """

    sql """
        CREATE TABLE `test_remote_doris_refresh_db`.`test_remote_doris_catalog_t` (
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
        SHOW TABLES FROM `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`
    """

    checkName(tables_before, "test_remote_doris_catalog_t")

    sql """REFRESH CATALOG test_remote_doris_refresh_catalog"""

    def tables_after = sql """
        SHOW TABLES FROM `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`
    """

    checkName(tables_after, "test_remote_doris_catalog_t")

    qt_sql """ DESC `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`.`test_remote_doris_catalog_t`"""

    sql """ ALTER TABLE `test_remote_doris_refresh_db`.`test_remote_doris_catalog_t` ADD COLUMN ( c_new int NULL) """

    qt_sql """ DESC `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`.`test_remote_doris_catalog_t`"""

    sql """ REFRESH CATALOG test_remote_doris_refresh_catalog """

    qt_sql """ DESC `test_remote_doris_refresh_catalog`.`test_remote_doris_refresh_db`.`test_remote_doris_catalog_t` """

    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_refresh_catalog`
    """
    sql """DROP DATABASE IF EXISTS test_remote_doris_refresh_db"""
}


