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

suite("test_remote_doris_variant_select", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_arrow_port = context.config.otherConfigs.get("extArrowFlightSqlPort")
    String remote_doris_http_port = context.config.otherConfigs.get("extArrowFlightHttpPort")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")
    String remote_doris_thrift_port = context.config.otherConfigs.get("extFeThriftPort")

    def showres = sql "show frontends";
    remote_doris_arrow_port = showres[0][6]
    remote_doris_http_port = showres[0][3]
    remote_doris_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${remote_doris_arrow_port}, http: ${remote_doris_http_port}, thrift: ${remote_doris_thrift_port}")

    def showres2 = sql "show backends";
    log.info("show backends log = ${showres2}")

    def db_name = "test_remote_doris_variant_select_db"
    def catalog_name = "test_remote_doris_variant_select_catalog"
    def catalog_arrow_name = "test_remote_doris_variant_select_catalog_with_arrow"

    sql "set enable_agg_state=true"

    sql """DROP DATABASE IF EXISTS `${db_name}`"""

    sql """CREATE DATABASE IF NOT EXISTS `${db_name}`"""

    sql """
        CREATE TABLE `${db_name}`.`test_remote_doris_variant_select_t` (
          `id` INT NOT NULL,
          `v` VARIANT NULL,
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        INSERT INTO `${db_name}`.`test_remote_doris_variant_select_t` values
           (1, 'null'),
           (2, NULL),
           (3, 'true'),
           (4, '-17'),
           (5, '123.12'),
           (6, '1.912'),
           (7, '"A quote"'),
           (8, '[-1, 12, false]'),
           (9, '{ "x": "abc", "y": false, "z": 10 }'),
           (10, '"2021-01-01"'),
           (11, '{"a":"a", "b":"0.1",  "c":{"c1":"c1", "c2":"1"}}'),
           (12, '{"a":"b", "b":"0.1X", "c":{"c1":"2",  "c2":"2"}}'),
           (13, '{"a":"ab\\"cde", "b":NULL, "d":{"d1":NULL, "d2":NULL}}'),
           (14, '{"a":"ab{{c\\"de\\"}}"'),
           (15, '{"a":"abc{{{de"');
    """

    // create catalog
    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """
    sql """
        DROP CATALOG IF EXISTS `${catalog_arrow_name}`
    """

    sql """
        CREATE CATALOG `${catalog_name}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'false'
        );
    """
    sql """
        CREATE CATALOG `${catalog_arrow_name}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` order by id
    """
    test {
        sql """
            select * from `${catalog_arrow_name}`.`${db_name}`.`test_remote_doris_variant_select_t` order by id
        """
        // check exception message contains
        exception "[NOT_IMPLEMENTED_ERROR]read_column_from_arrow with type variant. cur path: /dummyPath"
    }

    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` where v['b'] >= 0 order by id
    """

    qt_sql """
        select * from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` where v['c']['c2'] > 1 order by id
    """

    qt_sql """
        select v['a'] from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` order by id
    """

    qt_sql """
        select v['b'] from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` order by id
    """

    qt_sql """
        select sum(cast(v['b'] as double)) from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t`
    """

    qt_sql """
        select v['c']['c1'] from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t` order by id
    """

    qt_sql """
        select sum(cast(v['c']['c1'] as double)) from `${catalog_name}`.`${db_name}`.`test_remote_doris_variant_select_t`
    """

    sql """ DROP DATABASE IF EXISTS `${db_name}` """
    sql """ DROP CATALOG IF EXISTS `${catalog_name}` """
}
