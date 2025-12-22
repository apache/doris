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

suite("test_arrow_flight_catalog", "p0,external,doris,external_docker,external_docker_doris") {
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

    def catalog_name = "test_arrow_flight_catalog_catalog"

    // delete catalog
    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """

    // create catalog
    sql """
     CREATE CATALOG `${catalog_name}` PROPERTIES (
                'type' = 'arrow',
                'hosts' = '${arrow_flight_host}:${arrow_flight_arrow_port}',
                'user' = '${arrow_flight_user}',
                'password' = '${arrow_flight_psw}',
                'flight_sql_catalog_name' = 'internal',
                'session.enable_parallel_result_sink' = 'true'
        );
    """

    // show catalog
    sql """
        SHOW CREATE CATALOG `${catalog_name}`
    """

    // alter catalog
    sql """
        ALTER CATALOG `${catalog_name}` SET PROPERTIES ('session.enable_parallel_result_sink' = 'false');
    """

    sql """
        SHOW CREATE CATALOG `${catalog_name}`
    """

    sql """
        DROP CATALOG IF EXISTS `${catalog_name}`
    """
}


