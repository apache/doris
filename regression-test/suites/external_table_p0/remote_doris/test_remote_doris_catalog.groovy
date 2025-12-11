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

suite("test_remote_doris_catalog", "p0,external,doris,external_docker,external_docker_doris") {
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

    // delete catalog
    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_catalog_catalog`
    """

    // create catalog
    sql """
       CREATE CATALOG `test_remote_doris_catalog_catalog` PROPERTIES (
                'type' = 'doris',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    // show catalog
    sql """
        SHOW CREATE CATALOG `test_remote_doris_catalog_catalog`
    """

    // alter catalog
    sql """
        ALTER CATALOG `test_remote_doris_catalog_catalog` SET PROPERTIES ('enable_parallel_result_sink' = 'false');
    """

    sql """
        SHOW CREATE CATALOG `test_remote_doris_catalog_catalog`
    """

    sql """
        DROP CATALOG IF EXISTS `test_remote_doris_catalog_catalog`
    """
}


