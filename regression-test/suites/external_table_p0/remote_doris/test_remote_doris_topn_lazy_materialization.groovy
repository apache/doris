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

// Regression test for issue apache/doris#63526: TopN (ORDER BY ... LIMIT) over a remote
// doris catalog used to fail with "MaterializationSinkOperatorX failed to find rpc_struct"
// (arrow flight mode) or "Miss matched return row loc count" (virtual cluster mode), because
// the rowids of remote tables encode the remote cluster's backend ids while the second phase
// fetch address book only contained local backends.
// This test runs TopN queries over both catalog modes and compares the results with querying
// the local table directly.
suite("test_remote_doris_topn_lazy_materialization", "p0,external,doris,external_docker,external_docker_doris") {
    String remote_doris_host = context.config.otherConfigs.get("extArrowFlightSqlHost")
    String remote_doris_user = context.config.otherConfigs.get("extArrowFlightSqlUser")
    String remote_doris_psw = context.config.otherConfigs.get("extArrowFlightSqlPassword")

    def showres = sql "show frontends";
    def remote_doris_arrow_port = showres[0][6]
    def remote_doris_http_port = showres[0][3]
    def remote_doris_thrift_port = showres[0][5]
    log.info("show frontends log = ${showres}, arrow: ${remote_doris_arrow_port}, "
            + "http: ${remote_doris_http_port}, thrift: ${remote_doris_thrift_port}")

    def db_name = "test_remote_doris_topn_lazy_materialization_db"
    def table_name = "remote_topn_t"
    def arrow_catalog = "test_remote_doris_topn_arrow_catalog"
    def olap_catalog = "test_remote_doris_topn_olap_catalog"

    sql """DROP CATALOG IF EXISTS `${arrow_catalog}`"""
    sql """DROP CATALOG IF EXISTS `${olap_catalog}`"""
    sql """DROP DATABASE IF EXISTS ${db_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""

    sql """
        CREATE TABLE `${db_name}`.`${table_name}` (
          `id` INT NOT NULL,
          `k1` INT NOT NULL,
          `v1` VARCHAR(64) NULL,
          `v2` DOUBLE NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    // k1 is reverse of id, so ORDER BY k1 returns rows in descending id order.
    StringBuilder values = new StringBuilder()
    for (int i = 1; i <= 20; i++) {
        if (i > 1) {
            values.append(",")
        }
        values.append("(${i}, ${21 - i}, 'str_${i}', ${i * 1.5})")
    }
    sql """INSERT INTO `${db_name}`.`${table_name}` VALUES ${values.toString()}"""

    // arrow flight mode: the remote table is scanned via RemoteDorisScanNode (FileScan)
    sql """
        CREATE CATALOG `${arrow_catalog}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'true'
        );
    """

    // virtual cluster mode: the remote table is bound as a RemoteOlapTable (OlapScan)
    sql """
        CREATE CATALOG `${olap_catalog}` PROPERTIES (
                'type' = 'doris',
                'fe_http_hosts' = 'http://${remote_doris_host}:${remote_doris_http_port}',
                'fe_arrow_hosts' = '${remote_doris_host}:${remote_doris_arrow_port}',
                'fe_thrift_hosts' = '${remote_doris_host}:${remote_doris_thrift_port}',
                'user' = '${remote_doris_user}',
                'password' = '${remote_doris_psw}',
                'use_arrow_flight' = 'false'
        );
    """

    String localRef = "`${db_name}`.`${table_name}`"
    // topn lazy materialization is triggered when limit < 1024 (default thresholds)
    def topnTemplates = [
        "SELECT * FROM %s ORDER BY k1 LIMIT 5",
        // the shape reported in issue 63526: predicate + order by + limit
        "SELECT id, k1, v1, v2 FROM %s WHERE id > 3 ORDER BY k1 LIMIT 5",
        // projection variant
        "SELECT v1, v2 FROM %s WHERE id > 5 ORDER BY k1 LIMIT 8",
        "SELECT * FROM %s ORDER BY k1 LIMIT 1"
    ]

    def withDistributeHint = { String query ->
        return query.replaceFirst("(?i)^SELECT ",
                "SELECT /*+ SET_VAR(enable_nereids_distribute_planner=true) */ ")
    }

    def compareTopn = { String catalogName, String tableRef ->
        for (String template : topnTemplates) {
            String localQuery = withDistributeHint(String.format(template, localRef))
            String remoteQuery = withDistributeHint(String.format(template, tableRef))
            def localRes = sql localQuery
            def remoteRes = sql remoteQuery
            log.info("topn query on ${catalogName}: ${remoteQuery}")
            assertEquals("topn result mismatch on ${catalogName}: ${remoteQuery}",
                    localRes, remoteRes)
        }
    }

    compareTopn("arrow_flight_catalog", "`${arrow_catalog}`.`${db_name}`.`${table_name}`")
    compareTopn("virtual_cluster_catalog", "`${olap_catalog}`.`${db_name}`.`${table_name}`")

    sql """ DROP DATABASE IF EXISTS ${db_name} """
    sql """ DROP CATALOG IF EXISTS `${arrow_catalog}` """
    sql """ DROP CATALOG IF EXISTS `${olap_catalog}` """
}
