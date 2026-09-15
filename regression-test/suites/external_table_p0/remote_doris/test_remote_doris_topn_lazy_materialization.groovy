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
// (virtual cluster mode), because the rowids of remote tables encode the remote cluster's
// backend ids while the second phase fetch address book only contained local backends.
// This test runs TopN queries over the virtual cluster catalog (use_arrow_flight=false, the
// only mode whose tables pass MaterializeProbeVisitor) and compares the results with querying
// the local table directly.
//
// Coverage note: when this suite's docker environment points the catalog back at the suite's
// own cluster, the remote backend ids equal the local ones, so the conflict guard fires and
// the queries run through the fallback (normal single-phase) path. That still guards the
// "query must not fail and results must match" property of the guard, while the optimized
// cross-cluster second phase fetch (remote nodes_info, cluster_id, multiget to the remote
// backends) needs a genuinely separate second cluster and is verified manually there.
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

    // arrow flight mode: not supported by this fix (rejected by MaterializeProbeVisitor).
    // The catalog is still created to pin that boundary in the explain assertion below.
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
            assertEquals(localRes, remoteRes,
                    "topn result mismatch on ${catalogName}: ${remoteQuery}")
        }
    }

    compareTopn("virtual_cluster_catalog", "`${olap_catalog}`.`${db_name}`.`${table_name}`")

    // Plan shape oracles. The local assertion proves topn lazy materialization is enabled in
    // this environment at all; without it the remote notContains assertions could pass just
    // because the optimization never runs. On this self-referencing environment the remote
    // backend ids equal the local ones, so the conflict guard fires for the virtual cluster
    // catalog, and the arrow flight catalog is rejected by MaterializeProbeVisitor - both
    // must fall back to normal execution (no MaterializationNode in the plan).
    explain {
        sql(""" verbose SELECT * FROM ${localRef} ORDER BY k1 LIMIT 5 """)
        contains("VMaterializeNode")
    }
    explain {
        sql(""" verbose SELECT /*+ SET_VAR(enable_nereids_distribute_planner=true) */ *
                FROM `${olap_catalog}`.`${db_name}`.`${table_name}` ORDER BY k1 LIMIT 5 """)
        notContains("VMaterializeNode")
    }
    explain {
        sql(""" verbose SELECT * FROM `${arrow_catalog}`.`${db_name}`.`${table_name}` ORDER BY k1 LIMIT 5 """)
        notContains("VMaterializeNode")
    }

    sql """ DROP DATABASE IF EXISTS ${db_name} """
    sql """ DROP CATALOG IF EXISTS `${arrow_catalog}` """
    sql """ DROP CATALOG IF EXISTS `${olap_catalog}` """
}
