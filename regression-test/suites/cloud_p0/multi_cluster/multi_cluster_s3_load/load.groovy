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

// Most of the cases are copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

// syntax error:
// q06 q13 q15
// Test 23 suites, failed 3 suites

// Note: To filter out tables from sql files, use the following one-liner comamnd
// sed -nr 's/.*tables: (.*)$/\1/gp' /path/to/*.sql | sed -nr 's/,/\n/gp' | sort | uniq

import groovy.json.JsonOutput
import org.apache.doris.regression.util.MultiClusterStateGuard

suite("load") {
    withRestoredMultiClusterState(true) {
    def baseline = multiClusterBaseline()
    def ipList = baseline.collect { it.nodes[0].ip }
    def httpPortList = baseline.collect { it.nodes[0].http_port.toString() }

    // Establish the shared baseline after a previous suite or run left different group names.
    def existing = get_cluster.call(baseline[0].nodes[0].cloud_unique_id)
            .findAll { it.type == 'COMPUTE' }
    if (!MultiClusterStateGuard.frontendMatches(baseline,
            sql_return_maparray('SHOW CLUSTERS'), sql_return_maparray('SHOW BACKENDS'))) {
        existing.each { group -> drop_cluster.call(group.cluster_name, group.cluster_id) }
        sleep(20000)
        baseline.each { group ->
            def node = group.nodes[0]
            add_cluster.call(node.cloud_unique_id, node.ip, node.heartbeat_port.toString(),
                    group.cluster_name, group.cluster_id)
        }
        sleep(20000)
    }
    checkMultiClusterBaseline(baseline)
    sql "SET PROPERTY 'default_cloud_cluster' = '${baseline[0].cluster_name}'"
    List<List<Object>> result

    sql "use @${baseline[1].cluster_name}"
    result  = sql "show clusters"

    sql """ set enable_profile = true """

    def before_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("before_cluster0_load_rows : ${before_cluster0_load_rows}".toString())
    def before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    def before_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("before_cluster1_load_rows : ${before_cluster1_load_rows}".toString())
    def before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("before_cluster1_flush : ${before_cluster1_flush}".toString())

    // Map[tableName, rowCount]
    def tables = [supplier: 1000000]
    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()
    
    

    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    tables.each { table, rows ->
        // create table if not exists
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
        // check row count
        def rowCount = sql "select count(*) from ${table}"

        def loadLabel = table + "_" + uniqueID
        sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
        // load data from cos
        def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    def after_cluster0_load_rows = get_be_metric(ipList[0], httpPortList[0], "load_rows");
    log.info("after_cluster0_load_rows : ${after_cluster0_load_rows}".toString())
    def after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    log.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    def after_cluster1_load_rows = get_be_metric(ipList[1], httpPortList[1], "load_rows");
    log.info("after_cluster1_load_rows : ${after_cluster1_load_rows}".toString())
    def after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    log.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_load_rows == after_cluster0_load_rows)
    assertTrue(before_cluster0_flush == after_cluster0_flush)

    assertTrue(before_cluster1_load_rows != after_cluster1_load_rows)
    assertTrue(before_cluster1_flush != after_cluster1_flush)

    sql "use @${baseline[0].cluster_name}"
    result  = sql "show clusters"

    def before_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("before_cluster0_query_scan_rows : ${before_cluster0_query_scan_rows}".toString())
    def before_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("before_cluster1_query_scan_rows : ${before_cluster1_query_scan_rows}".toString())

    sql """
        select count(*) from supplier where S_NATIONKEY = 11
        """

    def after_cluster0_query_scan_rows = get_be_metric(ipList[0], httpPortList[0], "query_scan_rows");
    log.info("after_cluster0_query_scan_rows : ${after_cluster0_query_scan_rows}".toString())
    def after_cluster1_query_scan_rows = get_be_metric(ipList[1], httpPortList[1], "query_scan_rows");
    log.info("after_cluster1_query_scan_rows : ${after_cluster1_query_scan_rows}".toString())

    assertTrue(before_cluster0_query_scan_rows != after_cluster0_query_scan_rows)
    assertTrue(before_cluster1_query_scan_rows == after_cluster1_query_scan_rows)
    }
}
