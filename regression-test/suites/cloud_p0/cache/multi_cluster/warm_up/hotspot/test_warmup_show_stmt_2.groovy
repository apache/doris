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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_warmup_show_stmt_2") {
    def table = "customer"
    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier_delete.sql""").text
    // create table if not exists
    sql new File("""${context.file.parent}/../ddl/${table}_with_partition.sql""").text
    sql new File("""${context.file.parent}/../ddl/supplier.sql""").text
    // FIXME(gavin): should also reset the stats of cache hotspot on the BE side
    sql """ TRUNCATE TABLE __internal_schema.cloud_cache_hotspot; """

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()

    def load_customer_once =  { 
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/../ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
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

    connect('root') {
        sql "use @regression_cluster_name0"
        sql "use regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot"
        load_customer_once()
    }

    // generate cache hotspot
    for (int i = 0; i < 1000; i++) {
        sql "select count(*) from customer where C_CUSTKEY > 10001000"
        if (i % 2 == 0) {
            sql "select count(*) from customer where C_CUSTKEY < 4000000"
        }
    }

    sleep(40000)

    def getLineNumber = {
        def s = java.lang.Thread.currentThread().getStackTrace()[3]
        return s.getFileName() + ":" + s.getLineNumber()
    }

    def result = sql_return_maparray """ show cache hotspot "/" """
    log.info(result.toString())
    org.junit.Assert.assertTrue("result.size() " + result.size() + " > 0", result.size() > 0)
    def hotTableName = "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer"
    for (int i = 0; i < result.size(); ++i) {
        if (!result[i].get("hot_table_name").equals(hotTableName)) {
            org.junit.Assert.assertTrue(getLineNumber() + "cannot find expected cache hotspot ${hotTableName}", result.size() > i + 1)
            continue
        }
        assertEquals(result[i].get("cluster_id"), "regression_cluster_id0")
        assertEquals(result[i].get("cluster_name"), "regression_cluster_name0")
        assertEquals(result[i].get("hot_table_name"), "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer")
    }

    result = sql_return_maparray """ show cache hotspot "/regression_cluster_name0" """
    log.info(result.toString())
    org.junit.Assert.assertTrue(getLineNumber() + "result.size() " + result.size() + " > 0", result.size() > 0)
    assertEquals(result[0].get("hot_partition_name"), "p3")
    assertEquals(result[0].get("table_name"), "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer")
    // result = sql_return_maparray """ show cache hotspot "/regression_cluster_name1" """
    // assertEquals(result.size(), 0);
    // not queried table should not be the hotspot
    result = sql_return_maparray """ show cache hotspot "/regression_cluster_name0/regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.supplier" """
    log.info(result.toString())
    assertEquals(result.size(), 0);

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sleep(40000)
    result = sql_return_maparray """ show cache hotspot "/" """
    log.info(result.toString())
    org.junit.Assert.assertTrue("result.size() " + result.size() + " > 0", result.size() > 0)
    for (int i = 0; i < result.size(); ++i) {
        if (!result[i].get("hot_table_name").equals(hotTableName)) {
            org.junit.Assert.assertTrue("cannot find expected cache hotspot ${hotTableName}", result.size() > i + 1)
            continue
        }
        assertEquals(result[i].get("cluster_id"), "regression_cluster_id0")
        assertEquals(result[i].get("cluster_name"), "regression_cluster_name0")
        assertEquals(result[i].get("hot_table_name"), "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer")
        break
    }
}
