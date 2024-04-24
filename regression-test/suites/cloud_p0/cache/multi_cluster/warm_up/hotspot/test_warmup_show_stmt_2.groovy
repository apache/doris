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

    sql """ TRUNCATE TABLE __internal_schema.cloud_cache_hotspot; """
    sleep(30000)

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}")
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
    
    sql "use @regression_cluster_name0"
    load_customer_once()

    for (int i = 0; i < 1000; i++) {
        sql "select count(*) from customer where C_CUSTKEY > 10001000"
        if (i % 2 == 0) {
            sql "select count(*) from customer where C_CUSTKEY < 4000000"
        }
    }

    sleep(40000)
    result = sql """ show cache hotspot "/" """
    assertEquals(result.size(), 1);
    assertEquals(result[0][0], "regression_cluster_id0")
    assertEquals(result[0][3], "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer")

    result = sql """ show cache hotspot "/regression_cluster_name0" """
    assertEquals(result[0][3], "p3");
    result = sql """ show cache hotspot "/regression_cluster_name1" """
    assertEquals(result.size(), 0);
    result = sql """ show cache hotspot "/regression_cluster_name0/regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.supplier" """
    assertEquals(result.size(), 0);

    sql new File("""${context.file.parent}/../ddl/${table}_delete.sql""").text
    sleep(40000)
    result = sql """ show cache hotspot "/" """
    assertEquals(result.size(), 1);
    assertEquals(result[0][0], "regression_cluster_id0")
    assertEquals(result[0][3], "regression_test_cloud_p0_cache_multi_cluster_warm_up_hotspot.customer")
}
