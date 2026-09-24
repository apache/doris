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

suite("test_multi_stale_rowset", "nonConcurrent") {
    // This setting is static in FE; isolate it and restore it even when a cache assertion fails.
    String originalSyncLoad = sql("select @@enable_multi_cluster_sync_load")[0][0].toString()
    assertTrue(originalSyncLoad.toLowerCase() in ["true", "false", "1", "0"],
            "Unexpected sync-load setting: ${originalSyncLoad}")
    onFinish {
        sql "set enable_multi_cluster_sync_load = ${originalSyncLoad}"
        logger.info("Restored enable_multi_cluster_sync_load=${originalSyncLoad}")
    }

    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="1200", "disable_auto_compaction"="true") """
    List<String> ipList = new ArrayList<>();
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> brpcPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    int num = 0
    for(String values : bes) {
        if (num++ == 2) break;
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
        brpcPortList.add(beInfo[4]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);
    println("the brpc port is " + brpcPortList);

    def clearFileCache = { ip, port ->
        httpTest {
            endpoint ""
            uri ip + ":" + port + """/api/file_cache?op=clear&sync=true"""
            op "get"
            body ""
        }
    }

    def getMetricsMethod = { ip, port, check_func ->
        httpTest {
            endpoint ip + ":" + port
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    clearFileCache.call(ipList[0], httpPortList[0]);
    clearFileCache.call(ipList[1], httpPortList[1]);

    def tableName = "customer"
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

    sql "use @regression_cluster_name0"

    def table = "customer"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    // create table if not exists
    sql (new File("""${context.file.parent}/ddl/${table}.sql""").text + ttlProperties)
    sleep(10000)

    def load_customer_once =  {
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
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
    def getCurCacheSize = {
        def backendIdToCacheSize = [:]
        for (int i = 0; i < ipList.size(); i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("curl http://")
            sb.append(ipList[i])
            sb.append(":")
            sb.append(brpcPortList[i])
            sb.append("/vars/*file_cache_cache_size")
            String command = sb.toString()
            logger.info(command);
            def process = command.execute()
            def code = process.waitFor()
            def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            def out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            String[] str = out.split(':')
            assertEquals(str.length, 2)
            logger.info(str[1].trim())
            backendIdToCacheSize.put(beUniqueIdList[i], Long.parseLong(str[1].trim()))
        }
        return backendIdToCacheSize
    }
    sql """ set enable_multi_cluster_sync_load=true """
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    load_customer_once()
    sleep(30000);
    def backendIdToAfterLoadCacheSize = getCurCacheSize()
    assertTrue(backendIdToAfterLoadCacheSize.get(beUniqueIdList[0]) > 0,
            "Source backend should cache rowsets generated by load")
    assertTrue(backendIdToAfterLoadCacheSize.get(beUniqueIdList[1]) > 0,
            "Target backend should cache rowsets synchronized by multi-cluster sync load")

    sql "use @regression_cluster_name0"
    String[][] tablets = sql """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")

    sleep(90000);
    def backendIdToAfterCompactionCacheSize = getCurCacheSize()
    logger.info("File cache size after load: ${backendIdToAfterLoadCacheSize}, " +
            "after compaction: ${backendIdToAfterCompactionCacheSize}")
    assertTrue(backendIdToAfterCompactionCacheSize.get(beUniqueIdList[0]) > 0,
            "Source backend should cache the compaction output rowsets")
    assertTrue(backendIdToAfterCompactionCacheSize.get(beUniqueIdList[1]) <
            backendIdToAfterLoadCacheSize.get(beUniqueIdList[1]),
            "Target backend should recycle the stale rowset cache")

    // Multi-cluster sync load only synchronizes rowsets generated by load. It does not
    // synchronize subsequent compaction output rowsets, so the source and target cache
    // sizes are not expected to remain equal after stale rowset compaction.
    sql "use @regression_cluster_name1"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
}
