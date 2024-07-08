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

import groovy.json.JsonOutput
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import groovy.util.concurrent.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock;
import java.text.SimpleDateFormat;
import java.util.Date;

suite("add_node_load") {
    def prefix = "tpch/sf1"
    def parallel = 8
    def queryNumPerThread = 50

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    sleep(1000)
    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    wait_cluster_change()

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_node.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                  "regression_cluster_name0", "regression_cluster_id0");

    wait_cluster_change()

    result  = sql "show clusters"
    assertEquals(result.size(), 1);

    sql "use @regression_cluster_name0"

    // Map[tableName, rowCount]
    //def tables = [region: 5, nation: 25, supplier: 1000000, customer: 15000000, part: 20000000, partsupp: 80000000, orders: 150000000, lineitem: 600037902]
    //def tables = [lineitem_0: 600037902]
    def tables = [supplier_0: 10000]

    //def tableTabletNum = [region: 1, nation: 1, supplier: 32, customer: 32, part: 32, partsupp: 32, orders: 32, lineitem: 32]
    //def tableTabletNum = [lineitem_0: 32]
    def tableTabletNum = [supplier_0: 320]

    def checkBalance = { beNum ->
        boolean isBalanced = true;
        tables.each { table, rows ->
            avgNum = tableTabletNum.get(table) / beNum
            result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM ${table}; """

            for (row : result) {
                log.info("replica distribution: ${row} ".toString())
                if (row[1] == "0") {
                    continue;
                }
 
                if ( Integer.valueOf((String) row[1]) * 100 / avgNum >= 90 || Integer.valueOf((String) row[1]) * 100 / avgNum <= 110 ) {
                    continue;
                }

                if (Integer.valueOf((String) row[1]) <= avgNum + 1 || Integer.valueOf((String) row[1]) >= avgNum - 1) {
                    continue;
                }

                isBalanced = false;
            }
        }
        isBalanced
    }

    def waitBalanced = { beNum ->
        long startTs = System.currentTimeMillis() / 1000
        do {
            isBalanced = checkBalance.call(beNum)
            log.info("check balance: ${isBalanced} ".toString())
            if (!isBalanced) {
                sleep(5000)
            }
        } while(!isBalanced)

        long endTs = System.currentTimeMillis() / 1000
        endTs - startTs
    }

    
    

    for (int tableCnt = 0; tableCnt < 1; ++tableCnt) {
        sql """ DROP TABLE IF EXISTS supplier_${tableCnt}; """
        // create table if not exists
        sql new File("""${context.file.parent}/load_ddl/supplier_${tableCnt}.sql""").text
    }

    ExecutorService pool;
    List metric = [];

    def loadParallel = { parallelNum, loadNumPerThread, tableNum  ->
        def lock = new ReentrantLock()
        metric = [];

        pool = Executors.newFixedThreadPool(parallelNum)
        for(int i = 0; i < parallelNum; i++){
            int copyIndex = i;
            pool.execute{
                Map metricInThread = [:]
                int id = copyIndex;
                println("id " + id);
                def connRes = connect(user = 'root', password = '', url = context.config.jdbcUrl) {
                    for (int loadIndex = 0; loadIndex < loadNumPerThread; loadIndex++) {
                        sql """ use regression_test_cloud_multi_cluster_perf """
                        sql "use @regression_cluster_name0"

                        for (int tableCnt = 0; tableCnt < tableNum; ++tableCnt) {
                            String uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
                            String externalStageName = "regression_test_tpch" + "_" + uniqueID
                            int rows = 10000

                            sql """
                                create stage if not exists ${externalStageName} properties(
                                'endpoint' = '${getS3Endpoint()}' ,
                                'region' = '${getS3Region()}' ,
                                'bucket' = '${getS3BucketName()}' ,
                                'prefix' = 'regression' ,
                                'ak' = '${getS3AK()}' ,
                                'sk' = '${getS3SK()}' ,
                                'provider' = '${getProvider()}',
                                'access_type' = 'aksk',
                                'default.file.column_separator' = "|"
                                );
                            """

                            def loadSql = new File("""${context.file.parent}/load_ddl/supplier_load_${tableCnt}.sql""").text.replaceAll("\\\$\\{stageName\\}", externalStageName).replaceAll("\\\$\\{prefix\\}", prefix)
                            def copyResult = sql loadSql
                            logger.info("copy result: " + copyResult)
                            assertTrue(copyResult.size() == 1)
                            assertTrue(copyResult[0].size() == 8)
                            assertTrue(copyResult[0][1].equals("FINISHED"))
                            assertTrue(copyResult[0][4].equals(rows+""))

                            long timestamp = System.currentTimeMillis() / (1000 * 10)
                            if (metricInThread.containsKey(timestamp)) {
                                int cnt = metricInThread.get(timestamp);
                                cnt++
                                metricInThread.put(timestamp, cnt);
                                log.info("metricInThread: " + metricInThread);
                            } else {
                                metricInThread.put(timestamp, 1);
                                log.info("metricInThread: " + metricInThread);
                            }

                            sql """ DROP STAGE IF EXISTS ${externalStageName} """
                        }
                    }
                }

                lock.lock();
                log.info("metricInThread: " + metricInThread);
                metric.add(metricInThread)
                lock.unlock();
            }
        }

        println "done cycle"
        pool.shutdown()                 //all tasks submitted
    }

    def waitLoadFinish = { parallelNum ->
        while (!pool.isTerminated()){}  //waitfor termination
        println 'Finished all threads'

        totalMetric = [:]
        for (int i = 0; i < parallelNum; i++) {
            for (e : metric[i]) {
                if (!totalMetric.containsKey(e.key)) {
                    totalMetric.put(e.key, 0);
                }
                cnt = totalMetric.get(e.key);
                totalMetric.put(e.key, cnt + e.value);
            }
        }

        totalMetric
    }

    loadParallel.call(parallel, queryNumPerThread, 1);
    totalMetric = waitLoadFinish.call(parallel);
    for (e : totalMetric) {
        println("timestamp " + e.key + " qps " + e.value);
    }
    def firstTs = totalMetric.entrySet().iterator().next().getKey();

    def connRes = connect(user = 'admin', password = 'selectdb2022@', url = 'jdbc:mysql://192.144.213.234:18929/?useLocalSessionState=true') {
         for (e : totalMetric) {
             println("timestamp " + e.key + " qps " + e.value);
             int qps = e.value
             long timestamp = e.key * 1000 * 10

             SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd H:mm:ss");
             String formattedTime = dateFormat.format(new Date(timestamp));
             dateFormat = new SimpleDateFormat("yyyy-MM-dd");
             String formattedDate = dateFormat.format(new Date(timestamp));

             sql """ use performance_test_result """
             sql """ insert into node_change_perf values (1, 2, 0, 'add node load', ${qps}, "${formattedDate}", "${formattedTime}", 'luwei', 'cloud', '2.3.0') """;
         }
     }

    boolean isBalanced = checkBalance.call(2);
    println("isBalanced: " + isBalanced);

    add_node.call(beUniqueIdList[2], ipList[2], hbPortList[2],
                  "regression_cluster_name0", "regression_cluster_id0");

    loadParallel.call(parallel, queryNumPerThread, 1);

    balanceCostSec = waitBalanced.call(3);
    log.info("balance Cost Sec: " + balanceCostSec);

    dropTotalMetric = waitLoadFinish.call(parallel);
    for (e : dropTotalMetric) {
        println("timestamp " + e.key + " qps " + e.value);
    }

    def dropFirstTs = dropTotalMetric.entrySet().iterator().next().getKey();
    def diffSec = dropFirstTs - firstTs
    println("firstTs " + firstTs + " dropFirstTs " + dropFirstTs + " diffSec " + diffSec);

    def dropConnRes = connect(user = 'admin', password = 'selectdb2022@', url = 'jdbc:mysql://192.144.213.234:18929/?useLocalSessionState=true') {
        for (e : dropTotalMetric) {
            println("timestamp " + e.key + " qps " + e.value);
            int qps = e.value
            long timestamp = (e.key - diffSec) * 1000 * 10

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String formattedTime = dateFormat.format(new Date(timestamp));
            dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            String formattedDate = dateFormat.format(new Date(timestamp));

            sql """ use performance_test_result """
            sql """ insert into node_change_perf values (1, 2, 1, 'add node load', ${qps}, "${formattedDate}", "${formattedTime}", 'luwei', 'cloud', '2.3.0') """;
        }
    }
}
