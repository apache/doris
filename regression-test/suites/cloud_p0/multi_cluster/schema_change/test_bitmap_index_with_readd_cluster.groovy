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

suite("test_bitmap_index_with_readd_cluster") {
    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

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

    // add cluster regression_cluster_name0
    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    wait_cluster_change()
    result  = sql "show clusters"
    assertTrue(result.size() == 1);

    sql "use @regression_cluster_name0"
    result  = sql "show clusters"
    for (row : result) {
        logger.info("row:${row}");
        if(row[0] == "regression_cluster_name0") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    // create table and add bitmap index
    def tbName1 = "test_bitmap_index_with_readd_cluster"
    def getJobState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
        return jobStateResult[0][9]
    }
    sql "DROP TABLE IF EXISTS ${tbName1} FORCE"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1} (
                k1 TINYINT,
                k2 SMALLINT,
                k3 INT,
                k4 BIGINT,
                k5 CHAR,
                k6 VARCHAR,
                k7 DATE,
                k8 DATETIME,
                k9 LARGEINT,
               k10 DECIMAL,
               k11 BOOLEAN,
               k12 DATEV2,
               k13 DATETIMEV2,
               k14 DATETIMEV2(3),
               k15 DATETIMEV2(6)
            )
            DISTRIBUTED BY HASH(k1) BUCKETS 5 ;
        """
    
    sql "insert into ${tbName1} values(1,1,1,1,'1','1','2022-05-31','2022-05-31 10:00:00',1,1.0,1,'2022-05-31','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111','2022-05-31 10:00:00.111111');"
    
    sql """
            ALTER TABLE ${tbName1}
                ADD INDEX index1 (k1) USING BITMAP,
                ADD INDEX index2 (k2) USING BITMAP,
                ADD INDEX index3 (k3) USING BITMAP,
                ADD INDEX index4 (k4) USING BITMAP,
                ADD INDEX index5 (k5) USING BITMAP,
                ADD INDEX index6 (k6) USING BITMAP,
                ADD INDEX index7 (k7) USING BITMAP,
                ADD INDEX index8 (k8) USING BITMAP,
                ADD INDEX index9 (k9) USING BITMAP,
                ADD INDEX index10 (k10) USING BITMAP,
                ADD INDEX index11 (k11) USING BITMAP,
                ADD INDEX index12 (k12) USING BITMAP,
                ADD INDEX index13 (k13) USING BITMAP,
                ADD INDEX index14 (k14) USING BITMAP,
                ADD INDEX index15 (k15) USING BITMAP;
        """
    
    // drop cluster
    drop_cluster.call("regression_cluster_name0", "regression_cluster_id0");
    // add another cluster regression_cluster_name1
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    wait_cluster_change()
    result  = sql "show clusters"
    assertTrue(result.size() == 1);
    for (row : result) {
        logger.info("row:${row}");
        if(row[0] == "regression_cluster_name1") {
            assertTrue(row[1].toString().toLowerCase() == "false")
        }
    }

    // get schema change job state, should cancel
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tbName1)
        if (res == "CANCELLED") {
            logger.info(tbName1 + " alter job CANCELLED")
            break
        } else {
            Thread.sleep(1000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED", res)
            }
        }
    }
    sql "DROP TABLE IF EXISTS ${tbName1} FORCE"
}
