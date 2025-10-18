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

suite("test_materialized_view_with_readd_cluster") {
    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    for(String values : bes) {
        String[] beInfo = values.split(':');
        if (beUniqueIdList.contains(beInfo[3])) {
            continue
        }
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    logger.info("ipList:{}", ipList)
    logger.info("hbPortList:{}", hbPortList)
    logger.info("httpPortList:{}", httpPortList);
    logger.info("beUniqueIdList:{}", beUniqueIdList);

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

    def tbName1 = "test_materialized_view_with_readd_cluster"
    def mvName1 = "mv1"
    def mvName2 = "mv2"
    def getJobMaterializedState = { tableName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tableName}' ORDER BY CreateTime DESC LIMIT 1; """
        if (jobStateResult[0][8].equals("CANCELLED") || jobStateResult[0][8].equals("FINISHED")) {
            logger.info("jobStateResult:{}", jobStateResult)
        }
        return jobStateResult[0][8]
    }
    sql "DROP TABLE IF EXISTS ${tbName1} FORCE"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName1}(
                siteid INT(11) NOT NULL,
                citycode SMALLINT(6) NOT NULL,
                username VARCHAR(32) NOT NULL,
                pv BIGINT(20) SUM NOT NULL DEFAULT '0',
                uv BIGINT(20) SUM NOT NULL DEFAULT '0'
            )
            DISTRIBUTED BY HASH(siteid) BUCKETS 5 ;
        """
    sql "insert into ${tbName1} values(1, 1, 'test1', 100,100);"
    sql "insert into ${tbName1} values(2, 1, 'test1', 100,100);"
    sql "insert into ${tbName1} values(3, 1, 'test1', 100,100);"

    sql """create materialized view ${mvName1} as select siteid from ${tbName1} group by siteid;""";

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

    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobMaterializedState(tbName1)
        if (res.equals("CANCELLED") || res.equals("FINISHED")) {
            break
        } else {
            Thread.sleep(5000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED", res)
            }
        }
    }

    sql "use @regression_cluster_name1"
    result  = sql "show clusters"
    for (row : result) {
        logger.info("row:${row}");
        if(row[0] == "regression_cluster_name1") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql """create materialized view ${mvName2} as select siteid from ${tbName1} group by siteid;""";
    max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobMaterializedState(tbName1)
        if (res.equals("CANCELLED") || res.equals("FINISHED")) {
            break
        } else {
            Thread.sleep(5000)
            if (max_try_secs < 1) {
                logger.info("test timeout res:{}", res)
                assertEquals("FINISHED", res)
            }
        }
    }
    sql "DROP TABLE IF EXISTS ${tbName1}"
}
