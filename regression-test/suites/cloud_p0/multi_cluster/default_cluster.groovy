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

suite("default_cluster") {
    withRestoredMultiClusterState(false) {
    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    logger.info("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        logger.info("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    logger.info("the ip is " + ipList);
    logger.info("the heartbeat port is " + hbPortList);
    logger.info("the http port is " + httpPortList);
    logger.info("the be unique id is " + beUniqueIdList);

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(20000)

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name2", "regression_cluster_id2");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name3", "regression_cluster_id3");
    sleep(20000)

    result  = sql "show clusters"
    assertTrue(result.size() == 2);
    for (row : result) {
        logger.info("show cluster row: ${row}".toString())
    }

    sql "SET PROPERTY 'default_cloud_cluster' = 'regression_cluster_name3'"

    context.reconnectFe()
    def before_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    logger.info("before_cluster0_flush : ${before_cluster0_flush}".toString())

    def before_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    logger.info("before_cluster1_flush : ${before_cluster1_flush}".toString())


    sql """
        CREATE TABLE if not exists test_table (
            class INT,
            id INT,
            score INT SUM
        )
        AGGREGATE KEY(class, id)
        DISTRIBUTED BY HASH(class) BUCKETS 16
    """

    sql """
        insert into test_table values (1, 2, 3);
    """

    def after_cluster0_flush = get_be_metric(ipList[0], httpPortList[0], "memtable_flush_total");
    logger.info("after_cluster0_flush : ${after_cluster0_flush}".toString())

    def after_cluster1_flush = get_be_metric(ipList[1], httpPortList[1], "memtable_flush_total");
    logger.info("after_cluster1_flush : ${after_cluster1_flush}".toString())

    assertTrue(before_cluster0_flush == after_cluster0_flush)
    assertTrue(before_cluster1_flush < after_cluster1_flush)

    result  = sql "show clusters"
    for (row : result) {
        logger.info("row: ${row.toString()}")
        if(row[0] == "regression_cluster_name2") {
            assertTrue(row[1].toString().toLowerCase() == "false")
        }

        if(row[0] == "regression_cluster_name3") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql "use @regression_cluster_name2"
    result  = sql "show clusters"
    for (row : result) {
        logger.info("row: ${row.toString()}")
        if(row[0] == "regression_cluster_name2") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
        if(row[0] == "regression_cluster_name3") {
            assertTrue(row[1].toString().toLowerCase() == "false")
        }
    }

    sql "SET PROPERTY 'default_cloud_cluster' = 'regression_cluster_name2'"
    context.reconnectFe()
    result  = sql "show clusters"
    for (row : result) {
        logger.info("row: ${row.toString()}")
        if(row[0] == "regression_cluster_name2") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
        if(row[0] == "regression_cluster_name3") {
            assertTrue(row[1].toString().toLowerCase() == "false")
        }
    }

    sql "use @regression_cluster_name3"
    result  = sql "show clusters"
    for (row : result) {
        logger.info("row: ${row.toString()}")
        if(row[0] == "regression_cluster_name2") {
            assertTrue(row[1].toString().toLowerCase() == "false")
        }
        if(row[0] == "regression_cluster_name3") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql "SET PROPERTY 'default_cloud_cluster' = 'regression_cluster_name3'"
    test {
        sql "SET PROPERTY 'default_cloud_cluster' = 'xxxxxx'"
        // check exception message contains
        exception "errCode = 2,"
    }

    sql "SET PROPERTY 'default_cloud_cluster' = ''"

    result  = sql "show property"
    for (row : result) {
        logger.info("row: ${row.toString()}")
        if(row[0] == "default_cloud_cluster") {
            assertTrue(row[1].toString().isEmpty())
        }
    }

    sql """
        drop table if exists test_table
    """
    }
}
