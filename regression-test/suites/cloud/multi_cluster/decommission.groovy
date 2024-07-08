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

suite("decommission") {
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
    sleep(20000)

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    sleep(20000)

    result  = sql "show clusters"
    assertTrue(result.size() == 1);

    for (row : result) {
        println row
    }

    add_node.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                  "regression_cluster_name0", "regression_cluster_id0");

    sleep(20000)

    result  = sql "show clusters"
    assertTrue(result.size() == 1);

    for (row : result) {
        println row
    }
    sql """ drop table IF EXISTS table100 """
    sql """ drop table IF EXISTS table_p2 """
    sql """
        CREATE TABLE table100 (
        class INT,
        id INT,
        score INT SUM
        )
        AGGREGATE KEY(class, id)
        DISTRIBUTED BY HASH(class) BUCKETS 48

    """

    sql """
        CREATE TABLE table_p2 ( k1 int(11) NOT NULL, k2 varchar(20) NOT NULL, k3 int sum NOT NULL )
        AGGREGATE KEY(k1, k2)
        PARTITION BY RANGE(k1) (
        PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(k1) BUCKETS 3
    """

    sleep(20000)
    setFeConfig("balance_tablet_percent_per_run", 0.5)

    sql """ use @regression_cluster_name0 """

    result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
    assertEquals(result.size(), 2);

    for (row : result) {
        log.info("replica distribution: ${row} ".toString())
        assertTrue(Integer.valueOf((String) row[1]) <= 25 && Integer.valueOf((String) row[1]) >= 23)
    }

    sleep(20000)
    d_node.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                "regression_cluster_name0", "regression_cluster_id0");

    sleep(60000)
    resp = get_cluster.call(beUniqueIdList[1]);
    for (cluster : resp) {
        for (node : cluster.nodes) {
            if (node.ip == ipList[1] && node.heartbeat_port == hbPortList[1]) {
                assertEquals(node.status, "NODE_STATUS_DECOMMISSIONED");
            }
        }
    }

    result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
    for (row : result) {
        println row
    }
    assertEquals(result.size(), 2);
    for (row : result) {
        log.info("replica distribution: ${row} ".toString())
        assertTrue(Integer.valueOf((String) row[1]) == 0 || Integer.valueOf((String) row[1]) == 48)
    }

    setFeConfig("balance_tablet_percent_per_run", 0.05)
    sql """ drop table IF EXISTS table100 """
    sql """ drop table IF EXISTS table_p2 """
}
