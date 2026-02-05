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

suite("table_rebalance") {
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

    def testFunc = { ->
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
        sql """ use @regression_cluster_name0"""
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
            PARTITION p1998 VALUES [("19980101"), ("19990101")),
            PARTITION p1999 VALUES [("19990101"), ("20000101")),
            PARTITION p2000 VALUES [("20000101"), ("20010101")),
            PARTITION p2001 VALUES [("20010101"), ("20020101")),
            PARTITION p2002 VALUES [("20020101"), ("20030101")),
            PARTITION p2003 VALUES [("20030101"), ("20040101")),
            PARTITION p2004 VALUES [("20040101"), ("20050101")),
            PARTITION p2005 VALUES [("20050101"), ("20060101")),
            PARTITION p2006 VALUES [("20060101"), ("20070101")),
            PARTITION p2007 VALUES [("20070101"), ("20080101")),
            PARTITION p2008 VALUES [("20080101"), ("20090101")),
            PARTITION p2009 VALUES [("20090101"), ("20100101")),
            PARTITION p2010 VALUES [("20100101"), ("20110101")),
            PARTITION p2011 VALUES [("20110101"), ("20120101")),
            PARTITION p2012 VALUES [("20120101"), ("20130101")),
            PARTITION p2013 VALUES [("20130101"), ("30000000")))
            DISTRIBUTED BY HASH(k1) BUCKETS 2
        """

        for (int i = 0; i < 100; ++i) {
            int num = 19930201 + i * 3000
            sql """
                insert into table_p2 values ($num, 'a', 1);
            """
        }

        sleep(20000)

        add_node.call(beUniqueIdList[2], ipList[2], hbPortList[2],
                      "regression_cluster_name0", "regression_cluster_id0");
        sleep(240000)

        sql """ use @regression_cluster_name0 """

        result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM table100; """
        assertEquals(result.size(), 3);

        for (row : result) {
            log.info("replica distribution: ${row} ".toString())
            assertTrue(Integer.valueOf((String) row[1]) <= 17 && Integer.valueOf((String) row[1]) >= 15)
        }

        for (int i = 1992; i < 2014; ++i) {
            result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 PARTITION(p$i) """
            assertEquals(result.size(), 3);
            for (row : result) {
                log.info("replica distribution: ${row} ".toString())
                assertTrue(Integer.valueOf((String) row[1]) <= 1 && Integer.valueOf((String) row[1]) >= 0)
            }
        }

        result = sql """ ADMIN SHOW REPLICA DISTRIBUTION FROM table_p2 """
        assertEquals(result.size(), 3);
        for (row : result) {
            log.info("replica distribution: ${row} ".toString())
            assertTrue(Integer.valueOf((String) row[1]) <= 16 && Integer.valueOf((String) row[1]) >= 14)
        }

    }

    def balance_tablet_percent_per_run = 0.5
    for (def enable_global_balance in [false, true]) {
        for (def preheating_enabled in [false, true]) {
            log.info("test table rebalance with balance_tablet_percent_per_run=${balance_tablet_percent_per_run}, " 
                    + "enable_global_balance=${enable_global_balance}, preheating_enabled=${preheating_enabled}")
            try {
                setFeConfig('balance_tablet_percent_per_run', balance_tablet_percent_per_run)
                setFeConfig('enable_global_balance', enable_global_balance)
                setFeConfig('preheating_enabled', preheating_enabled)
                testFunc.call()
            } finally {
                setFeConfig('balance_tablet_percent_per_run', 0.05)
                setFeConfig('enable_global_balance', true)
                setFeConfig('preheating_enabled', true)
            }
        }
    }
    sql """ drop table IF EXISTS table100 """
    sql """ drop table IF EXISTS table_p2 """
}
