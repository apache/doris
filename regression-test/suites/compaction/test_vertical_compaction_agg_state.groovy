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

suite("test_vertical_compaction_agg_state") {
    def tableName = "vertical_compaction_agg_state_regression_test"

    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql "set enable_agg_state=true"
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
                user_id                         VARCHAR,
                agg_user_id                     agg_state<collect_set(string)> generic
                )ENGINE=OLAP
        AGGREGATE  KEY(`user_id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             ('a',collect_set_state('a'))
            """

        sql """ INSERT INTO ${tableName} VALUES
            ('a',collect_set_state('aa'))
            """

        qt_select_default """ SELECT user_id,collect_set_merge(agg_user_id) FROM ${tableName} t group by user_id ORDER BY user_id;"""

        sql """ INSERT INTO ${tableName} VALUES
             ('b',collect_set_state('b'))
            """

        sql """ INSERT INTO ${tableName} VALUES
            ('a',collect_set_state('aaa'))
            """

        qt_select_default """ SELECT user_id,collect_set_merge(agg_user_id) FROM ${tableName} t group by user_id ORDER BY user_id;"""

        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // trigger compactions for all tablets in ${tableName}
        trigger_and_wait_compaction(tableName, "cumulative")

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        int rowCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                rowCount += Integer.parseInt(rowset.split(" ")[1])
            }
        }
        assert (rowCount < 8 * replicaNum)
        qt_select_default """ SELECT user_id,collect_set_merge(agg_user_id) FROM ${tableName} t group by user_id ORDER BY user_id;"""
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
