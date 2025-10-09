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

import org.apache.doris.regression.suite.ClusterOptions

suite('test_dictionary_fe_failover', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 3
    options.beNum = 1

    docker(options) {
        sql "drop database if exists test_dictionary_failover"
        sql "create database test_dictionary_failover"
        sql "use test_dictionary_failover"

        // source table and data
        sql """
            create table source_table(
                k1 varchar(100) not null,
                v1 int not null,
                v2 string not null
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            properties("replication_num" = "1");
        """
        sql """
            insert into source_table values 
            ('key1', 1, 'value1'),
            ('key2', 2, 'value2'),
            ('key3', 3, 'value3');
        """

        sql """
            create table source_table_iptrie(
                k1 varchar(100) not null,
                v1 int not null,
                v2 string not null
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            properties("replication_num" = "1");
        """
        sql """
            insert into source_table_iptrie values 
            ('10.0.10.0/24', 1, 'value1'),
            ('10.0.11.0/24', 2, 'value2'),
            ('10.0.12.0/24', 3, 'value3');
        """

        // create dictionary and verify
        sql """
            create dictionary dict1 using source_table
            (
                k1 KEY,
                v1 VALUE,
                v2 VALUE
            )LAYOUT(HASH_MAP)
            properties('data_lifetime'='600');
        """
        waitAllDictionariesReady()
        def dictResult = sql "SHOW DICTIONARIES"
        assertEquals(dictResult[0][1], "dict1")

        // the second dictionary
        sql """
            create dictionary dict_iptrie using source_table_iptrie
            (
                k1 KEY,
                v1 VALUE,
                v2 VALUE
            )LAYOUT(IP_TRIE)
            properties('data_lifetime'='600');
        """
        waitAllDictionariesReady()
        dictResult = sql "SHOW DICTIONARIES"
        assertEquals(dictResult.size(), 2)

        // restart FE Master
        def oldMasterFe = cluster.getMasterFe()
        cluster.restartFrontends(oldMasterFe.index)

        boolean hasRestart = false
        for (int i = 0; i < 30; i++) {
            if (cluster.getFeByIndex(oldMasterFe.index).alive) {
                hasRestart = true
                break
            }
            sleep(1000)
        }
        assertTrue(hasRestart)

        // reconnect to old master FE, which is follower now
        context.reconnectFe()
        sql "use test_dictionary_failover"
        def newMasterFe = cluster.getMasterFe()
        assertTrue(oldMasterFe.index != newMasterFe.index)

        // check metadata consistency
        def dictResult2 = sql "SHOW DICTIONARIES"
        assertEquals(dictResult2.size(), 2)

        // new one
        sql """
            create dictionary dict3 using source_table
            (
                k1 KEY,
                v1 VALUE,
                v2 VALUE
            )LAYOUT(HASH_MAP)
            properties('data_lifetime'='600');
        """
        waitAllDictionariesReady()
        def finalDictResult = sql "SHOW DICTIONARIES"
        assertEquals(finalDictResult.size(), 3)
        waitAllDictionariesReady()

        // check dictionary availability
        qt_sql1 """
            select dict_get('test_dictionary_failover.dict1', 'v1', k1) from source_table order by 1;
        """
        qt_sql2 """
            select dict_get('test_dictionary_failover.dict_iptrie', 'v2', cast('10.0.11.123' as ipv4)) order by 1;
        """

        // connect to follower(old FE master, surely be follower now)
        def url = String.format(
                    "jdbc:mysql://%s:%s/?useLocalSessionState=true&allowLoadLocalInfile=false",
                    oldMasterFe.host, oldMasterFe.queryPort)
        context.connectTo(url, context.config.jdbcUser, context.config.jdbcPassword)
        sql "use test_dictionary_failover"

        // check metadata consistency
        finalDictResult = sql "SHOW DICTIONARIES"
        assertEquals(finalDictResult.size(), 3)
        // check dictionary availability
        qt_sql3 """
            select dict_get('test_dictionary_failover.dict1', 'v1', k1) from source_table order by 1;
        """
        qt_sql4 """
            select dict_get('test_dictionary_failover.dict_iptrie', 'v2', cast('10.0.11.123' as ipv4)) order by 1;
        """
    }
}
