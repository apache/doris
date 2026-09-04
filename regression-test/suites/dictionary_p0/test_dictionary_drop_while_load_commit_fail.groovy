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

// Regress the race reported in DORIS-27820: an async dictionary load task writes the INC version
// journal, then DROP deletes the dictionary, then the failed BE commit writes a DEC journal for
// the already dropped dictionary, making all followers exit when replaying it.
//
// With the fix, the DEC journal is skipped when the dictionary was dropped during commit, and
// replay of DEC journals of dropped dictionaries is a no-op, so the whole journal stream
// (CREATE -> INC -> DROP) replays cleanly and FEs stay healthy after restart.
suite('test_dictionary_drop_while_load_commit_fail', 'docker') {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.feNum = 3
    options.beNum = 1
    options.enableDebugPoints()

    docker(options) {
        sql "drop database if exists test_dictionary_drop_race"
        sql "create database test_dictionary_drop_race"
        sql "use test_dictionary_drop_race"

        sql """
            create table source_table(
                k1 varchar(100) not null,
                v1 int not null
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            properties("replication_num" = "1");
        """
        sql "insert into source_table values ('k1', 1), ('k2', 2), ('k3', 3)"

        // Block the load task right after the INC journal is written, before BE commit.
        GetDebugPoint().enableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
        try {
            sql """
                create dictionary dict1 using source_table
                (
                    k1 KEY,
                    v1 VALUE
                )LAYOUT(HASH_MAP)
                properties('data_lifetime'='600');
            """

            // wait until the load task is parked at the block point.
            // status is LOADING before the block, and the task cannot pass the block, so once we
            // observe LOADING for a grace period, INC journal is guaranteed already written.
            boolean loading = false
            for (int i = 0; i < 40; i++) {
                def res = sql "SHOW DICTIONARIES"
                if (res.size() == 1 && res[0][4] == "LOADING") {
                    loading = true
                    break
                }
                sleep(500)
            }
            assertTrue(loading)
            sleep(1500)

            // DROP the dictionary while the load task is between INC journal and commit
            sql "drop dictionary dict1"
            def dictRes = sql "SHOW DICTIONARIES"
            assertEquals(dictRes.size(), 0)

            // force the BE commit to fail, then release the blocked load task
            GetDebugPoint().enableDebugPointForAllFEs("DictionaryManager.commitNowVersion.fail")
            GetDebugPoint().disableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
            sleep(3000)

            // master must stay healthy: no DEC journal should have been written after DROP
            assertTrue(cluster.getMasterFe().alive)

            // restart the master: it must replay the whole journal stream without exit.
            // if a DEC journal for the dropped dictionary had been written, replay would throw
            // and the FE would never come back alive.
            def master = cluster.getMasterFe()
            cluster.restartFrontends(master.index)
            boolean hasRestart = false
            for (int i = 0; i < 60; i++) {
                if (cluster.getFeByIndex(master.index).alive) {
                    hasRestart = true
                    break
                }
                sleep(1000)
            }
            assertTrue(hasRestart)

            context.reconnectFe()
            sql "use test_dictionary_drop_race"
            def finalRes = sql "SHOW DICTIONARIES"
            assertEquals(finalRes.size(), 0)
        } finally {
            GetDebugPoint().disableDebugPointForAllFEs("DictionaryManager.commitNowVersion.fail")
            GetDebugPoint().disableDebugPointForAllFEs("DictionaryManager.afterIncJournal")
        }
    }
}
