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

suite("test_config_prune_delete_sign", "nonConcurrent") {

    def inspectRows = { sqlStr ->
        sql "set skip_delete_sign=true;"
        sql "set skip_delete_bitmap=true;"
        sql "sync"
        qt_inspect sqlStr
        sql "set skip_delete_sign=false;"
        sql "set skip_delete_bitmap=false;"
        sql "sync"
    }

    def custoBeConfig = [
        enable_prune_delete_sign_when_base_compaction : false
    ]

    setBeConfigTemporary(custoBeConfig) {
        def table1 = "test_config_prune_delete_sign"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int NOT NULL,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_mow_light_delete" = "false",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        def getDeleteSignCnt = {
            sql "set skip_delete_sign=true;"
            sql "set skip_delete_bitmap=true;"
            sql "sync"
            qt_del_cnt "select count() from ${table1} where __DORIS_DELETE_SIGN__=1;"
            sql "set skip_delete_sign=false;"
            sql "set skip_delete_bitmap=false;"
            sql "sync"
        }

        (1..30).each {
            sql "insert into ${table1} values($it,$it,$it);"
        }
        trigger_and_wait_compaction(table1, "cumulative")

        sql "delete from ${table1} where k1<=20;"
        sql "sync;"
        qt_sql "select count() from ${table1};"
        getDeleteSignCnt()

        (31..60).each {
            sql "insert into ${table1} values($it,$it,$it);"
        }
        trigger_and_wait_compaction(table1, "cumulative")

        trigger_and_wait_compaction(table1, "base")
        qt_sql "select count() from ${table1};"
        getDeleteSignCnt()

        def tablets = sql_return_maparray """ show tablets from ${table1}; """
        logger.info("tablets: ${tablets}")
        String compactionUrl = tablets[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assert code == 0
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets.size() == 1
        assert tabletJson.rowsets[0].contains("[0-62]")
    }
}
