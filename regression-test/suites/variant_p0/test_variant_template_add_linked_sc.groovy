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

// A template ADDed over a path that old rowsets keep in sparse storage, followed by a heavy schema
// change that can link the old segments (a fourth key column keeps three short keys and adds no
// expression or index change). Old sparse values must keep their stored form after compaction:
// sparse statistics are saturated, so only a sparse key scan can find them.
suite("test_variant_template_add_linked_sc", "p0,nonConcurrent") {
    sql "set default_variant_enable_doc_mode = false"
    def props = 'PROPERTIES("variant_max_subcolumns_count" = "1", "variant_max_sparse_column_statistics_size" = "1")'
    def tbl = "test_variant_template_add_linked_sc"
    def ctl = "test_variant_template_add_linked_sc_ctl"
    def fullCompaction = { String t ->
        trigger_and_wait_compaction(t, "full", 300)
    }
    for (t in [tbl, ctl]) {
        sql "DROP TABLE IF EXISTS ${t}"
        sql """CREATE TABLE ${t} (k1 INT, k2 INT, k3 INT, v variant<${props}>)
               DUPLICATE KEY(k1, k2, k3) DISTRIBUTED BY HASH(k1) BUCKETS 1
               PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")"""
    }
    // Old rowsets: a frequent path a0 takes the single subcolumn slot; p (a non-integer string) and
    // many other paths go to sparse, and the statistics keep only one sparse path.
    def oldRows = []
    for (int i = 1; i <= 60; i++) {
        def obj = [a0: i, b0: i, ("q${i % 7}".toString()): i, ("r${i % 5}".toString()): "s${i}".toString()]
        if (i % 3 == 1) obj["p"] = "x${i}".toString()
        oldRows << "(${i}, ${i}, ${i}, parse_to_variant('${groovy.json.JsonOutput.toJson(obj)}'))"
    }
    for (t in [tbl, ctl]) {
        sql "INSERT INTO ${t} VALUES ${oldRows[0..29].join(',')}"
        sql "INSERT INTO ${t} VALUES ${oldRows[30..59].join(',')}"
    }
    sql "ALTER TABLE ${tbl} MODIFY COLUMN v variant<MATCH_NAME 'p':int, ${props}>"
    waitForSchemaChangeDone {
        sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY createtime DESC LIMIT 1"
        time 600
    }
    // Heavy schema change on both tables that can be served by linking the old segments.
    for (t in [tbl, ctl]) {
        sql "ALTER TABLE ${t} ADD COLUMN k4 INT KEY DEFAULT '0' AFTER k3"
        waitForSchemaChangeDone {
            sql "SHOW ALTER TABLE COLUMN WHERE TableName='${t}' ORDER BY createtime DESC LIMIT 1"
            time 600
        }
    }
    def newRows = []
    for (int i = 61; i <= 90; i++) {
        def obj = [a0: i, p: i]
        newRows << "(${i}, ${i}, ${i}, 0, parse_to_variant('${groovy.json.JsonOutput.toJson(obj)}'))"
    }
    for (t in [tbl, ctl]) {
        sql "INSERT INTO ${t} VALUES ${newRows.join(',')}"
    }
    def check = { String stage ->
        for (String q : ["SELECT k1, CAST(v['p'] AS STRING) FROM @T ORDER BY k1",
                         "SELECT k1, CAST(v AS STRING) FROM @T ORDER BY k1",
                         "SELECT COUNT(*) FROM @T WHERE v['p'] IS NOT NULL"]) {
            def a = sql q.replace("@T", tbl)
            def e = sql q.replace("@T", ctl)
            assertEquals(e, a, "${stage}: ${q}")
        }
        def old = sql "SELECT CAST(v['p'] AS STRING) FROM ${tbl} WHERE k1 = 7"
        assertEquals("x7", old[0][0], stage)
    }
    check("before compaction")
    def dp = "variant_util.collect_compaction_schema.added_template_key_scan"
    def scanned = false
    try {
        GetDebugPoint().enableDebugPointForAllBEs(dp)
        trigger_and_wait_compaction(tbl, "full", 300, new String[0], [], new String[0], true)
    } catch (Throwable e) {
        logger.info("R1 probe: key scan ran: ${e.message}")
        scanned = true
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs(dp)
    }
    logger.info("R1 probe scanned=${scanned}")
    for (t in [tbl, ctl]) {
        fullCompaction(t)
    }
    check("after full compaction")
}
