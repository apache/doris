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

// A fixed partial update copies an old VARIANT verbatim after a template ADD. The copied path
// that does not fit the template must keep its old value even when the dynamic budget is
// exhausted by hotter paths.
suite("test_variant_template_carrier_budget", "p0") {
    def tbl = "test_variant_template_carrier_budget"
    def ctl = "${tbl}_ctl"
    sql "set default_variant_enable_doc_mode = false"
    sql "DROP TABLE IF EXISTS ${tbl}"
    sql "DROP TABLE IF EXISTS ${ctl}"
    for (t in [tbl, ctl]) {
        sql """CREATE TABLE ${t} (k INT, c INT, v variant<PROPERTIES("variant_max_subcolumns_count"="1")>)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                       "disable_auto_compaction"="true")"""
    }
    def rows = (1..30).collect { i ->
        def p = (i % 5 == 0) ? "\"p\":\"00${i}\"," : ""
        "(${i}, 0, '{${p}\"z\":${i},\"y\":${i}}')"
    }.join(",")
    sql "INSERT INTO ${tbl} VALUES ${rows}"
    sql "INSERT INTO ${ctl} VALUES ${rows}"
    sql """ALTER TABLE ${tbl} MODIFY COLUMN v variant<MATCH_NAME 'p':int,
            PROPERTIES("variant_max_subcolumns_count"="1")>"""
    waitForSchemaChangeDone { sql "SHOW ALTER TABLE COLUMN WHERE TableName='${tbl}' ORDER BY CreateTime DESC LIMIT 1"; time 600 }
    // Fold the old rowset first so the later cumulative inputs carry no pre-ADD schema.
    trigger_and_wait_compaction(tbl, "full")
    def pu = (1..30).collect { "(${it}, 1)" }.join(",")
    for (t in [tbl, ctl]) {
        sql "SET enable_unique_key_partial_update=true"
        sql "SET enable_insert_strict=false"
        sql "INSERT INTO ${t} (k, c) VALUES ${pu}"
        sql "SET enable_unique_key_partial_update=false"
    }
    def check = { tag ->
        def a = sql "SELECT k, cast(v as string), cast(v['p'] as string) FROM ${tbl} ORDER BY k"
        def b = sql "SELECT k, cast(v as string), cast(v['p'] as string) FROM ${ctl} ORDER BY k"
        logger.info("${tag}: ${a.findAll { it[0] % 5 == 0 }}")
        assertEquals(b, a)
    }
    check("before compaction")
    for (t in [tbl, ctl]) {
        sql """INSERT INTO ${t} VALUES (100, 0, '{"z":100,"y":100}')"""
        sql """INSERT INTO ${t} VALUES (101, 0, '{"z":101,"y":101}')"""
    }
    trigger_and_wait_compaction(tbl, "cumulative")
    check("after cumulative")
    trigger_and_wait_compaction(tbl, "full")
    check("after compaction")
}
