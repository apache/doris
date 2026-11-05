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

suite("dotted_key_escape_compaction_failure", "p0,nonConcurrent") {
    sql "set group_commit='off_mode'"
    sql "set enable_sql_cache=false"
    sql "set enable_query_cache=false"
    sql "DROP TABLE IF EXISTS dotted_key_escape_compaction_failure"
    sql """CREATE TABLE dotted_key_escape_compaction_failure (
        id INT, v VARIANT<PROPERTIES("variant_escape_dots_in_keys"="true",
                                    "variant_enable_nested_group"="false")>)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                   "compaction_policy"="time_series")"""
    sql """INSERT INTO dotted_key_escape_compaction_failure VALUES
        (1,CAST(CAST('{"a.b":1,"a":{"b":2}}' AS JSON) AS VARIANT))"""
    sql """INSERT INTO dotted_key_escape_compaction_failure VALUES
        (2,CAST(CAST('{"a.b":11,"a":{"b":12}}' AS JSON) AS VARIANT))"""
    def hosts = [:]
    def ports = [:]
    getBackendIpHttpPort(hosts, ports)
    def tablet = sql_return_maparray("SHOW TABLETS FROM dotted_key_escape_compaction_failure")[0]
    def status = {
        def (code, out, err) = be_show_tablet_status(hosts["${tablet.BackendId}"],
                ports["${tablet.BackendId}"], tablet.TabletId)
        assertEquals(0, code, err)
        return parseJson(out)
    }
    def rowsetIds = { value -> value.rowsets.collect { it.split(/\s+/)[4] }.toSet() }
    def inputs = rowsetIds(status())
    setBeConfigTemporary([enable_ordered_data_compaction: false]) {
        String point = "BetaRowsetWriter._check_segment_number_limit_too_many_segments"
        GetDebugPoint().enableDebugPointForAllBEs(point, [segnum: 1000000])
        Throwable failure = null
        try {
            trigger_and_wait_compaction("dotted_key_escape_compaction_failure", "full", 30)
        } catch (AssertionError error) {
            failure = error
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(point)
        }
        assertNotNull(failure, "The compaction helper accepted a failed task")
        assertTrue(failure.message.contains("too many segments"), failure.message)
        logger.info("Expected compaction failure: ${failure.message}")
        assertEquals(inputs, rowsetIds(status()), "Failed compaction replaced its inputs")
        qt_after_failure """SELECT id,v,v['a.b'],v['a']['b']
            FROM dotted_key_escape_compaction_failure ORDER BY id"""
        trigger_and_wait_compaction("dotted_key_escape_compaction_failure", "full")
        def output = status()
        assertEquals("[OK]", output['last full status'])
        assertTrue(inputs.intersect(rowsetIds(output)).isEmpty(), "Successful compaction kept its inputs")
        assertEquals(1, rowsetIds(output).size())
        qt_after_success """SELECT id,v,v['a.b'],v['a']['b']
            FROM dotted_key_escape_compaction_failure ORDER BY id"""
    }
}
