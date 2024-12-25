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

suite("test_full_compaction_with_ordered_data","nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    def tableName = "test_full_compaction_with_ordered_data"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    String backend_id;

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 3
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName} VALUES (0,0),(1,1),(2,2)"""
    sql """ delete from ${tableName} where k=0"""
    sql """ delete from ${tableName} where k=1"""
    sql """ delete from ${tableName} where k=2"""

    def exception = false;
    try {
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        // before full compaction, there are 12 rowsets.
        int rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 5 * replicaNum * 3)

        // trigger full compactions for all tablets in ${tableName}
        trigger_and_wait_compaction(tableName, "full")

        // after full compaction, there is only 1 rowset.
        rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 1 * replicaNum * 3)
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        assertFalse(exception)
    }

    sql """ delete from ${tableName} where k=0"""
    sql """ delete from ${tableName} where k=1"""
    sql """ delete from ${tableName} where k=2"""
    sql """ delete from ${tableName} where k=3"""
    sql """ delete from ${tableName} where k=4"""
    sql """ delete from ${tableName} where k=5"""
    sql """ delete from ${tableName} where k=6"""
    sql """ delete from ${tableName} where k=7"""
    sql """ delete from ${tableName} where k=8"""
    sql """ delete from ${tableName} where k=9"""
    sql """ INSERT INTO ${tableName} VALUES (10,10)"""

    GetDebugPoint().clearDebugPointsForAllBEs()

    exception = false;
    try {
        GetDebugPoint().enableDebugPointForAllBEs("FullCompaction.prepare_compact.set_cumu_point")
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        // before full compaction, there are 12 rowsets.
        int rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 12 * replicaNum * 3)

        // trigger full compactions for all tablets in ${tableName}
        trigger_and_wait_compaction(tableName, "full")
        // after full compaction, there is only 1 rowset.

        rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 1 * replicaNum * 3)
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("FullCompaction.prepare_compact.set_cumu_point")
        assertFalse(exception)
    }
}
