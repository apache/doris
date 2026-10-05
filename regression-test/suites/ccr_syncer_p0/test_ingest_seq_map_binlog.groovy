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

suite("test_ingest_seq_map_binlog") {

    def syncer = getSyncer()
    if (!syncer.checkEnableFeatureBinlog()) {
        logger.info("fe enable_feature_binlog is false, skip case test_ingest_seq_map_binlog")
        return
    }

    def tableName = "tbl_ingest_seq_map_binlog"
    def insert_num = 5
    def test_num = 0
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
           CREATE TABLE ${tableName} (
            `a` bigint(20) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
        """
    sql """ALTER TABLE ${tableName} set ("binlog.enable" = "true")"""

    target_sql "DROP TABLE IF EXISTS ${tableName}"
    target_sql """
            CREATE TABLE ${tableName} (
            `a` bigint(20) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
        """
    assertTrue(syncer.getTargetMeta("${tableName}"))

    logger.info("=== Test 1: Common ingest binlog case ===")
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index}+1, ${index}+1, ${index}+1, ${index}+1, ${index}+1);
        """
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.ingestBinlog())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        syncer.closeBackendClients()
    }
    // write two times, but above writes will be the final result
    for (int index = 0; index < insert_num; index++) {
        sql """
            INSERT INTO ${tableName} VALUES (${index}, ${index}, ${index}, ${index}, ${index}, ${index});
        """
        assertTrue(syncer.getBinlog("${tableName}"))
        assertTrue(syncer.beginTxn("${tableName}"))
        assertTrue(syncer.getBackendClients())
        assertTrue(syncer.ingestBinlog())
        assertTrue(syncer.commitTxn())
        assertTrue(syncer.checkTargetVersion())
        syncer.closeBackendClients()
    }

    target_sql " sync "
    def res = target_sql """SELECT * FROM ${tableName} order by a"""
    assertEquals(res.size(), insert_num)
    for (int index = 0; index < insert_num; index++) {
        assertEquals(res[index][0], index)
        assertEquals(res[index][1], index+1)
        assertEquals(res[index][2], index+1)
        assertEquals(res[index][3], index+1)
        assertEquals(res[index][4], index+1)
        assertEquals(res[index][5], index+1)
    }
    syncer.closeBackendClients()
}
