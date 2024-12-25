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

suite("test_time_series_compaction_polciy", "p0") {
    def tableName = "test_time_series_compaction_polciy"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def get_rowset_count = { tablets ->
        int rowsetCount = 0
        for (def tablet in tablets) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    sql """ DROP TABLE IF EXISTS ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NULL,
            `name` varchar(255) NULL,
            `hobbies` text NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "compaction_policy" = "time_series"
        );
    """
    // insert 16 lines, BUCKETS = 2
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (1, "bason", "bason hate pear", 99); """
    sql """ INSERT INTO ${tableName} VALUES (1, "andy", "andy love apple", 100); """
    sql """ INSERT INTO ${tableName} VALUES (100, "andy", "andy love apple", 100); """

    qt_sql_1 """ select count() from ${tableName} """

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    int replicaNum = 1
    def dedup_tablets = deduplicate_tablets(tablets)
    if (dedup_tablets.size() > 0) {
        replicaNum = Math.round(tablets.size() / dedup_tablets.size())
        if (replicaNum != 1 && replicaNum != 3) {
            assert(false)
        }
    }

    // BUCKETS = 2
    // before cumulative compaction, there are 17 * 2 = 34 rowsets.
    int rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 34 * replicaNum)

    // trigger cumulative compactions for all tablets in table
    trigger_and_wait_compaction(tableName, "cumulative")

    // after cumulative compaction, there is only 26 rowset.
    // 5 consecutive empty versions are merged into one empty version
    // 34 - 2*4 = 26
    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 26 * replicaNum)

    // trigger cumulative compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")

    // after cumulative compaction, there is only 22 rowset.
    // 26 - 4 = 22
    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 22 * replicaNum)

    qt_sql_2 """ select count() from ${tableName}"""
    if (isCloudMode()) {
        return;
    }
    sql """ alter table ${tableName} set ("time_series_compaction_file_count_threshold"="10")"""
    sql """sync"""
    // trigger cumulative compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")

    // after cumulative compaction, there is only 11 rowset.
    rowsetCount = get_rowset_count.call(tablets);
    assert (rowsetCount == 11 * replicaNum)
    qt_sql_3 """ select count() from ${tableName}"""
}
