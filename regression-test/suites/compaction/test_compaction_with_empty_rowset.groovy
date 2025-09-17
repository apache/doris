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
import org.awaitility.Awaitility

suite("test_compaction_mow_with_empty_rowset", "p0") {
    def tableName = "test_compaction_with_empty_rowset"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    sql """
    CREATE TABLE IF NOT EXISTS ${tableName} (
      `k1` int(11) NULL,
      `k2` tinyint(4) NULL,
      `k3` smallint(6) NULL,
      `k4` int(30) NULL,
      `k5` largeint(40) NULL,
      `k6` float NULL,
      `k7` double NULL,
      `k8` decimal(9, 0) NULL,
      `k9` char(10) NULL,
      `k10` varchar(1024) NULL,
      `k11` text NULL,
      `k12` date NULL,
      `k13` datetime NULL
    ) ENGINE=OLAP
    unique KEY(k1, k2, k3)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 2
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
    );
    """

    for (int i = 0; i < 10; i++) {
        sql """ insert into ${tableName} values (1, 2, 3, 4, 5, 6.6, 1.7, 8.8,
                'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """
    }   

    qt_sql """ select * from ${tableName} order by k1, k2, k3 """


    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    def replicaNum = get_table_replica_num(tableName)
    logger.info("get table replica num: " + replicaNum)

    // trigger compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")
    int rowCount = 0
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        def (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            rowCount += Integer.parseInt(rowset.split(" ")[1])
        }
    }
    assert (rowCount < 10 * replicaNum)
    qt_sql2 """ select * from ${tableName} order by k1, k2, k3 """

    for (int i = 0; i < 10; i++) {
        sql """ insert into ${tableName} values (2, 2, 3, 4, 5, 6.6, 1.7, 8.8,
                'a', 'b', 'c', '2021-10-30', '2021-10-30 00:00:00') """
    }   

    // trigger compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(tableName, "cumulative")
    rowCount = 0
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        def (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            rowCount += Integer.parseInt(rowset.split(" ")[1])
        }
    }
    assert (rowCount < 20 * replicaNum)
    qt_sql3 """ select * from ${tableName} order by k1, k2, k3 """
}
