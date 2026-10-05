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

suite("test_compaction_cumu_delete") {
    def tableName = "test_compaction_cumu_delete"

    try {

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`id`, `name`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true" );
        """

        String backend_id;
        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablet = (sql """ show tablets from ${tableName}; """)[0]
        backend_id = tablet[2]
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        boolean disableAutoCompaction = true
        boolean allowDeleteWhenCumu = false
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
            if (((List<String>) ele)[0] == "enable_delete_when_cumu_compaction") {
                allowDeleteWhenCumu = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        if (!allowDeleteWhenCumu) {
            logger.info("Skip test compaction when cumu compaction because not enabled this config")
            return
        }

        // insert 11 values for 11 version
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (2, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        // [0-1] [2-12]
        // write some key in version 13, delete it in version 14, write same key in version 15
        // make sure the key in version 15 will not be deleted
        trigger_and_wait_compaction(tableName, "base")
        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        qt_select_default """ SELECT * FROM ${tableName}; """
        sql """ DELETE FROM ${tableName} WHERE id = 4; """
        qt_select_default """ SELECT * FROM ${tableName}; """
        // insert one value with prior delete key
        sql """ INSERT INTO ${tableName} VALUES (4, "a", 100); """
        // insert more data to trigger base compaction
        sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (5, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (6, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (7, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (7, "a", 100); """
        qt_select_default """ SELECT * FROM ${tableName}; """

        trigger_and_wait_compaction(tableName, "cumulative")
        qt_select_default """ SELECT * FROM ${tableName}; """

        trigger_and_wait_compaction(tableName, "base")
        qt_select_default """ SELECT * FROM ${tableName}; """
    } finally {
        // try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
