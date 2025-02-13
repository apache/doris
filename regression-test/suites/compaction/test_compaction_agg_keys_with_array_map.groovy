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

suite("test_compaction_agg_keys_with_array_map") {
    def tableName = "compaction_agg_keys_regression_test_complex"

    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
        backend_id = backendId_to_backendIP.keySet()[0]

        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `user_id` LARGEINT NOT NULL COMMENT "用户id",
                `date` DATE NOT NULL COMMENT "数据灌入日期时间",
                `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_1` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
                `datetimev2_2` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
                `city` VARCHAR(20) COMMENT "用户所在城市",
                `age` SMALLINT COMMENT "用户年龄",
                `sex` TINYINT COMMENT "用户性别",
                `array_col` ARRAY<STRING> REPLACE NULL COMMENT "array column",
                `map_col` MAP<STRING, INT> REPLACE NULL COMMENT "map column")
            AGGREGATE KEY(`user_id`, `date`, `datev2`, `datetimev2_1`, `datetimev2_2`, `city`, `age`, `sex`) DISTRIBUTED BY HASH(`user_id`)
            PROPERTIES ( "replication_num" = "1" );
        """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['a', 'b'], map('a', 1));
            """

        sql """ INSERT INTO ${tableName} VALUES
             (1, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['a', 'b', 'c'], map('b', 1, 'c', 2));
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['amory', 'doris', 'commiter'], map('b', 1));
            """

        sql """ INSERT INTO ${tableName} VALUES
             (2, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['amory', 'doris', '2024-04-29'], map('c', 2));
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['e', 'f', 'g', 'd'], map());
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, ['e', 'f', 'g', 'd'], map('a', 1, 'b', 2));
            """

        sql """ INSERT INTO ${tableName} VALUES
             (3, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, NULL, NULL);
            """

        sql """ INSERT INTO ${tableName} VALUES
             (4, '2017-10-01', '2017-10-01', '2017-10-01 11:11:11.110000', '2017-10-01 11:11:11.110111', 'Beijing', 10, 1, [NULL, 'sdf'], NULL);
            """

        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,QueryHits,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        // trigger compactions for all tablets in ${tableName}
        trigger_and_wait_compaction(tableName, "cumulative")

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        int rowCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId

            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)

            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                rowCount += Integer.parseInt(rowset.split(" ")[1])
            }
        }
        assert (rowCount < 8 * replicaNum)
        qt_select_default2 """ SELECT * FROM ${tableName} t ORDER BY user_id; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}

