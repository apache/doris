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

suite("test_compaction_with_multi_append_columns", "p0") {
    def tableName = "test_compaction_with_multi_append_columns"

    def execStreamLoad = {
        streamLoad {
            table "${tableName}"

            set 'column_separator', ','

            file 'all_types.csv'
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(2500, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }
    }

    def checkNoDuplicatedKeys = {
        def res = sql """ show tablets from ${tableName}; """
        log.info("tablets: " + res)

        def rowCount = sql """ select count() from ${tableName}; """
        log.info("rowCount: " + rowCount)

        List<List<Object>> cnt = sql """ select k1,k2,k3,count(*) a  from ${tableName} group by k1,k2,k3 having a > 1; """
        // log.info("ensure there are no duplicated keys")
        if (cnt.size() > 0) {
            log.info("find duplicated keys: " + cnt.get(0))
        }
        assertEquals(0, cnt.size())
    }

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
    CLUSTER BY (`k13`, `k2`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 1
    PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
    );
    """

    // 1. load data
    for (int i = 0; i < 10; i++) {
        execStreamLoad()
        checkNoDuplicatedKeys(tableName)
    }

    // 2. compaction
    // get be info
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    // get config 'disableAutoCompaction'
    boolean disableAutoCompaction = true
    if (true) {
        String backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }
    }
    logger.info("disableAutoCompaction: " + disableAutoCompaction)
    // trigger compactions for all tablets in ${tableName}
    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        def backend_id = tablet.BackendId

        def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        logger.info("compact json: " + compactJson)
        if (compactJson.status.toLowerCase() == "fail") {
            assertEquals(disableAutoCompaction, false)
            logger.info("Compaction was done automatically!")
        }
        if (disableAutoCompaction) {
            assertEquals("success", compactJson.status.toLowerCase())
        }

        for (int i = 0; i < 10; i++) {
            (code, out, err) = be_show_tablet_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("loop " + i + ", Show tablet status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def json = parseJson(out.trim())
            logger.info("tablet rowsets: " + json)
            if (json.rowsets.size() <= 5) {
                break
            }
            sleep(2000)
        }
    }
    checkNoDuplicatedKeys(tableName)

    // 3. load data
    execStreamLoad()
    checkNoDuplicatedKeys(tableName)
}

