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

suite("test_base_compaction_no_value", "p2") {
    def tableName = "base_compaction_uniq_keys_no_value"

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

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
          L_ORDERKEY    INTEGER NOT NULL,
          L_PARTKEY     INTEGER NOT NULL,
          L_SUPPKEY     INTEGER NOT NULL,
          L_LINENUMBER  INTEGER NOT NULL,
          L_QUANTITY    DECIMAL(15,2) NOT NULL,
          L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
          L_DISCOUNT    DECIMAL(15,2) NOT NULL,
          L_TAX         DECIMAL(15,2) NOT NULL,
          L_RETURNFLAG  CHAR(1) NOT NULL,
          L_LINESTATUS  CHAR(1) NOT NULL,
          L_SHIPDATE    DATE NOT NULL,
          L_COMMITDATE  DATE NOT NULL,
          L_RECEIPTDATE DATE NOT NULL,
          L_SHIPINSTRUCT CHAR(25) NOT NULL,
          L_SHIPMODE     CHAR(10) NOT NULL,
          L_COMMENT      VARCHAR(44) NOT NULL
        )
        UNIQUE KEY(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT)
        DISTRIBUTED BY HASH(L_ORDERKEY) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1",
          "disable_auto_compaction" = "true"
        )

    """

    streamLoad {
        // a default db 'regression_test' is specified in
        // ${DORIS_HOME}/conf/regression-conf.groovy
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'
        set 'compress_type', 'GZ'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """${getS3Url()}/regression/tpch/sf1/lineitem.csv.split01.gz"""

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    streamLoad {
        // a default db 'regression_test' is specified in
        // ${DORIS_HOME}/conf/regression-conf.groovy
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'
        set 'compress_type', 'GZ'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """${getS3Url()}/regression/tpch/sf1/lineitem.csv.split01.gz"""

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId
        (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        assertEquals("success", compactJson.status.toLowerCase())
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    streamLoad {
        // a default db 'regression_test' is specified in
        // ${DORIS_HOME}/conf/regression-conf.groovy
        table tableName

        // default label is UUID:
        // set 'label' UUID.randomUUID().toString()

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', '|'
        set 'compress_type', 'GZ'

        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file """${getS3Url()}/regression/tpch/sf1/lineitem.csv.split00.gz"""

        time 10000 // limit inflight 10s

        // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId
        (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        assertEquals("success", compactJson.status.toLowerCase())
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    qt_select_default """ SELECT count(*) FROM ${tableName} """

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId
        (code, out, err) = be_run_base_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        assertEquals("success", compactJson.status.toLowerCase())
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        boolean running = true
        do {
            Thread.sleep(1000)
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

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
    qt_select_default2 """ SELECT count(*) FROM ${tableName} """
}
