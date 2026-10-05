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
import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_recycler_with_txn_label") {
    // create table
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = 'test_recycler_with_txn_label'

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE `${tableName}`
        (
            `siteid` INT DEFAULT '10',
            `citycode` SMALLINT,
            `username` VARCHAR(32) DEFAULT 'test',
            `pv` BIGINT SUM DEFAULT '0'
        )
        AGGREGATE KEY(`siteid`, `citycode`, `username`)
        DISTRIBUTED BY HASH(siteid) BUCKETS 1;
    """

    String label = UUID.randomUUID().toString()
    streamLoad {
        table tableName

        // default label is UUID:
        set 'label', "${label}"

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','
        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'table1_data.csv'

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

    qt_sql """ select count(*) from ${tableName};"""

    streamLoad {
        table tableName
        // default label is UUID:
        set 'label', "${label}"

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','
        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'table1_data.csv'

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
            assertEquals("label already exists", json.Status.toLowerCase())
        }
    }

    qt_sql """ select count(*) from ${tableName};"""

    int retry = 10
    boolean success = false
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(60000) // 60s
    } while (retry--)

    qt_sql """ select count(*) from ${tableName};"""

    streamLoad {
        table tableName

        // default label is UUID:
        set 'label', "${label}"

        // default column_separator is specify in doris fe config, usually is '\t'.
        // this line change to ','
        set 'column_separator', ','
        // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
        // also, you can stream load a http stream, e.g. http://xxx/some.csv
        file 'table1_data.csv'

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

    String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList:${tabletInfoList}")

    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tabletInfoList) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    retry = 3
    success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(50000)
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
