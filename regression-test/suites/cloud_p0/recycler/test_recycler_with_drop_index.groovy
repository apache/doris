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

suite("test_recycler_with_drop_index") {
    // create table
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = 'test_recycler_with_drop_index'
    def mvName = "mv1"

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
        `lo_orderkey` bigint(20) NOT NULL COMMENT "",
        `lo_linenumber` bigint(20) NOT NULL COMMENT "",
        `lo_custkey` int(11) NOT NULL COMMENT "",
        `lo_partkey` int(11) NOT NULL COMMENT "",
        `lo_suppkey` int(11) NOT NULL COMMENT "",
        `lo_orderdate` int(11) NOT NULL COMMENT "",
        `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
        `lo_shippriority` int(11) NOT NULL COMMENT "",
        `lo_quantity` bigint(20) NOT NULL COMMENT "",
        `lo_extendedprice` bigint(20) NOT NULL COMMENT "",
        `lo_ordtotalprice` bigint(20) NOT NULL COMMENT "",
        `lo_discount` bigint(20) NOT NULL COMMENT "",
        `lo_revenue` bigint(20) NOT NULL COMMENT "",
        `lo_supplycost` bigint(20) NOT NULL COMMENT "",
        `lo_tax` bigint(20) NOT NULL COMMENT "",
        `lo_commitdate` bigint(20) NOT NULL COMMENT "",
        `lo_shipmode` varchar(11) NOT NULL COMMENT ""
        )
        PARTITION BY RANGE(`lo_orderdate`)
        (PARTITION p1992 VALUES [("-2147483648"), ("19930101")),
        PARTITION p1993 VALUES [("19930101"), ("19940101")),
        PARTITION p1994 VALUES [("19940101"), ("19950101")),
        PARTITION p1995 VALUES [("19950101"), ("19960101")),
        PARTITION p1996 VALUES [("19960101"), ("19970101")),
        PARTITION p1997 VALUES [("19970101"), ("19980101")),
        PARTITION p1998 VALUES [("19980101"), ("19990101")))
        DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 4;
    """

    String[][] tabletInfoList1 = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList1:${tabletInfoList1}")
    HashSet<String> tabletIdSet1 = new HashSet<String>()
    for (tabletInfo : tabletInfoList1) {
        tabletIdSet1.add(tabletInfo[0])
    }
    logger.info("tabletIdSet1:${tabletIdSet1}")

    // create indexes
    def getJobState = { tbName ->
        def jobStateResult = sql """  SHOW ALTER TABLE MATERIALIZED VIEW WHERE TableName='${tbName}' ORDER BY CreateTime DESC LIMIT 1; """
        return jobStateResult[0][8]
    }

    sql "CREATE materialized VIEW ${mvName} AS SELECT lo_orderkey as lo_orderkey_t, lo_linenumber as lo_linenumber_t FROM ${tableName};"
    int max_try_secs = 60
    while (max_try_secs--) {
        String res = getJobState(tableName)
        if (res == "FINISHED") {
            break
        } else {
            Thread.sleep(2000)
            if (max_try_secs < 1) {
                println "test timeout," + "state:" + res
                assertEquals("FINISHED",res)
            }
        }
    }

    String[][] tabletInfoList2 = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList2:${tabletInfoList2}")
    HashSet<String> tabletIdSet2 = new HashSet<String>()
    for (tabletInfo : tabletInfoList2) {
        tabletIdSet2.add(tabletInfo[0])
    }
    logger.info("tabletIdSet2:${tabletIdSet2}")

    HashSet<String> tabletIdSet3 = new HashSet<String>()
    for (tableId : tabletIdSet2) {
        if (!tabletIdSet1.contains(tableId)) {
            tabletIdSet3.add(tableId);
        }
    }
    logger.info("tabletIdSet3:${tabletIdSet3}")

    // load data
    def columns = """lo_orderkey,lo_linenumber,lo_custkey,lo_partkey,lo_suppkey,lo_orderdate,lo_orderpriority, 
                    lo_shippriority,lo_quantity,lo_extendedprice,lo_ordtotalprice,lo_discount, 
                    lo_revenue,lo_supplycost,lo_tax,lo_commitdate,lo_shipmode,lo_dummy"""

    for (int index = 0; index < 2; index++) {
        streamLoad {
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'
            set 'columns', columns
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file """${getS3Url()}/regression/ssb/sf0.1/lineorder.tbl.gz"""

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
        logger.info("index:${index}")
    }

    qt_sql """ select count(*) from ${tableName} """

    sql """ DROP MATERIALIZED VIEW ${mvName} ON ${tableName};"""
    int retry = 15
    boolean success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000)  // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet3)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)

    qt_sql """ select count(*) from ${tableName} """
    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    retry = 15
    success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000) // 20s
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet1)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
