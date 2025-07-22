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

suite("test_recycler_inverted_index") {
    // create table
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId;
    def tableName = 'test_recycler_inverted_index'

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
        `lo_shipmode` varchar(11) NOT NULL COMMENT "",
        INDEX lo_orderkey_idx(lo_orderkey) USING INVERTED COMMENT "lo_orderkey index",
        INDEX lo_linenumber_idx(lo_linenumber) USING INVERTED COMMENT "lo_linenumber index",
        INDEX lo_custkey_idx(lo_custkey) USING INVERTED COMMENT "lo_custkey index",
        INDEX lo_partkey_idx(lo_partkey) USING INVERTED COMMENT "lo_partkey index",
        INDEX lo_suppkey_idx(lo_suppkey) USING INVERTED COMMENT "lo_suppkey index",
        INDEX lo_orderpriority_idx(lo_orderpriority) USING INVERTED PROPERTIES("parser"="standard") COMMENT "lo_orderpriority index",
        INDEX lo_shipmode_idx(lo_shipmode) USING INVERTED PROPERTIES("parser"="english") COMMENT "lo_shipmode index",
        INDEX lo_commitdate_idx(lo_commitdate) USING INVERTED COMMENT "lo_commitdate index"
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

    String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
    logger.info("tabletInfoList:${tabletInfoList}")
    HashSet<String> tabletIdSet= new HashSet<String>()
    for (tabletInfo : tabletInfoList) {
        tabletIdSet.add(tabletInfo[0])
    }
    logger.info("tabletIdSet:${tabletIdSet}")

    // drop table
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""

    int retry = 20
    boolean success = false
    // recycle data
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(10000)
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
