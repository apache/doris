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
import java.util.stream.Collectors

suite("test_recycler_with_drop_mv") {
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId
    def tableName = 'test_recycler_with_drop_mv'
    def mvName = "test_recycler_with_drop_mv_name"
    def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
    def loadLabel = tableName + "_" + uniqueID

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql """
        CREATE TABLE IF NOT EXISTS `${tableName}` (
            C_CUSTKEY     INTEGER NOT NULL,
            C_NAME        VARCHAR(25) NOT NULL,
            C_ADDRESS     VARCHAR(40) NOT NULL,
            C_NATIONKEY   INTEGER NOT NULL,
            C_PHONE       CHAR(15) NOT NULL,
            C_ACCTBAL     DECIMALV3(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
        ;
    """

    sql """
        LOAD LABEL ${loadLabel}
        (
            DATA INFILE('s3://${s3BucketName}/regression/tpch/sf1/customer.tbl')
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
        )
        WITH S3
        (
            'AWS_REGION' = '${getS3Region()}',
            'AWS_ENDPOINT' = '${getS3Endpoint()}',
            'AWS_ACCESS_KEY' = '${getS3AK()}',
            'AWS_SECRET_KEY' = '${getS3SK()}',
            'PROVIDER' = '${getS3Provider()}'
        )
        PROPERTIES
        (
            'exec_mem_limit' = '8589934592',
            'load_parallelism' = '1',
            'timeout' = '3600'
        )
    """

    checkBrokerLoadFinished(loadLabel)
    def rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 150000)

    String[][] tabletInfoList1 = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList1:${tabletInfoList1}")

    HashSet<String> tabletIdSet1 = tabletInfoList1.stream().map(tabletInfo -> tabletInfo[0]).collect(Collectors.toSet());
    logger.info("tabletIdSet1:${tabletIdSet1}")
    assertTrue(tabletIdSet1.size() > 0)

    sql "create materialized view ${mvName} as select C_CUSTKEY as C_CUSTKEY_T, C_ADDRESS as C_ADDRESS_T from ${tableName};"
    waitMvJobFinished(tableName)

    String[][] tabletInfoList2 = sql """ show tablets from ${tableName}; """
    logger.debug("tabletInfoList2:${tabletInfoList2}")
    HashSet<String> tabletIdSet2 = tabletInfoList2.stream().map(tabletInfo -> tabletInfo[0]).collect(Collectors.toSet());
    logger.info("tabletIdSet2:${tabletIdSet2}")
    assertTrue(tabletIdSet2.size() > tabletIdSet1.size())

    HashSet<String> tabletIdSet3 = tabletIdSet2.stream().filter(tabletId -> !tabletIdSet1.contains(tabletId)).collect(Collectors.toSet());
    logger.info("tabletIdSet3:${tabletIdSet3}")
    assertTrue(tabletIdSet3.size() > 0)
    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 150000)

    sql "drop materialized view ${mvName} on ${tableName};"
    int retry = 15
    boolean success = false
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000)
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet3)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)

    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 150000)

    sql "drop materialized view if exists ${mvName} on ${tableName};"
    sql """ drop table if exists ${tableName} force """
    // trigger recycle and check data has been deleted
    retry = 15
    success = false
    do {
        triggerRecycle(token, instanceId)
        Thread.sleep(20000)
        if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, tabletIdSet1)) {
            success = true
            break
        }
    } while (retry--)
    assertTrue(success)
}
