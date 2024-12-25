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

suite("test_schema_change") {

    // create table
    def tableName = 'test_schema_change'
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
            C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
            C_MKTSEGMENT  CHAR(10) NOT NULL,
            C_COMMENT     VARCHAR(117) NOT NULL
        )
        DUPLICATE KEY(C_CUSTKEY, C_NAME)
        DISTRIBUTED BY HASH(C_CUSTKEY) BUCKETS 32
        PROPERTIES (
            "replication_num" = "1"
        );
    """

    sql """
        LOAD LABEL ${loadLabel}
        (
            DATA INFILE('s3://${s3BucketName}/regression/tpch/sf100/customer.tbl')
            INTO TABLE ${tableName}
            COLUMNS TERMINATED BY "|"
            (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, temp)
        )
        WITH S3
        (
            'AWS_REGION' = '${getS3Region()}',
            'AWS_ENDPOINT' = '${getS3Endpoint()}',
            'AWS_ACCESS_KEY' = '${getS3AK()}',
            'AWS_SECRET_KEY' = '${getS3SK()}'
        )
        PROPERTIES
        (
            'exec_mem_limit' = '8589934592',
            'load_parallelism' = '1',
            'timeout' = '3600'
        )
    """

    def waitBrokerLoadJob = { String label /* param */ ->
        // check load state
        int tryTimes = 20
        while (tryTimes-- > 0) {
            def stateResult = sql "show load where Label = '${label}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ('cancelled'.equalsIgnoreCase(loadState)) {
                logger.info("stateResult:{}", stateResult)
                throw new IllegalStateException("load ${label} has been cancelled")
            } else if ('finished'.equalsIgnoreCase(loadState)) {
                logger.info("stateResult:{}", stateResult)
                break
            }
            sleep(60000)
        }
    }

    waitBrokerLoadJob(loadLabel)
    sql "sync"
    def rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 15000000)

    sql """ alter table ${tableName} drop column C_NAME"""

    def waitSchemaChangeJob = { String tbName /* param */ ->
        int tryTimes = 20
        while (tryTimes-- > 0) {
            def jobResult = sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tbName}' ORDER BY createtime DESC LIMIT 1 """
            def jobState = jobResult[0][9].toString()
            if ('cancelled'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                throw new IllegalStateException("${tbName}'s job has been cancelled")
            }
            if ('finished'.equalsIgnoreCase(jobState)) {
                logger.info("jobResult:{}", jobResult)
                break
            }
            sleep(60000)
        }
    }

    waitSchemaChangeJob(tableName)
    sql """ DESC ${tableName}"""
    rowCount = sql "select count(*) from ${tableName}"
    logger.info("rowCount:{}", rowCount)
    assertEquals(rowCount[0][0], 15000000)
    qt_sql "select * from ${tableName} order by C_CUSTKEY limit 4097;"
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
}
