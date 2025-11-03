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

suite("show") {
    def dbs = [
            'regression_test_catalog_recycle_bin_db0',
            'regression_test_catalog_recycle_bin_db1',
            'regression_test_catalog_recycle_bin_db2'
    ]
    def dbIds = []

    for (def db : dbs) {
        sql """ drop database if exists $db """
        sql """ create database $db """
        dbIds.add(getDbId(db) + '')
    }

    def tables = [
            [dbs[1], 'show0'],
            [dbs[1], 'show1'],
            [dbs[2], 'show2'],
            [dbs[2], 'show3'],
            [dbs[2], 'show4']
    ]
    def tableIds = []

    for (def entry in tables) {
        def dbName = entry[0]
        def tableName = entry[1]
        sql """
            CREATE TABLE IF NOT EXISTS $dbName.$tableName (
                `k1` int(11) NULL,
                `k2` tinyint(4) NULL,
                `k3` smallint(6) NULL,
                `k4` bigint(20) NULL,
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
            PARTITION BY RANGE(k1)
            (
                PARTITION p2000 VALUES LESS THAN ('2000'),
                PARTITION p5000 VALUES LESS THAN ('5000')
            )
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """
        tableIds.add(getTableId(dbName, tableName) + '')

        streamLoad {
            db "${dbName}"
            table "${tableName}"

            set 'column_separator', ','
            set 'compress_type', 'gz'

            file 'all_types.csv.gz'
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

    logger.info("dbIds: " + dbIds)
    logger.info("tableIds: " + tableIds)

    sql """ drop database ${dbs[0]} """
    sql """ drop database ${dbs[1]} """
    sql """ drop table ${tables[2][0]}.${tables[2][1]} """
    sql """ alter table ${tables[3][0]}.${tables[3][1]} drop partition p2000 """

    def checkShowResults = () -> {
        def results = sql_return_maparray """ show catalog recycle bin """
        // logger.info("show catalog recycle bin result: " + results)
        def recycleBinSize = 0
        for (final def result in results) {
            logger.info("result: " + result)
            if (result.DbId == dbIds[0] && result.TableId == '') { // db0 is dropped
                assertEquals(result['DataSize'], '0.000 ')
                assertEquals(result['RemoteDataSize'], '0.000 ')
                recycleBinSize++
            } else if (result.DbId == dbIds[1] && result.TableId == '') { // db1 is dropped
                // assertTrue(result['DataSize'].startsWith('44') && result['DataSize'].contains('KB'))
                assertEquals(result['RemoteDataSize'], '0.000 ')
                recycleBinSize++
            } else if (result.TableId == tableIds[2]) { // table2 is dropped
                assertEquals(result['PartitionId'], '')
                // assertTrue(result['DataSize'].startsWith('22') && result['DataSize'].contains('KB'))
                assertEquals(result['RemoteDataSize'], '0.000 ')
                recycleBinSize++
            } else if (result.TableId == tableIds[3]) { // the partition of table3 is dropped
                assertFalse(result['PartitionId'].isEmpty())
                // assertTrue(result['DataSize'].startsWith('12') && result['DataSize'].contains('KB'))
                assertEquals(result['RemoteDataSize'], '0.000 ')
                recycleBinSize++
            }
        }
        assertEquals(4, recycleBinSize)
    }

    for (def i = 0; i < 10; ++i) {
        try {
            logger.info("round " + i)
            checkShowResults()
            break
        } catch (Throwable e) {
            if (i == 9) {
                throw e
            } else {
                // wait for the data size report
                sleep(10000)
            }
        }
    }
}
