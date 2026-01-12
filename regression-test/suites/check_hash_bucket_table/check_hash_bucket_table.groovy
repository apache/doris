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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

suite("check_hash_bucket_table") {

    AtomicInteger dbNum = new AtomicInteger(0)
    AtomicInteger tableNum = new AtomicInteger(0)
    AtomicInteger partitionNum = new AtomicInteger(0)
    def executor = Executors.newFixedThreadPool(30)
    def futures = []

    def excludedDbs = ["mysql", "information_schema", "__internal_schema"].toSet()

    logger.info("===== [check] begin to check hash bucket tables")
    def checkPartition = { String db, String tblName, def info ->
        int bucketNum = info["Buckets"].toInteger()
        if (bucketNum <= 1) { return false}

        def bucketColumns = info["DistributionKey"]
        if (bucketColumns == "RANDOM") {return false}
        def columnsDetail = sql_return_maparray "desc ${tblName} all;"
        def bucketCols = bucketColumns.split(",").collect { it.trim() }
        def bucketColsStr = bucketCols.collect { "`${it}`" }.join(",")
        def partitionName = info["PartitionName"]
        try {
            def tabletIdList = sql_return_maparray(""" show replica status from ${tblName} partition(${partitionName}); """).collect { it.TabletId }.toList()
            def tabletIds = tabletIdList.toSet()
            int replicaNum = tabletIdList.stream().filter { it == tabletIdList[0] }.count()
            logger.info("""===== [check] Begin to check partition: ${db}.${tblName}, partition name: ${partitionName}, bucket num: ${bucketNum}, replica num: ${replicaNum}, bucket columns: ${bucketColsStr}""")
            (0..replicaNum-1).each { replica ->
                sql "set use_fix_replica=${replica};"
                tabletIds.each { it2 ->
                    def tabletId = it2
                    try {
                        def res = sql "select crc32_internal(${bucketColsStr}) % ${bucketNum} from ${db}.${tblName} tablet(${tabletId}) group by crc32_internal(${bucketColsStr}) % ${bucketNum};"
                        if (res.size() > 1) {
                            logger.info("""===== [check] check failed: ${db}.${tblName}, partition name: ${partitionName}, tabletId: ${tabletId}, bucket columns: ${bucketColsStr}, res.size()=${res.size()}, res=${res}""")
                            assert res.size() == 1
                        }
                    } catch (AssertionError e) {
                        throw e
                    } catch (Throwable e) {
                        logger.info("===== [check] catch exception, table: ${db}.${tblName}, partition name: ${partitionName}, tabletId: ${tabletId}, e=${e}")
                    }
                }
                sql "set use_fix_replica=-1;"
            }
            logger.info("""===== [check] Finish to check table partition: ${db}.${tblName}, partitionName: ${partitionName}, replica num: ${replicaNum}, bucket num: ${bucketNum}, bucket columns: ${bucketColsStr}""")
        } catch (AssertionError e) {
            throw e
        } catch (Throwable e) {
            logger.info("===== [check] catch exception, table: ${db}.${tblName}, partition name: ${partitionName}, e=${e}")
        }
        return true
    }

    def checkTable = { String db, String tblName ->
        sql "use ${db};"
        def showStmt = sql_return_maparray("show create table ${tblName}")[0]["Create Table"]
        def partitionInfo = sql_return_maparray """ show partitions from ${tblName}; """
        int checkedPartition = 0
        partitionInfo.each {
            if (checkPartition(db, tblName, it)) {
                ++checkedPartition
            }
        }
        logger.info("""===== [check] Finish to check table: ${db}.${tblName}""")
        partitionNum.addAndGet(checkedPartition)
        return checkedPartition > 0
    }

    def checkDb = { String db ->
        sql "use ${db};"
        dbNum.incrementAndGet()
        def tables = sql("show full tables").stream().filter{ it[1] == "BASE TABLE" }.collect{ it[0] }.toList()
        def asyncMVs = sql_return_maparray("""select * from mv_infos("database"="${db}");""").collect{ it.Name }.toSet()
        tables.each {
            def tblName = it
            if (!asyncMVs.contains(tblName)) {
                futures << executor.submit({
                    if (checkTable(db, tblName)) {
                        tableNum.incrementAndGet()
                    }
                })
            }
        }
    }

    def allDbs = sql "show databases"
    allDbs.each {
        def db = it[0]
        if (!excludedDbs.contains(db)) {
            checkDb(db)
        }
    }
    futures.each { it.get() }
    executor.shutdown()
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES)
    logger.info("===== [check] finish to check hash bucket tables, db num: ${dbNum}, table num: ${tableNum}, partition num: ${partitionNum}")
}
