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

suite("check_hash_bucket_table") {

    int dbNum = 0
    int tableNum = 0
    def excludedDbs = ["mysql", "information_schema", "__internal_schema"].toSet()

    logger.info("===== [check] begin to check hash bucket tables")

    def checkTable = { String db, String tblName ->
        sql "use ${db};"
        def showStmt = sql_return_maparray("show create table ${tblName}")[0]["Create Table"]
        boolean hashBucket = showStmt.toUpperCase().contains("DISTRIBUTED BY HASH")
        def matcher = showStmt =~ /(?i)DISTRIBUTED BY HASH\s*\((.*?)\)/
        if (!matcher.find()) { return } 
        def bucketColumns = matcher.group(1)
        def tabletStats = sql_return_maparray """ show tablets from ${tblName}; """
        def tabletIdList = tabletStats.collect { it.TabletId }.toList()
        def tabletIds = tabletIdList.toSet()

        def matcher2 = showStmt =~ /(?i)BUCKETS\s+(\d+)/
        if (!matcher2.find()) { return } 
        int bucketNum = matcher2.group(1).toInteger()
        if (bucketNum <= 1) { return }

        def bucketCols = bucketColumns.findAll(/`([^`]+)`/)*.replaceAll(/`/, '')
        def columnsDetail = sql_return_maparray "desc ${tblName} all;"
        boolean skip = false
        for (def col: bucketCols) {
            def colDetail = columnsDetail.find { it.Field == col }
            if (colDetail.InternalType.toLowerCase().contains("decimal")) {
                logger.info("===== [check] skip to check table: ${db}.${tblName} because bucket column: ${col} is ${colDetail.InternalType}")
                return
            }
        }

        logger.info("""===== [check] Begin to check table: ${db}.${tblName}, hash bucket: ${hashBucket}, bucket num: ${bucketNum}, replica num: ${tabletStats.size()}, bucket columns: ${bucketColumns}""")
        ++tableNum
        int replicaNum = tabletIdList.stream().filter { it == tabletIdList[0] }.count()
        (0..replicaNum-1).each { replica ->
            sql "set use_fix_replica=${replica};"
            tabletStats.each { it2 ->
                def tabletId = it2.TabletId
                try {
                    def res = sql "select crc32_internal(${bucketColumns}) % ${bucketNum} from ${db}.${tblName} tablet(${tabletId}) group by crc32_internal(${bucketColumns}) % ${bucketNum};"
                    if (res.size() > 1) {
                        logger.info("===== [check] check failed, table: ${db}.${tblName}, tabletId: ${tabletId}, replica=${replica}, res.size()=${res.size()}, res=${res}")
                        assert res.size() == 1
                    }
                } catch (AssertionError e) {
                    throw e
                } catch (Throwable e) {
                    logger.info("===== [check] catch exception, table: ${db}.${tblName}, tabletId: ${tabletId}, replica=${replica}, e=${e}")
                }
            }
            sql "set use_fix_replica=-1;"
        }
        logger.info("""===== [check] Finish to check table: ${db}.${tblName}, hash bucket: ${hashBucket}, bucket num: ${bucketNum}, replica num: ${tabletStats.size()}, bucket columns: ${bucketColumns}""")
    }

    def checkDb = { String db ->
        sql "use ${db};"
        def tables = sql("show full tables").stream().filter{ it[1] == "BASE TABLE" }.collect{ it[0] }.toList()
        ++dbNum
        def asyncMVs = sql_return_maparray("""select * from mv_infos("database"="${db}");""").collect{ it.Name }.toSet()
        tables.each {
            if (!asyncMVs.contains(it)) { checkTable(db, it) }
        }
    }

    def allDbs = sql "show databases"
    allDbs.each {
        def db = it[0]
        if (!excludedDbs.contains(db)) {
            checkDb(db)
        }
    }
    logger.info("===== [check] finish to check hash bucket tables, db num: ${dbNum}, table num: ${tableNum}")
}
