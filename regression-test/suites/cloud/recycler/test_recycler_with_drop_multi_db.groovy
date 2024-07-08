import groovy.json.JsonOutput
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_recycler_with_drop_multi_db") {
    // create table
    def token = "greedisgood9999"
    def instanceId = context.config.instanceId;
    def cloudUniqueId = context.config.cloudUniqueId

    def dbNames = ["regression_test_cloud_test_recycler_with_drop_multi_db1", "regression_test_cloud_test_recycler_with_drop_multi_db2"
            , "regression_test_cloud_test_recycler_with_drop_multi_db3", "regression_test_cloud_test_recycler_with_drop_multi_db4"
            , "regression_test_cloud_test_recycler_with_drop_multi_db5", "regression_test_cloud_test_recycler_with_drop_multi_db6"]

    def dropDbNames = ["regression_test_cloud_test_recycler_with_drop_multi_db1", "regression_test_cloud_test_recycler_with_drop_multi_db2"]

    def tableName = 'test_recycler_with_drop_multi_db'

    for (String dbName : dbNames) {
        sql """ DROP DATABASE IF EXISTS ${dbName} FORCE"""
        sql """ create database ${dbName}"""
        sql """ use ${dbName}"""
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
        streamLoad {
            db dbName
            table tableName
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
        qt_sql """ select count(*) from ${tableName} """
    }

    HashMap<String, HashSet<String>> dbToTabletSets = new HashMap<>();
    for (String dbName : dbNames) {
        sql """ use ${dbName}"""
        String[][] tabletInfoList = sql """ show tablets from ${tableName}; """
        logger.debug("tabletInfoList:${tabletInfoList}")
        HashSet<String> tabletIdSet= new HashSet<String>()
        for (tabletInfo : tabletInfoList) {
            tabletIdSet.add(tabletInfo[0])
        }
        dbToTabletSets[dbName] = tabletIdSet;
        logger.info("dbName:${dbName}, tabletIdSet:${tabletIdSet}")
    }
    logger.info("dbToTabletSets:${dbToTabletSets}")

    for (String dbName : dropDbNames) {
         sql """ DROP DATABASE IF EXISTS ${dbName} FORCE;"""
    }

    for (String dbName : dbNames) {
        if (dropDbNames.contains(dbName)) {
            continue;
        }
        sql """ use ${dbName}"""
        qt_sql """ select count(*) from ${tableName} """
    }

    for (String dbName : dropDbNames) {
        int retry = 15
        boolean success = false
        do {
            triggerRecycle(token, instanceId)
            Thread.sleep(20000)  // 20s
            if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, dbToTabletSets[dbName])) {
                success = true
                break
            }
        } while (retry--)
        assertTrue(success)
    }

    for (String dbName : dbNames) {
        if (dropDbNames.contains(dbName)) {
            continue;
        }
        sql """ use ${dbName}"""
        qt_sql """ select count(*) from ${tableName} """
        sql """ DROP DATABASE IF EXISTS ${dbName} FORCE;"""
    }

    for (String dbName : dbNames) {
        if (dropDbNames.contains(dbName)) {
            continue;
        }
        int retry = 15
        boolean success = false
        do {
            triggerRecycle(token, instanceId)
            Thread.sleep(20000)  // 20s
            if (checkRecycleTable(token, instanceId, cloudUniqueId, tableName, dbToTabletSets[dbName])) {
                success = true
                break
            }
        } while (retry--)
        assertTrue(success)
    }
}

