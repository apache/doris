import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

suite("test_in_null_no_return", "nonConcurrent") {
    
    // load data
    def load_data = { loadTableName, fileName ->
        streamLoad {
            table loadTableName
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            file fileName
            time 10000

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
    }

    def execute_sql = { key, value, sqlList ->
        sql """ set ${key} = ${value} """
        List<Object> resultList = new ArrayList<>()
        for (sqlStr in sqlList) {
            def sqlResult = sql """ ${sqlStr} """
            resultList.add(sqlResult)
        }
        return resultList
    }

    def compare_result = { result1, result2, executedSql ->
        assertEquals(result1.size(), result2.size())
        for (int i = 0; i < result1.size(); i++) {
            if (result1[i] != result2[i]) {
                logger.info("sql is {}", executedSql[i])
                assertTrue(False)
            }
        }
    }

    def run_compaction = { compactionTableName ->
        String backend_id;

        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        def tablets = sql_return_maparray """ show tablets from ${compactionTableName}; """
        
        // run
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            times = 1

            do{
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
            } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

            def compactJson = parseJson(out.trim())
            if (compactJson.status.toLowerCase() == "fail") {
                logger.info("Compaction was done automatically!")
            }
        }

        // wait
        for (def tablet : tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                def tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }
    
    
    
    try {
        // GetDebugPoint().enableDebugPointForAllBEs("match.invert_index_not_support_execute_match")

        def dupTableName = "dup_httplogs"
        sql """ drop table if exists ${dupTableName} """
        // create table
        sql """
            CREATE TABLE IF NOT EXISTS dup_httplogs
            (   
                `id`          bigint NOT NULL AUTO_INCREMENT(100),
                `@timestamp` int(11) NULL,
                `clientip`   varchar(20) NULL,
                `request`    text NULL,
                `status`     int(11) NULL,
                `size`       int(11) NULL,
                INDEX        clientip_idx (`clientip`) USING INVERTED COMMENT '',
                INDEX        request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
                INDEX        status_idx (`status`) USING INVERTED COMMENT '',
                INDEX        size_idx (`size`) USING INVERTED COMMENT ''
            ) DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH (`id`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compaction_policy" = "time_series",
            "inverted_index_storage_format" = "v2",
            "compression" = "ZSTD",
            "disable_auto_compaction" = "true"
            );
        """

        load_data.call(dupTableName, 'documents-1000.json');
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, NULL, 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', NULL, 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', NULL, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, NULL) """
        sql """ sync """

        sql """ SELECT count() from dup_httplogs WHERE clientip IN (NULL, '') or clientip IN ('') LIMIT 2 """
        sql """ SELECT count() from dup_httplogs WHERE clientip IN (NULL, '17.0.0.0') or clientip IN ('') LIMIT 2 """
        sql """ SELECT count() from dup_httplogs WHERE request IN (NULL, '') or clientip IN ('') LIMIT 2 """
        sql """ SELECT count() from dup_httplogs WHERE request IN (NULL, '17.0.0.0') or request IN ('') LIMIT 2 """
    } finally {
        // GetDebugPoint().disableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
    }
}