suite("test_group_commit_partial_update") {
    def tableName = "test_group_commit_partial_update"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${tableName}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def checkStreamLoadResult = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("success", json.Status.toLowerCase())
        assertTrue(json.GroupCommit)
        assertTrue(json.Label.startsWith("group_commit_"))
        assertEquals(total_rows, json.NumberTotalRows)
        assertEquals(loaded_rows, json.NumberLoadedRows)
        assertEquals(filtered_rows, json.NumberFilteredRows)
        assertEquals(unselected_rows, json.NumberUnselectedRows)
        if (filtered_rows > 0) {
            assertFalse(json.ErrorURL.isEmpty())
        } else {
            assertTrue(json.ErrorURL == null || json.ErrorURL.isEmpty())
        }
    }

    try {
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(1100) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            set 'columns', 'id, name, score'
            file "initial_data.csv"
            unset 'label'

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                checkStreamLoadResult(exception, result, 2, 2, 0, 0)
            }
        }

        streamLoad {
            table "${tableName}"

            set 'column_separator', ','
            set 'group_commit', 'async_mode'
            set 'partial_columns', 'true'
            set 'columns', 'id, score'
            file "partial_update.csv"
            unset 'label'

            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                checkStreamLoadResult(exception, result, 2, 2, 0, 0)
            }
        }

        // 验证部分更新失败后数据未变
        qt_sql """select * from ${tableName} order by id;"""

    } finally {
        // 测试结束后删除表
        sql """ DROP TABLE IF EXISTS ${tableName} """
    }
}
