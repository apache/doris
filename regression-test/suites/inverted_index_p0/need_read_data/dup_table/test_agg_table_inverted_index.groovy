import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

suite("test_agg_table_inverted_index") {
    
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

    def generate_agg_sql = { tableName ->
        List<String> list = new ArrayList<>()

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_PHRASE_PREFIX
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_REGEXP
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_PHRASE_EDGE
        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL ORDER BY  `@timestamp` LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL ORDER BY  `@timestamp` LIMIT 2");

        // BKD
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus = 200");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus = 200 ORDER BY  `@timestamp` LIMIT 2");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus != 200");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus != 200 ORDER BY  `@timestamp` LIMIT 2");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus < 800");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus < 800 ORDER BY  `@timestamp` LIMIT 2");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus <= 800");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus <= 800 ORDER BY  `@timestamp` LIMIT 2");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus > 200");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus > 200 ORDER BY  `@timestamp` LIMIT 2");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus >= 200");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus >= 200 ORDER BY  `@timestamp` LIMIT 2");

        // IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus IN (200, 1000, 1500)");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus IN (200, 1000, 1500) ORDER BY  `@timestamp` LIMIT 2");

        // NOT IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus NOT IN (800, 1000, 1500)");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus NOT IN (800, 1000, 1500) ORDER BY  `@timestamp` LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus IS NULL");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus IS NULL ORDER BY  `@timestamp` LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE staus IS NOT NULL");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE staus IS NOT NULL ORDER BY  `@timestamp` LIMIT 2");

        // STRING
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip = '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip != '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip != '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip < '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip < '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip <= '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip <= '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip > '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip > '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip >= '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip >= '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2')");
        list.add("SELECT request FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') ORDER BY  `@timestamp` LIMIT 2");

        // NOT IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2')");
        list.add("SELECT request FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_ANY
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' ORDER BY  `@timestamp` LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NULL");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NULL ORDER BY  `@timestamp` LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NOT NULL");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NOT NULL ORDER BY  `@timestamp` LIMIT 2");

        // FULLTEXT MATCH_ANY with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request = 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request != 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request < 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request <= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request > 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request >= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // FULLTEXT MATCH_ALL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request LIKE '%GET%'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request = 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request != 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request < 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request <= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request > 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request >= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // FULLTEXT MATCH_PHRASE with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request LIKE '%GET%'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request = 'GET'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request != 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request < 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request <= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request > 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request >= 'POST'");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // FULLTEXT MATCH_PHRASE_PREFIX with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request LIKE '%GET%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request = 'GET';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request != 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request < 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request <= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request > 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request >= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        // FULLTEXT MATCH_REGEXP with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request LIKE '%GET%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request = 'GET';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request != 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request < 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request <= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request > 'POST' ") 
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2 ") 

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request >= 'POST' ") 
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2 ") 

        // FULLTEXT MATCH_PHRASE_EDGE with others
        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request LIKE '%GET%' ");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request IN ('GET', 'POST', 'PUT')");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request = 'GET'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request != 'POST'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request < 'POST'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request <= 'POST'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request > 'POST'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request >= 'POST'");
        // list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2");

        // FULLTEXT IS NULL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request LIKE '%GET%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request = 'GET';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request != 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request < 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request <= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request > 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request >= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NULL AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        // FULLTEXT IS NOT NULL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request LIKE '%GET%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request LIKE '%GET%' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request IN ('GET', 'POST', 'PUT') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request = 'GET';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request = 'GET' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request != 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request != 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request < 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request < 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request <= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request <= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request > 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request > 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request >= 'POST';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE request IS NOT NULL AND request >= 'POST' ORDER BY  `@timestamp` LIMIT 2;");

        // BKD with others
        // =
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status = 200 AND status LIKE '%200%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status = 200 AND status LIKE '%200%' ORDER BY  `@timestamp` LIMIT 2;");

        // !=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status != 800 AND status LIKE '%0%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status != 800 AND status LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // <
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status < 800 AND status LIKE '%0%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status < 800 AND status LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // <=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status <= 800 AND status LIKE '%0%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status <= 800 AND status LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // >
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status > 800 AND status LIKE '%985%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status > 800 AND status LIKE '%985%' ORDER BY  `@timestamp` LIMIT 2;");

        // >=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status >= 800 AND status LIKE '%985%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status >= 800 AND status LIKE '%985%' ORDER BY  `@timestamp` LIMIT 2;");

        // IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status IN (800, 1000, 1500) AND status LIKE '%800%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status IN (800, 1000, 1500) AND status LIKE '%800%' ORDER BY  `@timestamp` LIMIT 2;");

        // NOT IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status NOT IN (800, 1000, 1500) AND status LIKE '%985%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status NOT IN (800, 1000, 1500) AND status LIKE '%985%' ORDER BY  `@timestamp` LIMIT 2;");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status IS NULL AND status LIKE '%800%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status IS NULL AND status LIKE '%800%' ORDER BY  `@timestamp` LIMIT 2;");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE status IS NOT NULL AND status LIKE '%800%';");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status IS NOT NULL AND status LIKE '%800%' ORDER BY  `@timestamp` LIMIT 2;");

        // STRING
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip = '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip != '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip != '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip < '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip < '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip <= '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip <= '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip > '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip > '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip >= '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip >= '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // NOT IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // MATCH_ANY
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NULL AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NULL AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NOT NULL AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NOT NULL AND clientip LIKE '%0%' ORDER BY  `@timestamp` LIMIT 2;");

        // ------------------
        // PROJECT
        list.add("SELECT clientip FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE status = 200 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE size = 800 ORDER BY  `@timestamp` LIMIT 2;");

        // ORDER BY
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY clientip,  `@timestamp` LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE status != 200 ORDER BY status,  `@timestamp` LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE size > 200 ORDER BY size,  `@timestamp` LIMIT 2;");

        // GROUP BY
        list.add("SELECT clientip, sum(status) FROM ${tableName} WHERE clientip MATCH_ANY '17' group by clientip ORDER BY clientip LIMIT 2;");
        list.add("SELECT status, sum(status) FROM ${tableName} WHERE status != 200 group by status ORDER BY status LIMIT 2;");
        list.add("SELECT status, sum(status) FROM ${tableName} WHERE status > 200 group by status ORDER BY status LIMIT 2;");

        // FUNCTION
        list.add("SELECT clientip FROM ${tableName} WHERE size > 0 AND size > status ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE status > 0 AND mod(status, 2) = 0 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE status > 0 AND (size + status) > 1000 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT max_by(size, status) FROM ${tableName} WHERE status > 0 GROUP BY  `@timestamp` ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT sum(size) FROM ${tableName} WHERE status > 0 and  `@timestamp` > 10;");
        list.add("SELECT CASE status WHEN 200 THEN 'TRUE' ELSE 'FALSE' END AS test_case FROM ${tableName} WHERE status > 0 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT IF(size = 0, 'true', 'false') test_if from ${tableName} WHERE status = 200 AND IF(size = 0, 'true', 'false') = 'false' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT IFNULL(size, 0) from ${tableName} WHERE status = 200 AND IFNULL(size, 0) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT IPV4_STRING_TO_NUM(clientip) from ${tableName} WHERE status = 200 AND IPV4_STRING_TO_NUM(clientip) > 33619968 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT to_base64(request) from ${tableName} WHERE status = 200 AND IPV4_STRING_TO_NUM(clientip) > 33619968 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` from ${tableName} WHERE status = 200 AND length(request) > 30 ORDER BY  `@timestamp` LIMIT 2;");

        // Ugly parameters
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status = NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status != NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status <= NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status >= NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status > NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status < NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status IN (NULL) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status NOT IN (NULL) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status = NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status != NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status <= NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status >= NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status > NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status < NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status IN (NULL) OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE status NOT IN (NULL) OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip = NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip != NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip <= NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip >= NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip > NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip < NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip IN (NULL) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip NOT IN (NULL) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip = NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip != NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip <= NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip >= NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip > NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip < NULL OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip IN (NULL, '') OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip NOT IN (NULL, '') OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip = '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip != '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip <= '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip >= '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip > '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip < '' ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip IN ('') ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip NOT IN ('') ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip = '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip != '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip <= '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip >= '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip > '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip < '' OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT  `@timestamp` FROM ${tableName} WHERE clientip IN ('', '') OR size > 10 ORDER BY  `@timestamp` LIMIT 2;");
        // list.add("SELECT clientip from dup_httplogs WHERE clientip NOT IN (NULL, '') or clientip IN ('') ORDER BY  `@timestamp` LIMIT 2")
    
        // PK
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' AND  `@timestamp` > 20 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE size > 100 AND  `@timestamp` > 20 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND  `@timestamp` > 20 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' OR size > 100 AND  `@timestamp` > 20 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 OR  `@timestamp` > 20 ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 AND  `@timestamp` > NULL ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 OR NOT ( `@timestamp` > NULL) ORDER BY  `@timestamp` LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 AND NOT ( `@timestamp` > NULL) ORDER BY  `@timestamp` LIMIT 2;");
        return list
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
        GetDebugPoint().enableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
        def aggTableName = "agg_httplogs"
        sql """ drop table if exists ${aggTableName} """
        // create table
        sql """
            CREATE TABLE IF NOT EXISTS agg_httplogs
            (   
                `@timestamp`  int(11) NULL,
                `clientip`   varchar(20) NULL,
                `request`    varchar(1000) NULL,
                `status`     int(11) NULL,
                `size`       int(11) SUM DEFAULT "0",
                INDEX        clientip_idx (`clientip`) USING INVERTED COMMENT '',
                INDEX        request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
                INDEX        status_idx (`status`) USING INVERTED COMMENT ''
            ) AGGREGATE KEY(`@timestamp`, `clientip`, `request`, `status`)
            DISTRIBUTED BY HASH (`@timestamp`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compaction_policy" = "time_series",
            "inverted_index_storage_format" = "v2",
            "compression" = "ZSTD"
            );
        """

        load_data.call(aggTableName, 'documents-1000.json');
        // load_data.call(aggTableName, 'documents-191998.json');
        // load_data.call(aggTableName, 'documents-201998.json');
        // load_data.call(aggTableName, 'documents-231998.json');
        // load_data.call(aggTableName, 'documents-221998.json');
        sql """ INSERT INTO ${aggTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${aggTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, NULL, 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${aggTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', NULL, 500, 1000) """
        sql """ INSERT INTO ${aggTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', NULL, 1000) """
        sql """ INSERT INTO ${aggTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, NULL) """
        sql """ sync """
    
        def all_agg_sql = generate_agg_sql.call(aggTableName)
        def agg_result1 = execute_sql.call("enable_no_need_read_data_opt", "true", all_agg_sql)
        logger.info("agg_result1 is {}", agg_result1);
        def agg_result2 = execute_sql.call("enable_no_need_read_data_opt", "false", all_agg_sql)
        logger.info("agg_result2 is {}", agg_result2);
        compare_result(agg_result1, agg_result2, all_agg_sql)

        // delete
        sql """ delete from dup_httplogs where clientip = '40.135.0.0'; """
        sql """ delete from dup_httplogs where status = 304; """
        sql """ delete from dup_httplogs where size = 24736; """
        sql """ delete from dup_httplogs where request = 'GET /images/hm_bg.jpg HTTP/1.0'; """

        def agg_result3 = execute_sql.call("enable_no_need_read_data_opt", "true", all_agg_sql)
        logger.info("agg_result3 is {}", agg_result3);
        def agg_result4 = execute_sql.call("enable_no_need_read_data_opt", "false", all_agg_sql)
        logger.info("agg_result4 is {}", agg_result4);
        compare_result(agg_result3, agg_result4, all_agg_sql)

        run_compaction.call(aggTableName)
        def agg_result5 = execute_sql.call("enable_no_need_read_data_opt", "true", all_agg_sql)
        logger.info("agg_result5 is {}", agg_result5);
        def agg_result6 = execute_sql.call("enable_no_need_read_data_opt", "false", all_agg_sql)
        logger.info("agg_result6 is {}", agg_result6);
        compare_result(agg_result5, agg_result6, all_agg_sql)
        compare_result(agg_result3, agg_result6, all_agg_sql)
        
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
    }
    

    
}