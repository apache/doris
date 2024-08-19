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

suite("test_dup_table_inverted_index", "p1") {
    
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

    // def generate_dup_mow_sql = { tableName ->
    //     List<String> list = new ArrayList<>()
    //     // FULLTEXT
    //     list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%'");
    //     list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2");
    //     return list
    // }

    def generate_dup_mow_sql = { tableName ->
        List<String> list = new ArrayList<>()
        // FULLTEXT
        // MATCH_ANY
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' ORDER BY id LIMIT 2");

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' ORDER BY id LIMIT 2");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' ORDER BY id LIMIT 2");

        // MATCH_PHRASE_PREFIX
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' ORDER BY id LIMIT 2");

        // MATCH_REGEXP
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' ORDER BY id LIMIT 2");

        // MATCH_PHRASE_EDGE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' ORDER BY id LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL ORDER BY id LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL ORDER BY id LIMIT 2");

        // LIKE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request LIKE '%GET%'");
        list.add("SELECT id FROM ${tableName} WHERE request LIKE '%GET%' ORDER BY id LIMIT 2");

        // IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT id FROM ${tableName} WHERE request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2");

        // NOT IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT id FROM ${tableName} WHERE request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2");

        // =
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE  request = 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE  request = 'GET' ORDER BY id LIMIT 2");

        // !=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request != 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request != 'POST' ORDER BY id LIMIT 2");

        // <
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request < 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request < 'POST' ORDER BY id LIMIT 2");

        // <=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request <= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request <= 'POST' ORDER BY id LIMIT 2");
        
        // >
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request > 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request > 'POST' ORDER BY id LIMIT 2");

        // >=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request >= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request >= 'POST' ORDER BY id LIMIT 2");

        // BKD
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size = 800");
        list.add("SELECT id FROM ${tableName} WHERE size = 800 ORDER BY id LIMIT 2");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size != 800");
        list.add("SELECT id FROM ${tableName} WHERE size != 800 ORDER BY id LIMIT 2");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size < 800");
        list.add("SELECT id FROM ${tableName} WHERE size < 800 ORDER BY id LIMIT 2");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size <= 800");
        list.add("SELECT id FROM ${tableName} WHERE size <= 800 ORDER BY id LIMIT 2");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size > 800");
        list.add("SELECT id FROM ${tableName} WHERE size > 800 ORDER BY id LIMIT 2");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size >= 800");
        list.add("SELECT id FROM ${tableName} WHERE size >= 800 ORDER BY id LIMIT 2");

        // IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IN (800, 1000, 1500)");
        list.add("SELECT id FROM ${tableName} WHERE size IN (800, 1000, 1500) ORDER BY id LIMIT 2");

        // NOT IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size NOT IN (800, 1000, 1500)");
        list.add("SELECT id FROM ${tableName} WHERE size NOT IN (800, 1000, 1500) ORDER BY id LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IS NULL");
        list.add("SELECT id FROM ${tableName} WHERE size IS NULL ORDER BY id LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IS NOT NULL");
        list.add("SELECT id FROM ${tableName} WHERE size IS NOT NULL ORDER BY id LIMIT 2");

        // LIKE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size LIKE '%800%';");
        list.add("SELECT id FROM ${tableName} WHERE size LIKE '%800%' ORDER BY id LIMIT 2;");

        // STRING
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip = '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' ORDER BY id LIMIT 2");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip != '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip != '17.0.0.0' ORDER BY id LIMIT 2");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip < '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip < '17.0.0.0' ORDER BY id LIMIT 2");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip <= '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip <= '17.0.0.0' ORDER BY id LIMIT 2");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip > '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip > '17.0.0.0' ORDER BY id LIMIT 2");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip >= '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip >= '17.0.0.0' ORDER BY id LIMIT 2");

        // IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2')");
        list.add("SELECT request FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') ORDER BY id LIMIT 2");

        // NOT IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2')");
        list.add("SELECT request FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') ORDER BY id LIMIT 2");

        // MATCH_ANY
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' ORDER BY id LIMIT 2");

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' ORDER BY id LIMIT 2");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0'");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' ORDER BY id LIMIT 2");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NULL");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NULL ORDER BY id LIMIT 2");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NOT NULL");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NOT NULL ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // FULLTEXT MATCH_ANY with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request = 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request = 'GET' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request != 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request != 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request < 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request < 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request <= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request <= 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request > 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request > 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request >= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ANY 'GET' AND request >= 'POST' ORDER BY id LIMIT 2");

        // FULLTEXT MATCH_ALL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request LIKE '%GET%'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request = 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request = 'GET' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request != 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request != 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request < 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request < 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request <= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request <= 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request > 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request > 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request >= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_ALL 'GET' AND request >= 'POST' ORDER BY id LIMIT 2");

        // FULLTEXT MATCH_PHRASE with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request LIKE '%GET%'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request = 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request = 'GET' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request != 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request != 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request < 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request < 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request <= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request <= 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request > 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request > 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request >= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE 'GET' AND request >= 'POST' ORDER BY id LIMIT 2");

        // FULLTEXT MATCH_PHRASE_PREFIX with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request LIKE '%GET%';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request = 'GET';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request = 'GET' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request != 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request != 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request < 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request < 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request <= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request <= 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request > 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request > 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request >= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_PREFIX 'GET' AND request >= 'POST' ORDER BY id LIMIT 2;");

        // FULLTEXT MATCH_REGEXP with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request LIKE '%GET%';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request = 'GET';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request = 'GET' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request != 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request != 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request < 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request < 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request <= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request <= 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request > 'POST' ") 
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request > 'POST' ORDER BY id LIMIT 2 ") 

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request >= 'POST' ") 
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_REGEXP 'GET' AND request >= 'POST' ORDER BY id LIMIT 2 ") 

        // FULLTEXT MATCH_PHRASE_EDGE with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request LIKE '%GET%' ");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request LIKE '%GET%' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request IN ('GET', 'POST', 'PUT')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request NOT IN ('DELETE', 'OPTIONS')");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request = 'GET'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request = 'GET' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request != 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request != 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request < 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request < 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request <= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request <= 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request > 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request > 'POST' ORDER BY id LIMIT 2");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request >= 'POST'");
        list.add("SELECT id FROM ${tableName} WHERE request MATCH_PHRASE_EDGE 'GET' AND request >= 'POST' ORDER BY id LIMIT 2");

        // FULLTEXT IS NULL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request LIKE '%GET%';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request LIKE '%GET%' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request = 'GET';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request = 'GET' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request != 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request != 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request < 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request < 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request <= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request <= 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request > 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request > 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NULL AND request >= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NULL AND request >= 'POST' ORDER BY id LIMIT 2;");

        // FULLTEXT IS NOT NULL with others
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request LIKE '%GET%';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request LIKE '%GET%' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request IN ('GET', 'POST', 'PUT');");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request IN ('GET', 'POST', 'PUT') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request NOT IN ('DELETE', 'OPTIONS');");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request NOT IN ('DELETE', 'OPTIONS') ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request = 'GET';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request = 'GET' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request != 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request != 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request < 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request < 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request <= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request <= 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request > 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request > 'POST' ORDER BY id LIMIT 2;");

        list.add("SELECT COUNT(*) FROM ${tableName} WHERE request IS NOT NULL AND request >= 'POST';");
        list.add("SELECT id FROM ${tableName} WHERE request IS NOT NULL AND request >= 'POST' ORDER BY id LIMIT 2;");

        // BKD with others
        // =
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size = 800 AND size LIKE '%800%';");
        list.add("SELECT id FROM ${tableName} WHERE size = 800 AND size LIKE '%800%' ORDER BY id LIMIT 2;");

        // !=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size != 800 AND size LIKE '%0%';");
        list.add("SELECT id FROM ${tableName} WHERE size != 800 AND size LIKE '%0%' ORDER BY id LIMIT 2;");

        // <
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size < 800 AND size LIKE '%0%';");
        list.add("SELECT id FROM ${tableName} WHERE size < 800 AND size LIKE '%0%' ORDER BY id LIMIT 2;");

        // <=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size <= 800 AND size LIKE '%0%';");
        list.add("SELECT id FROM ${tableName} WHERE size <= 800 AND size LIKE '%0%' ORDER BY id LIMIT 2;");

        // >
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size > 800 AND size LIKE '%985%';");
        list.add("SELECT id FROM ${tableName} WHERE size > 800 AND size LIKE '%985%' ORDER BY id LIMIT 2;");

        // >=
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size >= 800 AND size LIKE '%985%';");
        list.add("SELECT id FROM ${tableName} WHERE size >= 800 AND size LIKE '%985%' ORDER BY id LIMIT 2;");

        // IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IN (800, 1000, 1500) AND size LIKE '%800%';");
        list.add("SELECT id FROM ${tableName} WHERE size IN (800, 1000, 1500) AND size LIKE '%800%' ORDER BY id LIMIT 2;");

        // NOT IN
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size NOT IN (800, 1000, 1500) AND size LIKE '%985%';");
        list.add("SELECT id FROM ${tableName} WHERE size NOT IN (800, 1000, 1500) AND size LIKE '%985%' ORDER BY id LIMIT 2;");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IS NULL AND size LIKE '%800%';");
        list.add("SELECT id FROM ${tableName} WHERE size IS NULL AND size LIKE '%800%' ORDER BY id LIMIT 2;");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE size IS NOT NULL AND size LIKE '%800%';");
        list.add("SELECT id FROM ${tableName} WHERE size IS NOT NULL AND size LIKE '%800%' ORDER BY id LIMIT 2;");

        // STRING
        // EQ
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip = '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // NE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip != '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip != '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // LT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip < '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip < '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // LE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip <= '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip <= '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // GT
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip > '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip > '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // GE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip >= '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip >= '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // NOT IN LIST
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip NOT IN ('17.0.0.0', '17.0.0.1', '17.0.0.2') AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // MATCH_ANY
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ANY '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // MATCH_ALL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_ALL '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // MATCH_PHRASE
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip MATCH_PHRASE '17.0.0.0' AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // IS NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NULL AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NULL AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // IS NOT NULL
        list.add("SELECT COUNT(*) FROM ${tableName} WHERE clientip IS NOT NULL AND clientip LIKE '%0%';");
        list.add("SELECT request FROM ${tableName} WHERE clientip IS NOT NULL AND clientip LIKE '%0%' ORDER BY id LIMIT 2;");

        // ------------------
        // PROJECT
        list.add("SELECT clientip FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY id LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY id LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE status = 200 ORDER BY id LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE size = 800 ORDER BY id LIMIT 2;");

        // ORDER BY
        list.add("SELECT id FROM ${tableName} WHERE clientip MATCH_ANY '17' ORDER BY clientip, id LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE status != 200 ORDER BY status, id LIMIT 2;");
        list.add("SELECT * FROM ${tableName} WHERE size > 200 ORDER BY size, id LIMIT 2;");

        // GROUP BY
        list.add("SELECT clientip, sum(size) FROM ${tableName} WHERE clientip MATCH_ANY '17' group by clientip ORDER BY clientip LIMIT 2;");
        list.add("SELECT status, sum(size) FROM ${tableName} WHERE status != 200 group by status ORDER BY status LIMIT 2;");
        list.add("SELECT size, sum(status) FROM ${tableName} WHERE size > 200 group by size ORDER BY size LIMIT 2;");

        // FUNCTION
        list.add("SELECT clientip FROM ${tableName} WHERE size > 0 AND size > status ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE size > 0 AND mod(size, 2) = 0 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE size > 0 AND (size + status) > 1000 ORDER BY id LIMIT 2;");
        list.add("SELECT max_by(size, status) FROM ${tableName} WHERE size > 0 GROUP BY id ORDER BY id LIMIT 2;");
        list.add("SELECT sum(size) FROM ${tableName} WHERE size > 0 and id > 10;");
        list.add("SELECT CASE status WHEN 200 THEN 'TRUE' ELSE 'FALSE' END AS test_case FROM ${tableName} WHERE size > 0 ORDER BY id LIMIT 2;");
        list.add("SELECT IF(size = 0, 'true', 'false') test_if from ${tableName} WHERE status = 200 AND IF(size = 0, 'true', 'false') = 'false' ORDER BY id LIMIT 2;");
        list.add("SELECT IFNULL(size, 0) from ${tableName} WHERE status = 200 AND IFNULL(size, 0) ORDER BY id LIMIT 2;");
        list.add("SELECT IPV4_STRING_TO_NUM(clientip) from ${tableName} WHERE status = 200 AND IPV4_STRING_TO_NUM(clientip) > 33619968 ORDER BY id LIMIT 2;");
        list.add("SELECT to_base64(request) from ${tableName} WHERE status = 200 AND IPV4_STRING_TO_NUM(clientip) > 33619968 ORDER BY id LIMIT 2;");
        list.add("SELECT id from ${tableName} WHERE status = 200 AND length(request) > 30 ORDER BY id LIMIT 2;");


        // Ugly parameters
        // Ugly parameters
        list.add("SELECT id FROM ${tableName} WHERE status = NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status != NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status <= NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status >= NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status > NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status < NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status IN (NULL) ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status NOT IN (NULL) ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status = NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status != NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status <= NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status >= NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status > NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status < NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status IN (NULL) OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE status NOT IN (NULL) OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip = NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip != NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip <= NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip >= NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip > NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip < NULL ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip IN (NULL) ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip NOT IN (NULL) ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip = NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip != NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip <= NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip >= NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip > NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip < NULL OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip IN (NULL, '') OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip NOT IN (NULL, '') OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip = '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip != '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip <= '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip >= '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip > '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip < '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip IN ('') ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip NOT IN ('') ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip = '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip != '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip <= '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip >= '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip > '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip < '' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip IN ('', '') OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip from dup_httplogs WHERE clientip NOT IN (NULL, '') or clientip IN ('') ORDER BY id LIMIT 2")
        list.add("SELECT id FROM ${tableName} WHERE clientip > '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfH' OR size > 10 ORDER BY id LIMIT 2;");

        list.add("SELECT id FROM ${tableName} WHERE request != '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfH' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE request = '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5' OR size > 10 ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip MATCH('?') ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip MATCH('*') ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE request = '--' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE request = '' ORDER BY id LIMIT 2;");
        list.add("SELECT id FROM ${tableName} WHERE clientip > '' ORDER BY id LIMIT 2;");

        // PK
        list.add("SELECT request FROM ${tableName} WHERE clientip = '17.0.0.0' AND id > 20 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE size > 100 AND id > 20 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND id > 20 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' OR size > 100 AND id > 20 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 OR id > 20 ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 AND id > NULL ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 OR NOT (id > NULL) ORDER BY id LIMIT 2;");
        list.add("SELECT clientip FROM ${tableName} WHERE request MATCH 'GET' AND size > 100 AND NOT (id > NULL) ORDER BY id LIMIT 2;");

        // compound
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE request MATCH 'GET' OR size > 100 AND status > 20 OR clientip = '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE request MATCH 'GET' OR size > 100 AND clientip LIKE '%7%' OR clientip = '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE request MATCH 'GET' OR size > 100 AND status LIKE '%0%' OR clientip = '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE id > 200 OR id < 300 OR size > 100 AND status LIKE '%0%' OR clientip = '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE id > 3000 OR request MATCH 'GET' OR size > 100 AND status LIKE '%0%' OR clientip = '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE id != 200 AND request LIKE '%GET%' OR size > 100 AND status LIKE '%0%' OR clientip > '17.0.0.0' ORDER BY id LIMIT 2;");
        list.add("SELECT `@timestamp` FROM ${tableName} WHERE id != 200 OR request LIKE '%GET%' OR NOT (size > 100 AND size LIKE '%0%') OR clientip > '17.0.0.0' ORDER BY id LIMIT 2;");
        return list
    }
    
    
    try {
         sql """ set enable_match_without_inverted_index = true """
        sql """ set enable_common_expr_pushdown = true """
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
        load_data.call(dupTableName, 'documents-1000.json');
        load_data.call(dupTableName, 'documents-1000.json');
        load_data.call(dupTableName, 'documents-1000.json');
        load_data.call(dupTableName, 'documents-1000.json');
        // load_data.call(dupTableName, 'documents-191998.json');
        // load_data.call(dupTableName, 'documents-201998.json');
        // load_data.call(dupTableName, 'documents-231998.json');
        // load_data.call(dupTableName, 'documents-221998.json');
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, NULL, 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', NULL, 500, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', NULL, 1000) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, NULL) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', 304, 24736) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '?', '--', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '@', '--', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '4DMXbAotfXMmKJ0KDtkFJm5qHkMr5wpYd0GIHWb18KH8JWijlincsqkjCV13m7VWOMcFz2z', '4DMXbAotfXMmKJ0KDtkFJm5qHkMr5wpYd0GIHWb18KH8JWijlincsqkjCV13m7VWOMcFz2z', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '', '', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', -2, -3) """
        sql """ INSERT INTO ${dupTableName} (`@timestamp`, clientip, request, status, size) VALUES (100, '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', -2, -3) """
        sql """ sync """
        def all_dup_sql = generate_dup_mow_sql.call(dupTableName)
        def dup_result1 = execute_sql.call("enable_no_need_read_data_opt", "true", all_dup_sql)
        logger.info("dup_result1 is {}", dup_result1);
        def dup_result2 = execute_sql.call("enable_no_need_read_data_opt", "false", all_dup_sql)
        logger.info("dup_result2 is {}", dup_result2);
        compare_result(dup_result1, dup_result2, all_dup_sql)

        // delete
        sql """ delete from dup_httplogs where clientip = '40.135.0.0'; """
        sql """ delete from dup_httplogs where status = 304; """
        sql """ delete from dup_httplogs where size = 24736; """
        sql """ delete from dup_httplogs where request = 'GET /images/hm_bg.jpg HTTP/1.0'; """

        def dup_result3 = execute_sql.call("enable_no_need_read_data_opt", "true", all_dup_sql)
        logger.info("dup_result3 is {}", dup_result3);
        def dup_result4 = execute_sql.call("enable_no_need_read_data_opt", "false", all_dup_sql)
        logger.info("dup_result4 is {}", dup_result4);
        compare_result(dup_result3, dup_result4, all_dup_sql)

        run_compaction.call(dupTableName)
        def dup_result5 = execute_sql.call("enable_no_need_read_data_opt", "true", all_dup_sql)
        logger.info("dup_result5 is {}", dup_result5);
        def dup_result6 = execute_sql.call("enable_no_need_read_data_opt", "false", all_dup_sql)
        logger.info("dup_result6 is {}", dup_result6);
        compare_result(dup_result5, dup_result6, all_dup_sql)
        compare_result(dup_result3, dup_result6, all_dup_sql)

        def dup_result7 = execute_sql.call("enable_common_expr_pushdown_for_inverted_index", "true", all_dup_sql)
        logger.info("dup_result7 is {}", dup_result7);
        def dup_result8 = execute_sql.call("enable_common_expr_pushdown_for_inverted_index", "false", all_dup_sql)
        logger.info("dup_result8 is {}", dup_result8);
        compare_result(dup_result7, dup_result8, all_dup_sql)
        compare_result(dup_result3, dup_result8, all_dup_sql)

         def mowTable = "mow_httplogs"
        sql """ drop table if exists ${mowTable} """
        // create table
        sql """
            CREATE TABLE IF NOT EXISTS mow_httplogs
            (   
                `@timestamp` int(11) NULL,
                `clientip`   varchar(20) NULL,
                `request`    text NULL,
                `status`     int(11) NULL,
                `size`       int(11) NULL,
                `id`          bigint NOT NULL AUTO_INCREMENT(100),
                INDEX        clientip_idx (`clientip`) USING INVERTED COMMENT '',
                INDEX        request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
                INDEX        status_idx (`status`) USING INVERTED COMMENT '',
                INDEX        size_idx (`size`) USING INVERTED COMMENT ''
            ) UNIQUE KEY(`@timestamp`)
            DISTRIBUTED BY HASH (`@timestamp`) BUCKETS 32
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compaction_policy" = "time_series",
            "inverted_index_storage_format" = "v2",
            "compression" = "ZSTD",
            "disable_auto_compaction" = "true"
            );
        """

        load_data.call(mowTable, 'documents-1000.json');
        load_data.call(mowTable, 'documents-1000.json');
        load_data.call(mowTable, 'documents-1000.json');
        load_data.call(mowTable, 'documents-1000.json');
        load_data.call(mowTable, 'documents-1000.json');
        // load_data.call(mowTable, 'documents-191998.json');
        // load_data.call(mowTable, 'documents-201998.json');
        // load_data.call(mowTable, 'documents-231998.json');
        // load_data.call(mowTable, 'documents-221998.json');
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, NULL, 'GET /api/v1/organizations/1 HTTP/1.1', 500, 1000) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', NULL, 500, 1000) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', NULL, 1000) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '10.16.10.6', 'GET /api/v1/organizations/1 HTTP/1.1', 500, NULL) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', 304, 24736) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', 304, 24736) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '.', '--', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '?', '--', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '@', '--', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '4DMXbAotfXMmKJ0KDtkFJm5qHkMr5wpYd0GIHWb18KH8JWijlincsqkjCV13m7VWOMcFz2z', '4DMXbAotfXMmKJ0KDtkFJm5qHkMr5wpYd0GIHWb18KH8JWijlincsqkjCV13m7VWOMcFz2z', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '', '', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', -2, -3) """
        sql """ INSERT INTO ${mowTable} (`@timestamp`, clientip, request, status, size) VALUES (100, '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', '1zyvBkWVAy5H0DDDaQnrp9MmAhfo0UNB9bOGyvSEX9MW66eymeDElVzVmsvUKHORwWg2hRLN7yd253zhXGs6k7PVPHy6uqtYTaxvHWm7njZYWtlqraDGE1fvrtnyUvlrGFPJZzkuj5FQfpYl6dV2bJHV0A3gpzogKSXJSyfH02ryb2ObKaJC6dnkMic00P6R3rUCBotrU7KAaGieALbFUXBGTvjsFKUvLgJexqAEJcKwiioTp0JH9Y3NUWgi2y5kclPmUG4xVrKHWXu7bI1MYJ1DCL1eCQCuqXmUf7eFyKcR6pzTFpkurcYq5R3SjprK13EkuLmVcDJMS8DNiLVCcCIOpHQMNgVFNLI7SPCl461FPOrL1xSuULAsLNjP5xgjjpn5Bu2dAug906fSVcnJwfHuuCly0sqYfNEI0Bd1IMiQOyoqA1pwdJMYMa6hig6imR3bJcnPptA6Fo1rooqzzt6gFnloqXeo9Hd9UB1F7QhfZO21QOZho19A5d12wcnOZCb3sRzomQqcPKSyvb17SxzoP9coAEpfXZEBrds60iuPjZaez79zeGP8X4KxuK1WwVDFw661zB6nvKCtNKFQqeKVMSFWAazw735TkQRGkjlif31f3uspvmBrLagvtjlfMoT138NnNxc2FbsK5wmssNfKFRk9zNg629b46rX7qLnC3ItPYgXyPSFqSF7snjqOUHJpzvcPhyY7tuDZVW2VTd3OtRdjdlAwHbSUrI5jWI1BCeP8cObIsOjd5', -2, -3) """
        sql """ sync """
        
        def all_mow_sql = generate_dup_mow_sql.call(mowTable)
        def mow_result1 = execute_sql.call("enable_no_need_read_data_opt", "true", all_mow_sql)
        logger.info("mow_result1 is {}", mow_result1);
        def mow_result2 = execute_sql.call("enable_no_need_read_data_opt", "false", all_mow_sql)
        logger.info("mow_result2 is {}", mow_result2);
        compare_result(mow_result1, mow_result2, all_mow_sql)

        // delete
        sql """ delete from ${mowTable} where request MATCH 'GET' """
        sql """ delete from ${mowTable} where clientip = '40.135.0.0'; """
        sql """ delete from ${mowTable} where status = 304; """
        sql """ delete from ${mowTable} where size = 24736; """
        sql """ delete from ${mowTable} where request = 'GET /images/hm_bg.jpg HTTP/1.0'; """

       def mow_result3 = execute_sql.call("enable_no_need_read_data_opt", "true", all_mow_sql)
        logger.info("mow_result3 is {}", mow_result3);
        def mow_result4 = execute_sql.call("enable_no_need_read_data_opt", "false", all_mow_sql)
        logger.info("mow_result4 is {}", mow_result4);
        compare_result(mow_result3, mow_result4, all_mow_sql)

        run_compaction.call(mowTable)
        def mow_result5 = execute_sql.call("enable_no_need_read_data_opt", "true", all_mow_sql)
        logger.info("mow_result5 is {}", mow_result5);
        def mow_result6 = execute_sql.call("enable_no_need_read_data_opt", "false", all_mow_sql)
        logger.info("mow_result6 is {}", mow_result6);
        compare_result(mow_result5, mow_result6, all_mow_sql)
        compare_result(mow_result3, mow_result6, all_mow_sql)

        def generate_join_sql = { tableNameA, tableNameB ->
            List<String> list = new ArrayList<>()
            list.add("SELECT COUNT() from ${tableNameA} a, ${tableNameB} b WHERE a.clientip = b.clientip AND a.request = b.request AND a.status = b.status AND a.size = b.size")
            list.add("SELECT COUNT() from ${tableNameA} a JOIN ${tableNameB} b ON a.clientip = b.clientip  WHERE a.clientip MATCH '227.16.0.0'")
            list.add("SELECT COUNT() from ${tableNameA} a JOIN ${tableNameB} b ON a.size = b.size WHERE a.size > 20 ")
            list.add("SELECT COUNT() from ${tableNameA} a JOIN ${tableNameB} b ON a.status = b.status WHERE a.status > 200")
            return list
        }

        def all_join_sql = generate_join_sql.call(dupTableName, mowTable)
        def join_result1 = execute_sql.call("enable_no_need_read_data_opt", "true", all_join_sql)
        logger.info("join_result1 is {}", join_result1);
        def join_result2 = execute_sql.call("enable_no_need_read_data_opt", "false", all_join_sql)
        logger.info("mow_result2 is {}", join_result2);
        compare_result(join_result1, join_result2, all_join_sql)
    } finally {
        sql """ set enable_match_without_inverted_index = true """
    }
}