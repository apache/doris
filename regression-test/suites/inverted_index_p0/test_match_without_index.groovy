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


suite("test_match_without_index", "p0") {

    def testTable = "test_match_without_index"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE ${testTable} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` string NULL COMMENT "",
          `size` int NULL COMMENT "",
           INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
            INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="unicode", "lower_case" = "false") COMMENT '',
            INDEX status_idx (`status`) USING INVERTED COMMENT '',
            INDEX size_idx (`size`) USING INVERTED COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
      """
    sql """ set enable_common_expr_pushdown = true """

    sql """ INSERT INTO ${testTable} VALUES (123, '17.0.0.0', 'HTTP GET', '200', 20); """
    sql """ INSERT INTO ${testTable} VALUES (123, '17.0.0.0', 'Life is like a box of chocolates, you never know what you are going to get.', '200', 20); """
    // sql """ """

    List<Object> match_res_without_index = new ArrayList<>();
    List<Object> match_res_with_index =new ArrayList<>();
    def create_sql = {
        List<String> list = new ArrayList<>()
        list.add(" select count() from ${testTable} where clientip match_phrase '17' ");
        list.add(" select count() from ${testTable} where clientip match_all '17' ");
        list.add(" select count() from ${testTable} where clientip match_any '17' ");
        list.add(" select count() from ${testTable} where request match_any 'get' ");
        list.add(" select count() from ${testTable} where request match_phrase_prefix 'like box' ");
        return list;
    }

    def execute_sql = { resultList, sqlList ->
        for (sqlStr in sqlList) {
            def sqlResult = sql """ ${sqlStr} """
            resultList.add(sqlResult)
        }
    }
    
    def compare_result = { executedSql ->
        assertEquals(match_res_without_index.size(), match_res_with_index.size())
        for (int i = 0; i < match_res_without_index.size(); i++) {
            if (match_res_without_index[i] != match_res_with_index[i]) {
                logger.info("sql is {}", executedSql[i])
                logger.info("match_res_without_index is {}", match_res_without_index[i])
                logger.info("match_res_with_index is {}", match_res_with_index[i])
                assertTrue(false)
            }
        }
    }

    def index_sql = create_sql.call()
    try {
        GetDebugPoint().enableDebugPointForAllBEs("return_inverted_index_bypass")
        execute_sql.call(match_res_without_index, index_sql)
    
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("return_inverted_index_bypass")
        execute_sql.call(match_res_with_index, index_sql)
        compare_result.call(index_sql)
    }
    
}