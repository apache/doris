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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

import java.time.LocalDateTime
import java.time.Duration
import java.time.format.DateTimeFormatter

suite('test_manager_interface_5',"p0") {
    // /rest/v2/manager/query/trace_id/{trace_id}
    // /rest/v2/manager/query/trace_id/{trace_id}
    def test_trace_id = {
        def futures = []
        futures.add( thread {
            try{
                sql """set session_context="trace_id:test_manager_interface_5_trace_id""""
                sql """ select *  from numbers("number" = "9910") as  a  join numbers('number'="18880094567") as b on a.number = b.number; """
            }catch(Exception e){
                
            }
        })

        futures.add( thread {
            // test trace id in processlist
            sleep(500);
            List<List<Object>> result = sql_return_maparray """  show processlist  """
            def queryid = ""
            def x = 0
            logger.info("result = ${result}")
            for( int i =0 ;i < result.size();i++ ){
                if (result[i]["Info"].contains("18880094567")) {
                    queryid = result[i]["QueryId"]
                    assertTrue(result[i]["TraceId"].equals("test_manager_interface_5_trace_id"))
                    x = 1
                    break;
                }
            }
            assertTrue(x == 1)

            result = sql_return_maparray """  select * from information_schema.processlist  """
            def queryid2 = ""
            def x2 = 0
            logger.info("result = ${result}")
            for( int i =0 ;i < result.size();i++ ){
                if (result[i]["Info"].contains("18880094567")) {
                    queryid2 = result[i]["QueryId"]
                    assertTrue(result[i]["TraceId"].equals("test_manager_interface_5_trace_id"))
                    x2 = 1
                    break;
                }
            }
            assertTrue(x2 == 1)

            // test trace id in cancel query and get realtime query statistics
            // 1. test get query id by trace id
            def fes = sql_return_maparray "show frontends"
            logger.info("frontends: ${fes}")
            def fe = fes[0]
            def url = "http://${fe.Host}:${fe.HttpPort}/rest/v2/manager/query/trace_id/test_manager_interface_5_trace_id?is_all_node=true"
            def (code, out, err) = curl("GET", url, null, 10, context.config.jdbcUser, context.config.jdbcPassword)
            println "${out}"
            assertTrue(code == 0)
            def getQueryId = parseJson(out).get("data");
            assertEquals(queryid2, getQueryId);

            // 2. test get realtime query statistics by trace id
            url = "http://${fe.Host}:${fe.HttpPort}/rest/v2/manager/query/statistics/test_manager_interface_5_trace_id?is_all_node=true"
            (code, out, err) = curl("GET", url, null, 10, context.config.jdbcUser, context.config.jdbcPassword)
            println "${out}"
            assertTrue(code == 0)
            def stats = parseJson(out).get("data");
            assertTrue(stats.containsKey("cpuMs"));

            // 3. test cancel query by query id
            url = "http://${fe.Host}:${fe.HttpPort}/rest/v2/manager/query/kill/${queryid2}?is_all_node=true"
            (code, out, err) = curl("POST", url, null, 10, context.config.jdbcUser, context.config.jdbcPassword)
            println "${out}"
            assertTrue(code == 0)
        })
        futures.each { it.get() }
    }
    test_trace_id();
}
