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

import org.apache.doris.regression.util.Http

suite("test_show_processlist") {
    sql """set show_all_fe_connection = false;"""
    def result = sql """show processlist;"""
    logger.info("result:${result}")
    assertTrue(result[0].size() == 14)
    sql """set show_all_fe_connection = true;"""
    result = sql """show processlist;"""
    logger.info("result:${result}")
    assertTrue(result[0].size() == 14)
    sql """set show_all_fe_connection = false;"""

    def url1 = "http://${context.config.feHttpAddress}/rest/v1/session"
    result =  Http.GET(url1, true)
    logger.info("result:${result}")
    assertTrue(result["data"]["column_names"].size() == 14);

    def url2 = "http://${context.config.feHttpAddress}/rest/v1/session/all"
    result = Http.GET(url2, true)
    logger.info("result:${result}")
    assertTrue(result["data"]["column_names"].size() == 14);
}