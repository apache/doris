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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
*   @Params url is "/xxx"
*   @Return response body
*/
def http_get(url) {
    def dst = 'http://' + context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("GET")
    //token for root
    conn.setRequestProperty("Authorization","Basic cm9vdDo=")
    return conn.getInputStream().getText()
}

def SUCCESS_MSG = 'success'
def SUCCESS_CODE = 0

suite('test_insert_profile') {
    def table = 'test_insert_profile_table'
    def id_data = [1,2,3,4,5,6,7]
    def name_data = ["A","B","C","D","E","F","G"]
    def len = id_data.size

    assertEquals(id_data.size, name_data.size)

    sql """ DROP TABLE IF EXISTS ${table} """

    sql """
        CREATE TABLE IF NOT EXISTS ${table}(
            `id` INT,
            `name` VARCHAR(20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """ SET enable_profile = true """

    for(int i = 0; i < len; i++){
        sql """ INSERT INTO ${table} values (${id_data[i]}, "${name_data[i]}") """
    }
    
    def url = '/rest/v1/query_profile/'
    def query_list_result = http_get(url)

    def obj = new JsonSlurper().parseText(query_list_result)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    for(int i = 0 ; i < len ; i++){
        def insert_order = len - i - 1;
        def stmt_query_info = obj.data.rows[i]
        assertNotNull(stmt_query_info["Query ID"])
        assertNotEquals(stmt_query_info["Query ID"], "N/A")
        assertEquals(stmt_query_info['Sql Statement'].toString(), 
            """ INSERT INTO ${table} values (${id_data[insert_order]}, "${name_data[insert_order]}") """.toString())
    }

    sql """ SET enable_profile = false """
    sql """ DROP TABLE IF EXISTS ${table} """
}
