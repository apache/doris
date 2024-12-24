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
def QUERY_NUM = 5

def random = new Random()

def getRandomNumber(int num){
    return random.nextInt(num)
}

suite('test_profile') {

    // nereids not return same profile with legacy planner, fallback to legacy planner.
    sql """set enable_nereids_planner=false"""

    def table = 'test_profile_table'
    def id_data = [1,2,3,4,5,6,7]
    def value_data = [1,2,3,4,5,6,7]
    def len = id_data.size

    assertEquals(id_data.size, value_data.size)

    sql """ DROP TABLE IF EXISTS ${table} """

    sql """
        CREATE TABLE IF NOT EXISTS ${table}(
            `id` INT,
            `cost` INT
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql """ SET enable_profile = true """

    //———————— test for insert stmt ——————————
    for(int i = 0; i < len; i++){
        sql """ INSERT INTO ${table} values (${id_data[i]}, "${value_data[i]}") """
    }

    //———————— test for insert stmt (SQL) ——————————
    log.info("test HTTP API interface for insert profile")
    def url = '/rest/v1/query_profile/'
    def query_list_result = http_get(url)

    def obj = new JsonSlurper().parseText(query_list_result)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    for(int i = 0 ; i < len ; i++){
        def insert_order = len - i - 1
        def stmt_query_info = obj.data.rows[i]
        
        assertNotNull(stmt_query_info["Profile ID"])
        assertNotEquals(stmt_query_info["Profile ID"], "N/A")
        
        assertEquals(stmt_query_info['Sql Statement'].toString(), 
            """ INSERT INTO ${table} values (${id_data[insert_order]}, "${value_data[insert_order]}") """.toString())
    }

    //———————— test for select stmt ———————————
    def op_data = ["<", ">", "=", "<=", ">="]

    def ops = []
    def nums = []
    
    for(int i = 0 ; i < QUERY_NUM ; i++){
        ops.add(op_data[getRandomNumber(5)])
        nums.add(getRandomNumber(len + 1))
        sql """ SELECT * FROM ${table} WHERE cost ${ops[i]} ${nums[i]} """
    }

    //———————— test for select stmt (HTTP)————————
    log.info("test HTTP API interface for query profile")
    url = '/rest/v1/query_profile/'
    query_list_result = http_get(url)

    obj = new JsonSlurper().parseText(query_list_result)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    for(int i = 0 ; i < QUERY_NUM ; i++){
        def insert_order = QUERY_NUM - i - 1
        def stmt_query_info = obj.data.rows[i]
        
        assertNotNull(stmt_query_info["Profile ID"])
        assertNotEquals(stmt_query_info["Profile ID"].toString(), "N/A".toString())
        assertNotNull(stmt_query_info["Default Catalog"])
        assertEquals(stmt_query_info['Sql Statement'].toString(), 
           """ SELECT * FROM ${table} WHERE cost ${ops[insert_order]} ${nums[insert_order]} """.toString())
    }
    
    //———————— clean table and disable profile ————————
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    int randomInt = Math.random() * 2000000000
    profile("test_profile_time_${randomInt}") {
        run {
            sql "/* test_profile_time_${randomInt} */ select ${randomInt} from test_profile"
        }

        check { profileString, exception ->
            log.info(profileString)
            assertTrue(profileString.contains("Nereids  GarbageCollect  Time"))
            assertTrue(profileString.contains("Nereids  BeFoldConst  Time"))
        }
    }

    sql """ SET enable_profile = false """
    sql """ DROP TABLE IF EXISTS ${table} """
}
