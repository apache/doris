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
*   @Params url is "/xxx", data is request body
*   @Return response body
*/
def http_post(url, data = null) {
    def dst = "http://"+ context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("Authorization", "Basic cm9vdDo=")
    if (data) {
        // 
        logger.info("query body: " + data)
        conn.doOutput = true
        def writer = new OutputStreamWriter(conn.outputStream)
        writer.write(data)
        writer.flush()
        writer.close()
    }
    return conn.content.text
}

def SUCCESS_MSG = "success"
def SUCCESS_CODE = 0

class Stmt {
    String stmt
}
suite("test_query_stmt") {
    def url= "/api/query/default_cluster/" + context.config.defaultDb

    // test select
    def stmt1 = """ select * from numbers('number' = '10') """ 
    def stmt1_json = JsonOutput.toJson(new Stmt(stmt: stmt1));

    def resJson = http_post(url, stmt1_json)
    def obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)
    def data = [[0],[1],[2],[3],[4],[5],[6],[7],[8],[9]];
    assertEquals(obj.data.data, data)

    def tableName = "test_query_stmt"
    def stmt_drop_table =  """ DROP TABLE IF EXISTS ${tableName} """
    def stmt_drop_table_json = JsonOutput.toJson(new Stmt(stmt: stmt_drop_table));
    resJson = http_post(url, stmt_drop_table_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test create table if not exists
    def stmt2 = """
        CREATE TABLE IF NOT EXISTS ${tableName}
        (
            id TINYINT,
            name CHAR(10) NOT NULL DEFAULT "zs"
        )
        COMMENT "test query_stmt table"
        DISTRIBUTED BY HASH(id)
        PROPERTIES("replication_num" = "1");
    """
    def stmt2_json = JsonOutput.toJson(new Stmt(stmt: stmt2));
    resJson = http_post(url, stmt2_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test insert
    def stmt3 = " insert into ${tableName} (id,name) values (1,'aa'),(2,'bb'),(3,'cc')"
    def stmt3_json = JsonOutput.toJson(new Stmt(stmt: stmt3));
    resJson = http_post(url, stmt3_json)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)

    // test select
    def stmt4 = " select * from ${tableName}"
    def stmt4_json = JsonOutput.toJson(new Stmt(stmt: stmt4));
    resJson = http_post(url, stmt4_json)
    // println(resJson)
    obj = new JsonSlurper().parseText(resJson)
    assertEquals(obj.msg, SUCCESS_MSG)
    assertEquals(obj.code, SUCCESS_CODE)
    // we can only check the number is correctly
    assertEquals(obj.data.data.size, 3)
}
