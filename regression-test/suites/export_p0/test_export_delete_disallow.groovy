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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_export_delete_disallow", "p0") {
    def db = "regression_test_export_p0"
    
    // check whether the FE config 'enable_outfile_to_local' is true
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")

    String command = strBuilder.toString()
    def process = command.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    def configJson = response.data.rows
    boolean enableOutfileToLocal = false
    for (Object conf: configJson) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "enable_outfile_to_local") {
            enableOutfileToLocal = ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
        }
    }
    if (!enableOutfileToLocal) {
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile")
        return
    }
    
    // create table and insert
    sql """ DROP TABLE IF EXISTS test_export_delete_disallow_tbl"""
    sql """
    CREATE TABLE IF NOT EXISTS test_export_delete_disallow_tbl (
        `id` int(11) NULL,
        `Name` string NULL
        )
        PROPERTIES("replication_num" = "1");
    """
    sql """insert into test_export_delete_disallow_tbl values(1, "abc");"""
    
    test {
         sql """select * from test_export_delete_disallow_tbl limit 10 INTO OUTFILE "file:///tmp/" properties("delete_existing_files" = "true");"""
         exception """Deleting existing files is not allowed"""
    }

    test {
        sql """EXPORT TABLE test_export_delete_disallow_tbl TO "file:///tmp" PROPERTIES ("delete_existing_files" = "true");"""
        exception """Deleting existing files is not allowe"""
    }
}
