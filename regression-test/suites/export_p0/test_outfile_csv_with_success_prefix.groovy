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

suite("test_outfile_csv_with_success_prefix") {
    def dbName = "test_outfile_csv_with_success_prefix"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"
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
    def tableName = "outfile_csv_with_success_prefix"
    def tableName2 = "outfile_csv_with_success_prefix2"
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp/test_outfile_csv_with_success_prefix_${uuid}"""

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `user_id` INT NOT NULL COMMENT "用户id"
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES (1),(2);
            """
        order_qt_select_default1 """ SELECT * FROM ${tableName} t ORDER BY user_id; """

        // check outfile
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${tableName} t ORDER BY user_id INTO OUTFILE "file://${outFilePath}/" FORMAT AS CSV_WITH_NAMES
            PROPERTIES("column_separator" = ",", "success_file_name"="_SUCCESS");
        """

        File[] files = path.listFiles()
        boolean haveSuccessFile = false
        // check file name
        for(File eachFile : files) {
            if ("_SUCCESS".equals(eachFile.getName())) {
                haveSuccessFile = true
                break
            }
        }
        assert(haveSuccessFile == true)
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }
}

