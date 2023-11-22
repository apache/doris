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

suite("test_outfile_separator") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


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
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile_separator")
        return
    }
    def dbName = context.config.getDbNameByFile(context.file)
    def tableName = "outfile_test_separator"
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp/test_outfile_separator_${uuid}"""
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            k1 int, k2 varchar(100)
            )
            DISTRIBUTED BY HASH(k1) PROPERTIES("replication_num" = "1");
        """
        sql """ INSERT INTO ${tableName} VALUES
            (1, "abc"), (2, "def");
            """
        qt_select_1 """ SELECT * FROM ${tableName} t ORDER BY k1; """

        // check outfile
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${tableName} t INTO OUTFILE "file://${outFilePath}/" properties("column_separator" = "\\\\x01");
        """
        File[] files = path.listFiles()
        assert files.length == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        assert outLines.size() == 2

        streamLoad {
            db """${dbName}"""
            table 'outfile_test_separator'
            set 'column_separator', '\\x01'
            file files[0].getAbsolutePath()
            time 10000 // limit inflight 10s
        }

        sql """ sync; """

        qt_select_2 """ SELECT * FROM ${tableName} t ORDER BY k1; """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

}
