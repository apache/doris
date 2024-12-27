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

suite("test_map_export", "export") {
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

    // define the table
    def testTable = "tbl_test_map_export"

    sql "DROP TABLE IF EXISTS ${testTable}"


    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            m Map<STRING, INT>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // make data
    sql """ INSERT INTO ${testTable} VALUES (1, NULL); """
    sql """ INSERT INTO ${testTable} VALUES (2, {}); """
    sql """ INSERT INTO ${testTable} VALUES (3, {"  33,amory  ":2, " bet ":20, " cler ": 26}); """
    sql """ INSERT INTO ${testTable} VALUES (4, {"k3":23, null: 20, "k4": null}); """
    sql """ INSERT INTO ${testTable} VALUES (5, {null:null}); """

    // check result
    qt_select """ SELECT * FROM ${testTable} ORDER BY id; """
    qt_select_count """SELECT COUNT(m) FROM ${testTable}"""

    def outFilePath = """${context.file.parent}/test_map_export"""
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def outFile = outFilePath
    if (backends.size() > 1) {
        outFile = "/tmp"
    }
    def urlHost = ""
    def csvFiles = ""
    logger.info("test_map_export the outFilePath=" + outFilePath)
    // map select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        def result = sql """
                    SELECT * FROM ${testTable} ORDER BY id INTO OUTFILE "file://${outFile}/";
        """
        def url = result[0][3]
        urlHost = url.substring(8, url.indexOf("${outFile}"))
        if (backends.size() > 1) {
            // custer will scp files
            def filePrifix = url.split("${outFile}")[1]
            csvFiles = "${outFile}${filePrifix}*.csv"
            scpFiles ("root", urlHost, csvFiles, outFilePath)
        }

        File[] files = path.listFiles()
        assert files.length == 1

        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8)
        assert outLines.size() == 5
        for (int r = 0; r < outLines.size(); r++) {
            String[] outLine = outLines.get(r).split("\t")
            assert outLine.size() == 2
            // check NULL
            if (outLine[0] == 1) {
                assert outLine[1] == "\\N"
            }
            // check empty
            if (outLine[0] == 2) {
                assert outLine[1] == "{}"
            }
            // check key contains ','
            if (outLine[0] == 3) {
                assert outLine[1] == "{\"  33,amory  \":2, \" bet \":20, \" cler \": 26}"
            }
            // check key val NULL
            if (outLine[0] == 4) {
                assert outLine[1] == "{\"k3\":23, null: 20, \"k4\": null}"
            }
            // check key val empty
            if (outLine[0] == 5) {
                assert outLine[1] == "{null:null}"
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f : path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
        if (csvFiles != "") {
            def cmd = "rm -rf ${csvFiles}"
            sshExec("root", urlHost, cmd)
        }
    }
}
