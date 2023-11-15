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

suite("test_struct_export", "export") {
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
    def testTable = "tbl_test_struct_export"

    sql "DROP TABLE IF EXISTS ${testTable}"


    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
                      `k1` INT(11) NULL,
                      `k2` STRUCT<f1:BOOLEAN,f2:TINYINT,f3:SMALLINT,f4:INT,f5:INT,f6:BIGINT,f7:LARGEINT> NULL,
                      `k3` STRUCT<f1:FLOAT,f2:DOUBLE,f3:DECIMAL(3,3)> NULL,
                      `k4` STRUCT<f1:DATE,f2:DATETIME,f3:DATEV2,f4:DATETIMEV2> NULL,
                      `k5` STRUCT<f1:CHAR(10),f2:VARCHAR(10),f3:STRING> NOT NULL
        )
        DUPLICATE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // make data
    sql "INSERT INTO ${testTable} VALUES(1, {1,11,111,1111,11111,11111,111111},null,null,{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(2, {null,null,null,null,null,null,null},{2.1,2.22,0.333},null,{null,null,null})"
    sql "INSERT INTO ${testTable} VALUES(3, null,{null,null,null},{'2023-02-23','2023-02-23 00:10:19','2023-02-23','2023-02-23 00:10:19'},{'','',''})"
    sql "INSERT INTO ${testTable} VALUES(4, null,null,{null,null,null,null},{'abc','def','hij'})"

    // check result
    qt_select """ SELECT * FROM ${testTable} ORDER BY k1; """
    qt_select_count """SELECT COUNT(k2), COUNT(k4) FROM ${testTable}"""

    def outFilePath = """${context.file.parent}/test_struct_export"""
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def outFile = outFilePath
    if (backends.size() > 1) {
        outFile = "/tmp"
    }
    def url = ""
    def urlHost = ""
    def csvFiles = ""
    logger.info("test_struct_export the outFilePath=" + outFilePath)
    // struct select into outfile
    try {
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        result = sql """
                    SELECT * FROM ${testTable} ORDER BY k1 INTO OUTFILE "file://${outFile}/";
        """
        url = result[0][3]
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
        assert outLines.size() == 4
        for (int r = 0; r < outLines.size(); r++) {
            String[] outLine = outLines.get(r).split("\t")
            logger.info("test_struct_export: " + outLines.get(r))
            assert outLine.size() == 5
            // check NULL
            if (outLine[0] == 1) {
                assert outLine[1] == "{1,11,111,1111,11111,11111,111111}"
                assert outline[2] == "\\N"
                assert outline[3] == "\\N"
                assert outline[4] == "{'','',''}"
            }
            if (outLine[0] == 2) {
                assert outLine[1] == "{null,null,null,null,null,null,null},{2.1,2.22,2.333},null,{null,null,null})"
                assert outLine[2] == "{2.1,2.22,2.333}"
                assert outline[3] == "\\N"
                assert outline[4] == "{null,null,null}"
            }
            if (outLine[0] == 3) {
                assert outLine[1] == "\\N"
                assert outline[2] == "{null,null,null}"
                assert outline[3] == "{'2023-02-23','2023-02-23 00:10:19','2023-02-23','2023-02-23 00:10:19'}"
                assert outline[4] == "{'','',''}"
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
            cmd = "rm -rf ${csvFiles}"
            sshExec("root", urlHost, cmd)
        }
    }
}
