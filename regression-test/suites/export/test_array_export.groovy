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

suite("test_array_export", "export") {
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
    
    // define the table and out file path
    def tableName = "array_outfile_test"
    def outFilePath = """${context.file.parent}/tmp"""
    logger.warn("test_array_export the outFilePath=" + outFilePath)
    
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql "ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true')"
        result1 = sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<SMALLINT> NOT NULL COMMENT "",
              `k3` ARRAY<INT(11)> NOT NULL COMMENT "",
              `k4` ARRAY<BIGINT> NOT NULL COMMENT "",
              `k5` ARRAY<CHAR> NOT NULL COMMENT "",
              `k6` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k7` ARRAY<DATE> NOT NULL COMMENT "", 
              `k8` ARRAY<DATETIME> NOT NULL COMMENT "",
              `k9` ARRAY<FLOAT> NOT NULL COMMENT "",
              `k10` ARRAY<DOUBLE> NOT NULL COMMENT "",
              `k11` ARRAY<DECIMAL(27, 9)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
        """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        sql """ INSERT INTO ${tableName} VALUES
                        (1, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], ['a', 'b', 'c'], ["hello", "world"], 
                        ['2022-07-13'], ['2022-08-15 12:30:00'], [0.331111, 0.672222], [3.141592, 0.878787], [4.2222, 5.5555, 6.67])
                        """
        
        sql """ INSERT INTO ${tableName} VALUES
                        (2, [4, 5, 6], [32767, 32768, 32769], [65534, 65535, 65536], ['d', 'e', 'f'], ["good", "luck"], 
                        ['2022-07-13'], ['2022-08-15 15:59:59'], [0.333336, 0.666677], [3.141592, 0.878787], [4.22222, 5.5555555, 6.6666777])
                        """
        
        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY k1; """

        // check outfile
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM ${tableName} t ORDER BY k1 INTO OUTFILE "file://${outFilePath}/";
        """
        File[] files = path.listFiles()
        assert files.length == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        List<String> baseLines = Files.readAllLines(Paths.get("""${context.config.dataPath}/export/test_array_export.out"""), StandardCharsets.UTF_8)
        for (int j = 0; j < outLines.size(); j ++) {
            String[] outLine = outLines.get(j).split("\t")
            String[] baseLine = baseLines.get(j + 2).split("\t")
            for (int slotId = 0; slotId < outLine.size(); slotId ++) {
                assert outLine[slotId] == baseLine[slotId]
            }
        }
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
