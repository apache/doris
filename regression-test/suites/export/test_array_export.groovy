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
    def outFilePath = """${context.file.parent}/test_array_export"""
    List<List<Object>> backends =  sql """ show backends """
    assertTrue(backends.size() > 0)
    def outFile = outFilePath
    if (backends.size() > 1) {
        outFile = "/tmp"
    }
    def urlHost = ""
    def csvFiles = ""
    logger.warn("test_array_export the outFile=" + outFile)

    def create_test_table = {testTablex ->
        sql """ DROP TABLE IF EXISTS ${tableName} """

        def result1 = sql """
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
              `k11` ARRAY<DECIMALV3(27, 9)> NULL COMMENT ""
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
    }

    def export_to_hdfs = {exportTable, exportLable, hdfsPath, exportFormat, BrokerName, HdfsUserName, HdfsPasswd->
        sql """ EXPORT TABLE ${exportTable}
                TO "${hdfsPath}"
                PROPERTIES (
                    "label" = "${exportLable}",
                    "column_separator"=",",
                    "format"="${exportFormat}"
                )
                WITH BROKER "${BrokerName}" (
                    "username"="${HdfsUserName}",
                    "password"="${HdfsPasswd}"
                )
            """
    }

    def select_out_file = {exportTable, HdfsPath, outFormat, BrokerName, HdfsUserName, HdfsPasswd->
        sql """
            SELECT * FROM ${exportTable}
            INTO OUTFILE "${HdfsPath}"
            FORMAT AS "${outFormat}"
            PROPERTIES
            (
                "broker.name" = "${BrokerName}",
                "line_delimiter" = "\n",
                "max_file_size" = "10MB",
                "broker.username"="${HdfsUserName}",
                "broker.password"="${HdfsPasswd}"
            )
        """
    }

    def check_export_result = {checklabel->
        max_try_milli_secs = 15000
        while(max_try_milli_secs) {
            def result = sql "show export where label='${checklabel}'"
            if(result[0][2] == "FINISHED") {
                break
            } else {
                sleep(1000)  // wait 1 second every time
                max_try_milli_secs -= 1000
                if(max_try_milli_secs <= 0) {
                    assertEquals(1,2)
                }
            }
        }
    }

    def check_download_result={resultlist, expectedTotalRows->
        int totalLines = 0
        for(String oneFile :resultlist) {
            totalLines += getTotalLine(oneFile)
            deleteFile(oneFile)
        }
        assertEquals(expectedTotalRows, totalLines)
    }

    // case1: test "select ...into outfile ...."
    try {
        create_test_table.call(tableName)

        qt_select_default """ SELECT * FROM ${tableName} t ORDER BY k1; """

        // check outfile
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        def result = sql """
            SELECT * FROM ${tableName} t ORDER BY k1 INTO OUTFILE "file://${outFile}/";
        """
        def url = result[0][3]
        urlHost = url.substring(8, url.indexOf("${outFile}"))
        if (backends.size() > 1) {
            // custer will scp files
            def filePrifix = url.split("${outFile}")[1]
            csvFiles = "${outFile}${filePrifix}*.csv"
            scpFiles ("root", urlHost, csvFiles, outFilePath)
        }

        // path is from outputfilepath
        File[] files = path.listFiles()
        assert files.length == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        List<String> baseLines = Files.readAllLines(Paths.get("""${context.config.dataPath}/export/test_array_export.out"""), StandardCharsets.UTF_8)
        for (int j = 0; j < outLines.size(); j ++) {
            String[] outLine = outLines.get(j).split("\t")
            String[] baseLine = baseLines.get(j + 2).split("\t")
            assert outLine.length == baseLine.length
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
        if (csvFiles != "") {
            def cmd = "rm -rf ${csvFiles}"
            sshExec("root", urlHost, cmd)
        }
    }
    
    
    if (enableHdfs()) {
        brokerName = getBrokerName()
        hdfsUser = getHdfsUser()
        hdfsPasswd = getHdfsPasswd()
        hdfsDataDir = getHdfsDataDir()

        // case2: test "select ...into outfile 'hdfs_path'"
        try {
            create_test_table.call(tableName)

            resultCount = sql "select count(*) from ${tableName}"
            currentTotalRows = resultCount[0][0]

            label = UUID.randomUUID().toString().replaceAll("-", "")
            select_out_file(tableName, hdfsDataDir + "/" + label + "/export-data", "csv", brokerName, hdfsUser, hdfsPasswd)
            result = downloadExportFromHdfs(label + "/export-data")
            check_download_result(result, currentTotalRows)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }

        // case3: test "export table to hdfs"
        try {
            create_test_table.call(tableName)

            resultCount = sql "select count(*) from ${tableName}"
            currentTotalRows = resultCount[0][0]

            label = UUID.randomUUID().toString().replaceAll("-", "")
            export_to_hdfs.call(tableName, label, hdfsDataDir + "/" + label, '', brokerName, hdfsUser, hdfsPasswd)
            check_export_result(label)
            result = downloadExportFromHdfs(label + "/export-data")
            check_download_result(result, currentTotalRows)
        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }
    }
}
