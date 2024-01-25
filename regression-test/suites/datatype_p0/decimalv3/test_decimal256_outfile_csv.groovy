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

suite("test_decimal256_outfile_csv") {
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

    sql "set enable_nereids_planner = true;"
    sql "set enable_decimal256 = true;"

    sql "DROP TABLE IF EXISTS `test_decimal256_outfile_csv`"
    sql """
    CREATE TABLE IF NOT EXISTS `test_decimal256_outfile_csv` (
      `k1` decimalv3(76, 9) NULL COMMENT "",
      `k2` decimalv3(76, 10) NULL default '999999999999999999999999999999999999999999999999999999999999999999.9999999999' COMMENT "",
      `k3` decimalv3(76, 11) NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 8
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );
    """
   streamLoad {
       // you can skip db declaration, because a default db has already been
       // specified in ${DORIS_HOME}/conf/regression-conf.groovy
       // db 'regression_test'
       table "test_decimal256_outfile_csv"

       // default label is UUID:
       // set 'label' UUID.randomUUID().toString()

       // default column_separator is specify in doris fe config, usually is '\t'.
       // this line change to ','
       set 'column_separator', ','

       // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
       // also, you can stream load a http stream, e.g. http://xxx/some.csv
       file """test_decimal256_load.csv"""

       time 10000 // limit inflight 10s

       // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

       // if declared a check callback, the default check condition will ignore.
       // So you must check all condition
       check { result, exception, startTime, endTime ->
           if (exception != null) {
               throw exception
           }
           log.info("Stream load result: ${result}".toString())
           def json = parseJson(result)
           assertEquals("success", json.Status.toLowerCase())
           assertEquals(19, json.NumberTotalRows)
           assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
           assertTrue(json.LoadBytes > 0)
       }
   }
    sql "sync"
    qt_sql_select_all """
        SELECT * FROM test_decimal256_outfile_csv t order by 1,2,3;
    """

    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp/test_decimal256_outfile_csv_${uuid}"""
    try {
        logger.info("outfile: " + outFilePath)
        // check outfile
        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }
        sql """
            SELECT * FROM test_decimal256_outfile_csv t order by 1,2,3 INTO OUTFILE "file://${outFilePath}/" properties("column_separator" = ",");
        """
        File[] files = path.listFiles()
        assert files.length == 1
        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8);
        assert outLines.size() == 19
    } finally {
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }
}