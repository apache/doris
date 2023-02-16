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

suite("test_map_load_and_function", "p0") {
    // define a sql table
    def testTable = "tbl_test_map"
    def dataFile = "test_map.csv"

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

    // load the map data from csv file
    streamLoad {
        table testTable
        
        file dataFile // import csv file
        time 10000 // limit inflight 10s

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals("OK", json.Message)
            assertEquals(15, json.NumberTotalRows)
            assertTrue(json.LoadBytes > 0)

        }
    }

    // check result
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES(12, NULL)"""
    sql """INSERT INTO ${testTable} VALUES(13, {"k1":100, "k2": 130})"""

    // map element_at
    qt_select "SELECT m['k2'] FROM ${testTable}"

    // map select into outfile
    // check outfile
    def outFilePath = """${context.file.parent}/tmp"""
    logger.warn("test_map_selectOutFile the outFilePath=" + outFilePath)

    File path = new File(outFilePath)
    if (path.exists()) {
        for (File f: path.listFiles()) {
            f.delete();
        }
        path.delete();
    }
    if (!path.exists()) {
        assert path.mkdirs()
    }
    sql """
                SELECT * FROM ${testTable} INTO OUTFILE "file://${outFilePath}/";
    """
    File[] files = path.listFiles()
    assert files.length == 1
}
