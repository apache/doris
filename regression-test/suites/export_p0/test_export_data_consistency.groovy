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

suite("test_export_data_consistency", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

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

    def table_export_name = "test_export_data_consistency"
    def table_load_name = "test_load_data_consistency"
    def outfile_path_prefix = """/tmp/test_export_data_consistency"""

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `id` int(11) NULL,
        `name` string NULL,
        `age` int(11) NULL
        )
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(id)
        (
            PARTITION less_than_20 VALUES LESS THAN ("20"),
            PARTITION between_20_70 VALUES [("20"),("70")),
            PARTITION more_than_70 VALUES LESS THAN ("151")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 150; i ++) {
        sb.append("""
            (${i}, 'ftw-${i}', ${i + 18}),
        """)
    }
    sb.append("""
            (${i}, NULL, NULL)
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export """ SELECT * FROM ${table_export_name} t ORDER BY id; """


    def check_path_exists = { dir_path ->
        File path = new File(dir_path)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${dir_path} already exists! """)
        }
    }

    def check_file_amounts = { dir_path, amount ->
        File path = new File(dir_path)
        File[] files = path.listFiles()
        assert files.length == amount
    }

    def delete_files = { dir_path ->
        File path = new File(dir_path)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }

    def waiting_export = { the_db, export_label ->
        while (true) {
            def res = sql """ show export from ${the_db} where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                break;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // 1. basic test
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator" = ",",
                "parallelism" = "10"
            );
        """
        // do insert in parallel
        // [0, 20), [20, 70), [70, +inf)
        // The export task should keep partition consistency.
        sql """INSERT INTO ${table_export_name} VALUES
            (10, 'test', 11),
            (15, 'test', 11),
            (30, 'test', 21),
            (40, 'test', 51),
            (80, 'test', 51),
            (90, 'test', 51)
            """

        // wait export
        waiting_export.call(db, label)

        // check file amounts
        check_file_amounts.call("${outFilePath}", 3)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """

        File[] files = new File("${outFilePath}").listFiles()
        for (exportLoadFile in files) {
            String file_path = exportLoadFile.getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
                set 'columns', 'id, name, age'
                set 'strict_mode', 'true'

                file "${file_path}"
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
        }

        // The partition ranges are:
        // [0, 20), [20, 70), [70, +inf)
        // The export task should keep partition consistency.
        def result = sql """ SELECT * FROM ${table_load_name} t WHERE id in (10,15,30,40,80,90) ORDER BY id; """
        logger.info("result ${result}")
        assert result.size == 6
        if (result[0][1] == 'test') {
            assert result[1][1] == 'test'
        } else {
            assert result[0][1] == 'ftw-10'
            assert result[1][1] == 'ftw-15'
        }
        if (result[2][1] == 'test') {
            assert result[3][1] == 'test'
        } else {
            assert result[2][1] == 'ftw-30'
            assert result[3][1] == 'ftw-40'
        }
        if (result[4][1] == 'test') {
            assert result[5][1] == 'test'
        } else {
            assert result[4][1] == 'ftw-80'
            assert result[5][1] == 'ftw-90'
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }
}
