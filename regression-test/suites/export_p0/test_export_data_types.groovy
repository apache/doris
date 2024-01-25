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

suite("test_export_data_types", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


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

    def table_export_name = "test_export_data_types"
    def table_load_name = "test_load_data_types"
    def outfile_path_prefix = """/tmp/test_export"""

    def create_table = {table_name -> 
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `user_id` INT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datev2` DATEV2 NOT NULL COMMENT "数据灌入日期时间2",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_1` DATETIMEV2 NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_2` DATETIMEV2(3) NOT NULL COMMENT "数据灌入日期时间",
            `datetimev2_3` DATETIMEV2(6) NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
            `street` STRING COMMENT "用户所在街道",
            `age` SMALLINT COMMENT "用户年龄",
            `sex` TINYINT COMMENT "用户性别",
            `bool_col` boolean COMMENT "",
            `int_col` int COMMENT "",
            `bigint_col` bigint COMMENT "",
            `largeint_col` largeint COMMENT "",
            `float_col` float COMMENT "",
            `double_col` double COMMENT "",
            `char_col` CHAR(10) COMMENT "",
            `decimal_col` decimal COMMENT "",
            `decimalv3_col` decimalv3 COMMENT "",
            `decimalv3_col2` decimalv3(1,0) COMMENT "",
            `decimalv3_col3` decimalv3(1,1) COMMENT "",
            `decimalv3_col4` decimalv3(9,8) COMMENT "",
            `decimalv3_col5` decimalv3(20,10) COMMENT "",
            `decimalv3_col6` decimalv3(38,0) COMMENT "",
            `decimalv3_col7` decimalv3(38,37) COMMENT "",
            `decimalv3_col8` decimalv3(38,38) COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """
    }

    create_table(table_export_name);

    StringBuilder sb = new StringBuilder()
    int i = 1
    sb.append("""
            (${i}, '2023-04-20', '2023-04-20', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
            'Beijing', 'Haidian',
            ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
            ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i}),
        """)

    sb.append("""
        (${++i}, '9999-12-31', '9999-12-31', '9999-12-31 23:59:59', '9999-12-31 23:59:59', '2023-04-20 00:00:00.12', '2023-04-20 00:00:00.3344',
        '', 'Haidian',
        ${Short.MIN_VALUE}, ${Byte.MIN_VALUE}, true, ${Integer.MIN_VALUE}, ${Long.MIN_VALUE}, -170141183460469231731687303715884105728, ${Float.MIN_VALUE}, ${Double.MIN_VALUE}, 'char${i}',
        100000000, 100000000, 4, 0.1, 0.99999999, 9999999999.9999999999, 99999999999999999999999999999999999999, 9.9999999999999999999999999999999999999, 0.99999999999999999999999999999999999999),
    """)
    
    sb.append("""
            (${++i}, '2023-04-21', '2023-04-21', '2023-04-20 12:34:56', '2023-04-20 00:00:00', '2023-04-20 00:00:00.123', '2023-04-20 00:00:00.123456',
            'Beijing', '', 
            ${Short.MAX_VALUE}, ${Byte.MAX_VALUE}, true, ${Integer.MAX_VALUE}, ${Long.MAX_VALUE}, 170141183460469231731687303715884105727, ${Float.MAX_VALUE}, ${Double.MAX_VALUE}, 'char${i}',
            999999999, 999999999, 9, 0.9, 9.99999999, 1234567890.0123456789, 12345678901234567890123456789012345678, 1.2345678901234567890123456789012345678, 0.12345678901234567890123456789012345678),
        """)

    sb.append("""
            (${++i}, '0000-01-01', '0000-01-01', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00', '2023-04-20 00:00:00',
            'Beijing', 'Haidian',
            ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}',
            ${i}, ${i}, ${i}, 0.${i}, ${i}, ${i}, ${i}, ${i}, 0.${i})
        """)

    
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
   
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """

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

    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
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

    // 1. test csv
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
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        
        // check file amounts
        check_file_amounts.call("${outFilePath}", 1)

        create_table(table_load_name);

        File[] files = new File("${outFilePath}").listFiles()
        String file_path = files[0].getAbsolutePath()
        streamLoad {
            table "${table_load_name}"

            set 'strict_mode', 'true'
            set 'format', 'csv'
            set 'column_separator', ','

            file "${file_path}"
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_select_load1 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 2. test parquet
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet"
            );
        """
        waiting_export.call(label)
        
        // check file amounts
        check_file_amounts.call("${outFilePath}", 1)

        create_table(table_load_name);

        File[] files = new File("${outFilePath}").listFiles()
        String file_path = files[0].getAbsolutePath()
        streamLoad {
            table "${table_load_name}"

            set 'strict_mode', 'true'
            set 'format', 'parquet'

            file "${file_path}"
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_select_load2 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 3. test orc
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc"
            );
        """
        waiting_export.call(label)
        
        // check file amounts
        check_file_amounts.call("${outFilePath}", 1)

        create_table(table_load_name);

        File[] files = new File("${outFilePath}").listFiles()
        String file_path = files[0].getAbsolutePath()
        streamLoad {
            table "${table_load_name}"

            set 'strict_mode', 'true'
            set 'format', 'orc'

            file "${file_path}"
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_select_load3 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 4. test csv_with_names
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        
        // check file amounts
        check_file_amounts.call("${outFilePath}", 1)

        create_table(table_load_name);

        File[] files = new File("${outFilePath}").listFiles()
        String file_path = files[0].getAbsolutePath()
        streamLoad {
            table "${table_load_name}"

            set 'strict_mode', 'true'
            set 'format', 'csv_with_names'
            set 'column_separator', ','

            file "${file_path}"
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_select_load4 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 5. test csv_with_names_and_types
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names_and_types",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        
        // check file amounts
        check_file_amounts.call("${outFilePath}", 1)

        create_table(table_load_name);

        File[] files = new File("${outFilePath}").listFiles()
        String file_path = files[0].getAbsolutePath()
        streamLoad {
            table "${table_load_name}"

            set 'strict_mode', 'true'
            set 'format', 'csv_with_names_and_types'
            set 'column_separator', ','

            file "${file_path}"
            time 10000 // limit inflight 10s

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(4, json.NumberTotalRows)
                assertEquals(0, json.NumberFilteredRows)
            }
        }

        qt_select_load5 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    try_sql("DROP TABLE IF EXISTS ${table_export_name}")
}
