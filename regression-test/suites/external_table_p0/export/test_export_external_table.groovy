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

suite("test_export_external_table", "p0,external,mysql,external_docker,external_docker_mysql") {
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

    def check_path_exists = { dir_path ->
        List<List<Object>> backends =  sql """ show backends """
        assertTrue(backends.size() > 0)
        File path = new File(dir_path)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${dir_path} already exists! """)
        }
        if (backends.size() > 1) {
            for (List<Object> backend : backends) {
                def be_host = backend[1]
                def cmd="""mkdir -p ${dir_path}"""
                sshExec("root", be_host, cmd.toString())
            }
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
    

    // this table name must be `test1`, because this is an external table.
    def table_export_name = "test1"
    def table_load_name = "test_load_external__basic"
    def outfile_path_prefix = """/tmp/test_export"""

    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "mysql_jdbc_catalog";
        String internal_db_name = "regression_test_external_table_p0_export";
        String ex_db_name = "doris_test";
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        def create_load_table = {table_name ->
            sql """ DROP TABLE IF EXISTS internal.${internal_db_name}.${table_name} """
            sql """
            CREATE TABLE IF NOT EXISTS internal.${internal_db_name}.${table_name} (
                    k1 boolean,
                    k2 char(100),
                    k3 varchar(128),
                    k4 date,
                    k5 float,
                    k6 tinyint,
                    k7 smallint,
                    k8 int,
                    k9 bigint,
                    k10 double,
                    k11 datetime,
                    k12 decimal(10, 3)
                )
                DISTRIBUTED BY HASH(k8)
                PROPERTIES("replication_num" = "1");
            """
        }

        sql """create database if not exists ${internal_db_name}; """

        sql """drop catalog if exists ${catalog_name} """
        sql """create catalog if not exists ${catalog_name} properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false&zeroDateTimeBehavior=convertToNull",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql  """ use ${internal_db_name} """

        qt_sql """select current_catalog()"""
        sql """switch ${catalog_name}"""
        qt_sql """select current_catalog()"""
        sql """ use ${ex_db_name}"""

        order_qt_export_table """ select * from ${table_export_name} where k8 < 100 order by k8; """

        // 1. basic test
        def uuid = UUID.randomUUID().toString()
        def outFilePath = """${outfile_path_prefix}_${uuid}"""
        def label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 100
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv",
                    "column_separator"=","
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

            File[] files = new File("${outFilePath}").listFiles()
            String file_path = files[0].getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
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
                    assertEquals(100, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load1 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }

        // 2. export external table under internal catalog
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")
            
            sql """ switch internal """
            // exec export
            sql """
                EXPORT TABLE ${catalog_name}.${ex_db_name}.${table_export_name} where k8 < 100
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv",
                    "column_separator"=","
                );
            """

            sql """ switch ${catalog_name} """
            sql """ use ${ex_db_name} """

            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

            File[] files = new File("${outFilePath}").listFiles()
            String file_path = files[0].getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
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
                    assertEquals(100, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load2 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }

        sql """ switch ${catalog_name} """
        // 3. csv_with_names
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names",
                    "column_separator"=","
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

            File[] files = new File("${outFilePath}").listFiles()
            String file_path = files[0].getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
                set 'strict_mode', 'true'
                set 'format', 'csv_with_names'

                file "${file_path}"
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(30, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load3 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }


        // 4. csv_with_names_and_types
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names_and_types",
                    "column_separator"=","
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

            File[] files = new File("${outFilePath}").listFiles()
            String file_path = files[0].getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
                set 'strict_mode', 'true'
                set 'format', 'csv_with_names_and_types'

                file "${file_path}"
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(30, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load4 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }


        // 5. orc
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "orc"
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

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
                    assertEquals(30, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load5 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }


        // 5. parquet
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "parquet"
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

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
                    assertEquals(30, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load6 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }


        // 7. test columns property
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}_${uuid}"""
        label = "label_${uuid}"
        try {
            // check export path
            check_path_exists.call("${outFilePath}")

            // exec export
            sql """
                EXPORT TABLE ${table_export_name} where k8 < 30
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv_with_names",
                    "columns" = "k8, k1, k5, k3, k7",
                    "column_separator"=","
                );
            """
            waiting_export.call(label)
            
            // check file amounts
            check_file_amounts.call("${outFilePath}", 1)

            // check data correctness
            create_load_table(table_load_name)

            File[] files = new File("${outFilePath}").listFiles()
            String file_path = files[0].getAbsolutePath()
            streamLoad {
                table "${table_load_name}"

                set 'column_separator', ','
                set 'strict_mode', 'true'
                set 'format', 'csv_with_names'
                set 'columns', 'k8, k1, k5, k3, k7'

                file "${file_path}"
                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(30, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }

            order_qt_select_load7 """ SELECT * FROM internal.${internal_db_name}.${table_load_name} order by k8; """
        
        } finally {
            delete_files.call("${outFilePath}")
        }
    }
}
