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

suite("test_export_table_with_label_retry", "p0") {
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

    def table_export_name = "test_export_table_with_label_retry"
    def table_load_name = "test_load_table_with_label_retry"
    def wrong_outfile_path_prefix = """mnt/disk2/ftw/projects/doris/output/be"""
    def outfile_path_prefix = """/tmp/test_export_table_with_label_retry"""
    def local_tvf_prefix = "tmp/test_export_table_with_label_retry"

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `user_id` LARGEINT NOT NULL COMMENT "用户id",
        `date` DATE NOT NULL COMMENT "数据灌入日期时间",
        `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
        `city` VARCHAR(20) COMMENT "用户所在城市",
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
        `ipv4_col` ipv4 COMMENT "",
        `ipv6_col` ipv6 COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 100; i ++) {
        sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i}, '0.0.0.${i}', '::${i}'),
        """)
    }
    sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """

    def machine_user_name = "root"
    def check_path_exists = { dir_path ->
        mkdirRemotePathOnAllBE(machine_user_name, dir_path)
    }

    def delete_files = { dir_path ->
        deleteRemotePathOnAllBE(machine_user_name, dir_path)
    }

    def waiting_export_expect_failed = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                throw new IllegalStateException("""export finished is unexpected""")
            } else if (res[0][2] == "CANCELLED") {
                break
            } else {
                sleep(5000)
            }
        }
    }

    def waiting_export_expect_success = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            assert res.size() == 2
            logger.info("export state: " + res[1][2])
            if (res[1][2] == "FINISHED") {
                break;
            } else if (res[1][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[1][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def uuid = UUID.randomUUID().toString()
    def outFilePath = "${outfile_path_prefix}" + "/${table_export_name}_${uuid}"
    def wrongFilePath = "${wrong_outfile_path_prefix}" + "/${table_export_name}_${uuid}"
    def label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec wrong export
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${wrongFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=","
            );
        """
        waiting_export_expect_failed.call(label)

        // exec right export with same label again
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=","
            );
        """

        waiting_export_expect_success.call(label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `user_id` LARGEINT NOT NULL COMMENT "用户id",
            `date` DATE NOT NULL COMMENT "数据灌入日期时间",
            `datetime` DATETIME NOT NULL COMMENT "数据灌入日期时间",
            `city` VARCHAR(20) COMMENT "用户所在城市",
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
            `ipv4_col` ipv4 COMMENT "",
            `ipv6_col` ipv6 COMMENT ""
            )
            DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
        """

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "column_separator" = ",",
                    "format" = "csv");         
                """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load1 """ SELECT * FROM ${table_load_name} t ORDER BY user_id; """

    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }
    try_sql("DROP TABLE IF EXISTS ${table_export_name}")
}
