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

suite("test_export_basic", "p0") {
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

    
    def table_export_name = "test_export_basic"
    def table_load_name = "test_load_basic"
    def outfile_path_prefix = """/tmp/test_export_basic"""
    def local_tvf_prefix = "tmp/test_export_basic"

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `id` int(11) NULL,
        `Name` string NULL,
        `age` int(11) NULL
        )
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


    def machine_user_name = "root"
    def check_path_exists = { dir_path ->
        mkdirRemotePathOnAllBE(machine_user_name, dir_path)
    }

    def delete_files = { dir_path ->
        deleteRemotePathOnAllBE(machine_user_name, dir_path)
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

    def waiting_export_with_exception = { the_db, export_label, exception_msg ->
        while (true) {
            def res = sql """ show export from ${the_db} where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                throw new IllegalStateException("""export finished, do not contains exception: ${exception_msg}""")
            } else if (res[0][2] == "CANCELLED") {
                assertTrue(res[0][10].contains("${exception_msg}"))
                break;
            } else {
                sleep(5000)
            }
        }
    }

    // 1. basic test
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
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
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `Name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
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
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load1 """ SELECT * FROM ${table_load_name} t ORDER BY id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 2. test partition1
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (less_than_20)
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `Name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
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
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load2 """ SELECT * FROM ${table_load_name} t ORDER BY id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 3. test partition2
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (between_20_70)
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `Name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
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
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load3 """ SELECT * FROM ${table_load_name} t ORDER BY id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 4. test partition3 and where clause
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (more_than_70) where id >100
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `Name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
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
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load4 """ SELECT * FROM ${table_load_name} t ORDER BY id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 5. test order by and limit clause
    def uuid1 = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    def label1 = "label_${uuid1}"
    def uuid2 = UUID.randomUUID().toString()
    def label2 = "label_${uuid2}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (less_than_20)
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label1}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (between_20_70)
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label2}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label1)
        waiting_export.call(db, label2)

        // check show export correctness
        def res = sql """ show export where STATE = "FINISHED" order by JobId desc limit 2"""
        assertTrue(res[0][0] > res[1][0])

    } finally {
        delete_files.call("${outFilePath}")
    }


    // 6. test columns property
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (more_than_70) where age >100
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "columns" = "id, Name",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `Name` string NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name} (id, Name)
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load6 """ SELECT * FROM ${table_load_name} t ORDER BY id; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 7. test columns property 2
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_name} PARTITION (more_than_70) where age >100
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "columns" = "id",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(db, label)

        // check data correctness
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
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
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        qt_select_load7 """ SELECT * FROM ${table_load_name} t ORDER BY id; """

        // test label
        def label_db = "export_p0_test_label"
        sql """ DROP DATABASE IF EXISTS ${label_db}"""
        sql """ CREATE DATABASE ${label_db}"""
        sql """
        CREATE TABLE IF NOT EXISTS ${label_db}.${table_load_name} (
            `id` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        sql """insert into ${label_db}.${table_load_name} values(1)""";

        // 1. first export
        uuid = UUID.randomUUID().toString()
        outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
        label = "label_${uuid}"
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${label_db}.${table_load_name}
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(label_db, label)

        // 2. use same label again
        test {
            sql """
                EXPORT TABLE ${label_db}.${table_load_name}
                TO "file://${outFilePath}/"
                PROPERTIES(
                    "label" = "${label}",
                    "format" = "csv",
                    "column_separator"=",",
                    "data_consistency" = "none"
                );
            """
            exception "has already been used"
        }

        // 3. drop database and create again
        sql """ DROP DATABASE IF EXISTS ${label_db}"""
        sql """ CREATE DATABASE ${label_db}"""
        sql """
        CREATE TABLE IF NOT EXISTS ${label_db}.${table_load_name} (
            `id` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
        """
        sql """insert into ${label_db}.${table_load_name} values(1)""";
        
        // 4. exec export using same label
        sql """
            EXPORT TABLE ${label_db}.${table_load_name}
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call(label_db, label)

        // test illegal column names
        uuid = UUID.randomUUID().toString()
        label = "label_${uuid}"

        sql """
            EXPORT TABLE ${label_db}.${table_load_name}
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "columns" = "col1",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export_with_exception(label_db, label, "Unknown column");
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }
}
