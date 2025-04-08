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

suite("test_export_view", "p0") {
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

    def machine_user_name = "root"
    def check_path_exists = { dir_path ->
        mkdirRemotePathOnAllBE(machine_user_name, dir_path)
    }
    def delete_files = { dir_path ->
        deleteRemotePathOnAllBE(machine_user_name, dir_path)
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
    def create_load_table = {table_name ->
        sql """ DROP TABLE IF EXISTS ${table_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            `s1` varchar NULL,
            `k1` int(11) NULL,
            `k2` int(11) NULL,
            `k3` int(11) NULL
            )
            DISTRIBUTED BY HASH(s1)
            PROPERTIES("replication_num" = "1");
        """
    }

    def table_export_name = "test_export_base_table"
    def table_export_view_name = "test_export_view_table"
    def table_load_name = "test_load_view_basic"
    def outfile_path_prefix = """/tmp/test_export_view"""
    def local_tvf_prefix = "tmp/test_export_view"

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `k1` int(11) NULL,
        `k2` string NULL,
        `k3` int(11) NULL,
        `v1` int(11) NULL
        )
        PARTITION BY RANGE(k1)
        (
            PARTITION less_than_20 VALUES LESS THAN ("20"),
            PARTITION between_20_70 VALUES [("20"),("70")),
            PARTITION more_than_70 VALUES LESS THAN ("151")
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 150; i ++) {
        if (i < 50) {
            sb.append("""
                    (${i}, 'zhangsan', ${i + 18}, ${i}),
                """)
        } else if (i < 80) {
            sb.append("""
                    (${i}, 'lisi', ${i + 18}, ${i}),
                """)
        } else if (i < 120) {
            sb.append("""
                    (${i}, 'wangwu', ${i + 18}, ${i}),
                """)
        } else {
            sb.append("""
                    (${i}, 'fangfang', ${i + 18}, ${i}),
                """)
        }
    }
    sb.append("""
            (${i}, 'xiexie', NULL, ${i})
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    qt_select_export """ SELECT * FROM ${table_export_name} t ORDER BY k1; """


    sql """ DROP VIEW IF EXISTS ${table_export_view_name} """
    sql """
    CREATE VIEW ${table_export_view_name}
        (
            s1 COMMENT "first key",
            k1 COMMENT "second key",
            k2 COMMENT "third key",
            k3 COMMENT "first value"
        )
        COMMENT "my first view"
        AS
        SELECT k2, min(k1), max(k3), SUM(v1) FROM ${table_export_name}
        WHERE k1 > 30 GROUP BY k2;
    """

    // 1. basic test
    def uuid = UUID.randomUUID().toString()
    def outFilePath = "${outfile_path_prefix}/${table_export_view_name}_${uuid}"
    def label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        

        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "format" = "csv",
                    "column_separator" = ","
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load1 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    // 2. test csv_with_names
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        

        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "column_separator" = ",",
                    "format" = "csv_with_names"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load2 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }

    
    // 3. test where clause
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        

        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "column_separator" = ",",
                    "format" = "csv_with_names"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load3 """ SELECT * FROM ${table_load_name}; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 4. test where clause and columns property
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "columns" = "k3, s1, k1",
                "column_separator"=","
            );
        """

        waiting_export.call(label)
        
        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select s1,k1, null as k2,k3 from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "column_separator" = ",",
                    "format" = "csv_with_names"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load4 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 5. test csv_with_names_and_types
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv_with_names_and_types",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "column_separator"=","
            );
        """
        waiting_export.call(label)
        
        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "column_separator" = ",",
                    "format" = "csv_with_names_and_types"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load5 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 6. test orc type
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc",
                "max_file_size" = "512MB"
            );
        """
        waiting_export.call(label)
        
        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select * from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "format" = "orc"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load6 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 8. test orc type, where clause and columns property
    uuid = UUID.randomUUID().toString()
    outFilePath = "${outfile_path_prefix}" + "/${table_export_view_name}_${uuid}"
    label = "label_${uuid}"
    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} where s1 = 'fangfang' OR k1 = 31
            TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "orc",
                "max_file_size" = "512MB",
                "parallelISM" = "5",
                "columns" = "k3, s1, k1"
            );
        """

        waiting_export.call(label)
        // check data correctness
        create_load_table(table_load_name)

        // use local() tvf to reload the data
        def ipList = [:]
        def portList = [:]
        getBackendIpHeartbeatPort(ipList, portList)
        ipList.each { beid, ip ->
            logger.info("Begin to insert into ${table_load_name} from local()")
            sql """
                insert into ${table_load_name}
                select s1,k1, null as k2,k3 from local(
                    "file_path" = "${local_tvf_prefix}/${table_export_view_name}_${uuid}/*",
                    "backend_id" = "${beid}",
                    "format" = "orc"
                );         
            """ 
            insert_res = sql "show last insert;"
            logger.info("insert from local(), BE id = ${beid}, result: " + insert_res.toString())
        }

        order_qt_select_load8 """ SELECT * FROM ${table_load_name} t; """
    
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
        delete_files.call("${outFilePath}")
    }


    // 7. test parquet type use s3
    uuid = UUID.randomUUID().toString()
    label = "label_${uuid}"
    try {

        String ak = getS3AK()
        String sk = getS3SK()
        String s3_endpoint = getS3Endpoint()
        String region = getS3Region()
        String bucket = context.config.otherConfigs.get("s3BucketName");

        outFilePath = """${bucket}/export/p0/view/parquet"""

        // exec export
        sql """
            EXPORT TABLE ${table_export_view_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet"
            )
            WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """

        def outfile_url = ""
        while (true) {
            def res = sql """ show export where label = "${label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                log.info("outfile_path: ${json.url[0][0]}")
                outfile_url = json.url[0][0];
                break;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }

        order_qt_select_load7 """ select * from s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "parquet",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            );
            """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_load_name}")
    }
}
