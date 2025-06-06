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

suite("test_nereids_show_export") {
    sql """ drop database if exists  test_nereids_show_export_db"""
    sql """create database test_nereids_show_export_db"""
    sql """use test_nereids_show_export_db;"""
    sql """ DROP TABLE IF EXISTS test_nereids_show_export """

    sql """
        CREATE TABLE IF NOT EXISTS test_nereids_show_export (
            `k1` bigint(20) NULL,
            `k2` bigint(20) NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """ insert into test_nereids_show_export values (1,1),(2,2); """

    // export config
    def table_export_name = "test_nereids_show_export"
    def outfile_path_prefix = """/tmp/test_export_basic"""
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}/${table_export_name}_${uuid}"""
    def label = "label_xxyyzz_1"
    def label1 = "label_xxyyzz_2"
    def label2 = "label_xxyyzz_3"
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

    try {
        // check export path
        check_path_exists.call("${outFilePath}")

        // exec export 1
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call("test_nereids_show_export_db", label)

        // exec export 2
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label1}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call("test_nereids_show_export_db", label1)

        // exec export 3
        sql """
            EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
            PROPERTIES(
                "label" = "${label2}",
                "format" = "csv",
                "column_separator"=",",
                "data_consistency" = "none"
            );
        """
        waiting_export.call("test_nereids_show_export_db", label2)

        // test show
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db order by Label")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db order by Label limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123 limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123 order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123 order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123 order by Label")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where id = 123 order by Label limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123 limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123 order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123 order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123 order by Label")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where JobId = 123 order by Label limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%'")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%' limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%' order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%' order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%' order by Label")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label like 'F%' order by Label limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED'")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by state")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by state limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx'")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx' limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx' order by JobId")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx' order by JobId limit 1")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx' order by CreateTime")
        checkNereidsExecute("SHOW EXPORT FROM test_nereids_show_export_db where label = 'xxx' order by CreateTime limit 1")

        def res1 = sql """SHOW EXPORT FROM test_nereids_show_export_db"""
        assertEquals(3, res1.size())
        def res2 = sql """SHOW EXPORT FROM test_nereids_show_export_db order by JobId"""
        assertEquals(3, res2.size())
        def res3 = sql """SHOW EXPORT FROM test_nereids_show_export_db order by JobId limit 1"""
        assertEquals(1, res3.size())
        def res4 = sql """SHOW EXPORT FROM test_nereids_show_export_db where label = 'label_xxyyzz_2'"""
        assertEquals(1, res4.size())
        assertEquals("label_xxyyzz_2", res4.get(0).get(1))
        def res5 = sql """SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED'"""
        assertEquals(3, res5.size())
        def res6 = sql """SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by label"""
        assertEquals(3, res6.size())
        assertEquals("label_xxyyzz_1", res6.get(0).get(1))
        def res7 = sql """SHOW EXPORT FROM test_nereids_show_export_db where state = 'FINISHED' order by label desc"""
        assertEquals(3, res7.size())
        assertEquals("label_xxyyzz_3", res7.get(0).get(1))
        def res8 = sql """SHOW EXPORT FROM test_nereids_show_export_db where LABEL like 'label_xxyyzz_%'"""
        assertEquals(3, res8.size())
        def res9 = sql """SHOW EXPORT FROM test_nereids_show_export_db where LABEL like 'label_xxyyzz_%' order by LABEL limit 1"""
        assertEquals(1, res9.size())
        assertEquals("label_xxyyzz_1", res9.get(0).get(1))
        def jobid = res2.get(0).get(0)
        def res10 = sql """SHOW EXPORT FROM test_nereids_show_export_db where jobid = ${jobid}"""
        assertEquals(1, res10.size())
    } finally {
        try_sql("DROP TABLE IF EXISTS test_nereids_show_export")
        try_sql("DROP DATABASE IF EXISTS test_nereids_show_export_db")
        delete_files.call("${outFilePath}")
    }
}
