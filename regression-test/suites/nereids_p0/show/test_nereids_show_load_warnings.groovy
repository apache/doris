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

suite("test_nereids_show_load_warnings") {
    sql "drop database if exists  test_nereids_show_load_warnnings_db"
    sql "create database test_nereids_show_load_warnnings_db"
    sql "use test_nereids_show_load_warnnings_db;"
    sql "DROP TABLE IF EXISTS test_nereids_show_load_warningns_tbl "

    sql """
        CREATE TABLE IF NOT EXISTS test_nereids_show_load_warningns_tbl (
            `k1` int NULL,
            `k2` int NULL
        ) ENGINE=OLAP
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    if (enableHdfs()) {
        var base_path = uploadToHdfs("doc/data-operate/import/import-way/broker_load")
        def load_label = UUID.randomUUID().toString().replaceAll("-", "")
        sql """
                LOAD LABEL ${load_label}
                (
                    DATA INFILE("${base_path}/test_hdfs.txt")
                    INTO TABLE `test_nereids_show_load_warningns_tbl`
                    COLUMNS TERMINATED BY "\\t"
                    (k1,k2)
                )
                with HDFS
                (
                  "fs.defaultFS"="${getHdfsFs()}",
                  "hadoop.username" = "${getHdfsUser()}",
                  "hadoop.password" = "${getHdfsPasswd()}"
                )
                PROPERTIES
                (
                    "timeout"="1200",
                    "max_filter_ratio"="0.1"
                );
            """
        waitForBrokerLoadDone("${load_label}")

        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db limit 1")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where label = '${load_label}'")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where label = '${load_label}' limit 1")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where LOAD_JOB_ID = 123")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where LOAD_JOB_ID = 123 limit 1")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where LOAD_JOB_ID = 123 and label = '${load_label}'")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where LOAD_JOB_ID = 123 and label = '${load_label}' limit 1")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where JobId = 123")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where JobId = 123 limit 1")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where JobId = 123 and label = '${load_label}'")
        checkNereidsExecute("SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where JobId = 123 and label = '${load_label}' limit 1")

        def res1 = sql """SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db limit 1"""
        assertEquals("${load_label}", res1.get(0).get(1))

        def res2 = sql """SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where label = '${load_label}'"""
        assertEquals("${load_label}", res2.get(0).get(1))

        def jobid = res2.get(0).get(0)
        def res3 = sql """SHOW LOAD WARNINGS FROM test_nereids_show_load_warnnings_db where JobId = ${jobid}"""
        assertEquals(1, res3.size())
    }
    sql "DROP TABLE IF EXISTS test_nereids_show_load_warningns_tbl "
    sql "drop database if exists  test_nereids_show_load_warnnings_db"
}
