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

suite("test_broker_load_func", "p0") {

    if (enableHdfs()) {
        try {
            String database_name = "test_broker_load_func"
            String broker_name = getBrokerName()
            String hdfsUser = getHdfsUser()
            String hdfsPasswd = getHdfsPasswd()
            def uuid = UUID.randomUUID().toString().replaceAll("-", "")
            def test_load_label="label_test_broker_load_func_${uuid}"
            String table_name="simple"
            def hdfs_csv_file_path = uploadToHdfs "external_table_p0/broker_load/test_broker_load_func.csv"

            sql """drop database if exists ${database_name}; """
            sql """create database if not exists ${database_name};"""
            sql """use ${database_name}; """

            sql """
                create table ${table_name} (
                    `t_empty_string`  varchar(255) NULL COMMENT '',
                    `t_string` varchar(255) NULL COMMENT ''
                ) engine=olap
                distributed by hash(t_empty_string) buckets 1
                properties (
                    "replication_num" = "1"
                );
                """

            sql """
                LOAD LABEL ${database_name}.${test_load_label}
                (
                    DATA INFILE("${hdfs_csv_file_path}")
                    INTO TABLE ${table_name}
                    COLUMNS TERMINATED BY ","
                )
                WITH BROKER "${broker_name}"
                (
                     "username"="${hdfsUser}",
                     "password"="${hdfsPasswd}"
                );
            """

            def check_load_result = {checklabel, testTablex ->
                def max_try_milli_secs = 600000
                while(max_try_milli_secs) {
                    def result = sql "show load where label = '${checklabel}'"
                    if(result[0][2] == "FINISHED") {
                        //sql "sync"
                        def res = sql "select count(*) from ${database_name}.${testTablex};"
                        assertEquals(10,res[0][0])
                        break
                    } else {
                        sleep(1000) // wait 1 second every time
                        max_try_milli_secs -= 1000
                        if(max_try_milli_secs <= 0) {
                            assertEquals(1, 2)
                        }
                    }
                }
            }

            check_load_result.call(test_load_label, table_name)

        } finally {
        }
    }
}
