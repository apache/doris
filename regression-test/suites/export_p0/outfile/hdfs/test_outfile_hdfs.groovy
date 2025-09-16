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

suite("test_outfile_with_hdfs", "external,hive,external_docker") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        def table_export_name = "test_outfile_with_hdfs"
        // create table and insert
        sql """ DROP TABLE IF EXISTS ${table_export_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_export_name} (
            `id` int(11) NULL,
            `name` string NULL,
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
        for (; i < 10; i ++) {
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
        qt_select_export """ SELECT * FROM ${table_export_name} t ORDER BY id; """

        String hdfs_port = context.config.otherConfigs.get("hive2HdfsPort")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        // It's okay to use random `hdfsUser`, but can not be empty.
        def hdfsUserName = "doris"
        def defaultFS = "hdfs://${externalEnvIp}:${hdfs_port}"


        // test outfile
        def test_outfile = {format, uri ->
            def res = sql """
                SELECT * FROM ${table_export_name} t ORDER BY id
                INTO OUTFILE "${defaultFS}${uri}"
                FORMAT AS ${format}
                PROPERTIES (
                    "fs.defaultFS"="${defaultFS}",
                    "hadoop.username" = "${hdfsUserName}"
                );
            """

            def outfile_url = res[0][3]
            // check data correctness
            order_qt_select """ select * from hdfs(
                    "uri" = "${outfile_url}.${format}",
                    "hadoop.username" = "${hdfsUserName}",
                    "format" = "${format}");
                """
        }

        test_outfile('csv', '/tmp/ftw/export/exp_');
        test_outfile('parquet', '/tmp/ftw/export/exp_');
        test_outfile('orc', '/tmp/ftw/export/exp_');
        test_outfile('csv_with_names', '/tmp/ftw/export/exp_');
        test_outfile('csv_with_names_and_types', '/tmp/ftw/export/exp_');

        // test uri with multi '/'
        test_outfile('parquet', '//tmp/ftw/export/exp_');
        test_outfile('parquet', '//tmp//ftw/export/exp_');
        test_outfile('parquet', '//tmp/ftw/export//exp_');
        test_outfile('parquet', '//tmp/ftw//export//exp_');
        test_outfile('parquet', '//tmp/ftw//export/exp_');
        test_outfile('parquet', '///tmp/ftw/export/exp_');
        test_outfile('parquet', '////tmp/ftw/export/exp_');
    }
}
