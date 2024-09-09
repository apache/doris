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

suite("test_export_with_parallelism", "p2") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def table_export_name = "test_export_with_parallelism"
    def table_load_name = "test_load_with_parallelism"


    // create table and insert data
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `id` int(11) NULL,
        `name` string NULL,
        `age` int(11) NULL
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 100; i ++) {
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


    def waiting_export = { export_label, parallelism ->
        while (true) {
            def res = sql """ show export where label = "${export_label}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List

                // because parallelism = min(parallelism, tablets_num)
                assertEquals(Integer.min(3, parallelism), json.size())

                List<String> resultUrl = new ArrayList<String>();
                // because parallelism = min(parallelism, tablets_num)
                for (int idx = 0; idx < parallelism && idx < 3; ++idx) {
                    assertEquals("1", json.fileNumber[idx][0])
                    log.info("outfile_path: ${json.url[idx][0]}")
                    resultUrl.add(json.url[idx][0])
                }

                return resultUrl;
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    def outFilePath = """${bucket}/export/exp_"""

    def test_export = {format, file_suffix, isDelete, parallelism ->
        def uuid = UUID.randomUUID().toString()
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}"
            PROPERTIES(
                "label" = "${uuid}",
                "format" = "${format}",
                "parallelism" = "${parallelism}",
                "data_consistency" = "none",
                "delete_existing_files"="${isDelete}"
            )
            WITH s3 (
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """

        def outfile_url_list = waiting_export.call(uuid, parallelism)

        // create table and insert
        sql """ DROP TABLE IF EXISTS ${table_load_name} """
        sql """
        CREATE TABLE IF NOT EXISTS ${table_load_name} (
            `id` int(11) NULL,
            `name` string NULL,
            `age` int(11) NULL
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES("replication_num" = "1");
        """

        // because parallelism = min(parallelism, tablets_num)
        for (int j = 0; j < parallelism && j < 3; ++j) {
            // check data correctness
            sql """ insert into ${table_load_name}
                        select * from s3(
                        "uri" = "http://${bucket}.${s3_endpoint}${outfile_url_list.get(j).substring(5 + bucket.length())}.${file_suffix}",
                        "s3.access_key"= "${ak}",
                        "s3.secret_key" = "${sk}",
                        "format" = "${format}",
                        "region" = "${region}",
                        "provider" = "${getS3Provider()}",
                        "use_path_style" = "false" -- aliyun does not support path_style
                );
                """
        }

        order_qt_select """ select * from ${table_load_name}; """
    }

    // parallelism = 2
    test_export('csv', 'csv', true, 2);
    test_export('parquet', 'parquet', true, 2);
    test_export('orc', 'orc', true, 2);
    test_export('csv_with_names', 'csv', true, 2);
    test_export('csv_with_names_and_types', 'csv', true, 2);


    // parallelism = 3
    test_export('csv', 'csv', true, 3);
    test_export('parquet', 'parquet', true, 3);
    test_export('orc', 'orc', true, 3);
    test_export('csv_with_names', 'csv', true, 3);
    test_export('csv_with_names_and_types', 'csv', true, 3);

    // parallelism = 4
    test_export('csv', 'csv', true, 4);
    test_export('parquet', 'parquet', true, 4);
    test_export('orc', 'orc', true, 4);
    test_export('csv_with_names', 'csv', true, 4);
    test_export('csv_with_names_and_types', 'csv', true, 4);
}
