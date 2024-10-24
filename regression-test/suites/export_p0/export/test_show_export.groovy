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

suite("test_show_export", "p0") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """


    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");


    def thisDb = "regression_test_export_p0_export"
    def table_export_name = "test_show_export"
    def table_load_name = "test_show_export_load_back"
    def outfile_path_prefix = """${bucket}/export/test_show_export/exp_"""

    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
    CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `user_id` INT NOT NULL COMMENT "用户id",
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
        `decimal_col` decimal COMMENT ""
        )
        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
    """
    StringBuilder sb = new StringBuilder()
    int i = 1
    for (; i < 100; i ++) {
        sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', 'Beijing', ${i}, ${i % 128}, true, ${i}, ${i}, ${i}, ${i}.${i}, ${i}.${i}, 'char${i}', ${i}),
        """)
    }
    sb.append("""
            (${i}, '2017-10-01', '2017-10-01 00:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
    sql """ INSERT INTO ${table_export_name} VALUES
            ${sb.toString()}
        """
    def insert_res = sql "show last insert;"
    logger.info("insert result: " + insert_res.toString())
    order_qt_select_export1 """ SELECT * FROM ${table_export_name} t ORDER BY user_id; """


    def waiting_export = { export_label ->
        while (true) {
            def res = sql """ show export where label = "${export_label}";"""
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                log.info("outfile_path: ${json.url[0][0]}")
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    }

    // 1. exec export 
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${outfile_path_prefix}_${uuid}"""
    def label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet",
                'columns' = 'user_id, date, datetime, city, age, sex, bool_col, int_col, bigint_col, float_col, double_col, char_col, decimal_col, largeint_col'
            )
            WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        order_qt_select_load1 """ select * from s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "parquet",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            ) ORDER BY user_id;
            """
    
    } finally {
    }


    // 2. exec export 
    uuid = UUID.randomUUID().toString()
    outFilePath = """${outfile_path_prefix}_${uuid}"""
    label = "label_${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} where user_id < 70 and user_id >= 20 TO "s3://${outFilePath}/"
            PROPERTIES(
                "label" = "${label}",
                "format" = "parquet",
                'columns' = 'user_id, date, datetime, city, age, sex, bool_col, int_col, bigint_col, float_col, double_col, char_col, decimal_col, largeint_col'
            )
            WITH S3(
                "s3.endpoint" = "${s3_endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key"="${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${getS3Provider()}"
            );
        """
        def outfile_url = waiting_export.call(label)
        
        order_qt_select_load1 """ select * from s3(
                "uri" = "http://${bucket}.${s3_endpoint}${outfile_url.substring(5 + bucket.length(), outfile_url.length() - 1)}0.parquet",
                "s3.access_key"= "${ak}",
                "s3.secret_key" = "${sk}",
                "format" = "parquet",
                "provider" = "${getS3Provider()}",
                "region" = "${region}"
            ) ORDER BY user_id;
            """
    
    } finally {
    }

    // test show export
    def show_export_res1 = sql_return_maparray "show export;"
    assertEquals(2, show_export_res1.size())

    // test: show proc
    def show_proc_jobs = sql_return_maparray """show proc "/jobs";"""
    def dbId = ""
    for (def row : show_proc_jobs) {
        if (row.DbName == "regression_test_export_p0_export") {
            dbId = row.DbId
            break
        }
    }
    // test: show proc "/jobs/${dbId}""
    def show_proc_jobs_DB = sql_return_maparray """show proc "/jobs/${dbId}";"""
    for (def row : show_proc_jobs) {
        if (row.JobType == "export") {
            assertEquals(2, row.Finished)
            assertEquals(2, row.Total)
            break
        }
    }
    // test: show proc "/jobs/${dbId}/export"
    def show_proc_jobs_dbid = sql_return_maparray """show proc "/jobs/${dbId}/export";"""
    assertEquals(2, show_proc_jobs_dbid.size())

    def show_export_res2 = sql_return_maparray "show export from ${thisDb};"
    assertEquals(2, show_export_res2.size())

    def show_export_res3 = sql_return_maparray "show export from ${thisDb} order by JobId;"
    assertEquals(2, show_export_res3.size())

    def show_export_res4 = sql_return_maparray "show export from ${thisDb} order by JobId, State;"
    assertEquals(2, show_export_res4.size())

    def show_export_res5 = sql_return_maparray "show export from ${thisDb} order by JobId, State limit 1"
    assertEquals(1, show_export_res5.size())

    def show_export_res6 = sql_return_maparray "show export from ${thisDb} order by JobId, State limit 1"
    assertEquals(1, show_export_res6.size())

    def jobId = show_export_res6[0].JobId
    def show_export_res7 = sql_return_maparray "show export from ${thisDb} where Id = ${jobId} order by JobId, State"
    assertEquals(1, show_export_res7.size())

    // test where label like
    def show_export_label_like = sql_return_maparray """show export from ${thisDb} where Label like "%${uuid}" """
    def show_export_label_eq = sql_return_maparray """show export from ${thisDb} where Label = "${label}" """
    assertEquals(show_export_label_like[0].JobId, show_export_label_eq[0].JobId)

    def show_export_res8 = sql_return_maparray """show export from ${thisDb} where STATE = "FINISHED" """
    assertEquals(2, show_export_res8.size())

    // test invalid where
    test {
        sql "show export from ${thisDb} where Progress = 100"
        // check exception
        exception """Where clause should looks like below"""
    }

}
