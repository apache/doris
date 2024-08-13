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

suite("test_export_big_data", "p2") {
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

    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");
    

    def delete_files = { dir_path ->
        File path = new File(dir_path)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete();
            }
            path.delete();
        }
    }


    def table_export_name = "test_export_big_data"
    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
        CREATE TABLE ${table_export_name} (
        `user_id` largeint(40) NOT NULL COMMENT 'id',
        `date` date NOT NULL,
        `datetime` datetime NOT NULL,
        `city` varchar(20) NULL,
        `age` int(11) NULL,
        `sex` int(11) NULL,
        `bool_col` boolean NULL,
        `int_col` int(11) NULL,
        `bigint_col` bigint(20) NULL,
        `largeint_col` largeint(40) NULL,
        `float_col` float NULL,
        `double_col` double NULL,
        `char_col` char(10) NULL,
        `decimal_col` DECIMAL NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`user_id`, `date`, `datetime`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );  
    """

    sql """ INSERT INTO ${table_export_name} select * from s3(
            "uri" = "https://${bucket}.${s3_endpoint}/regression/export_p2/export_orc/test_export_big_data_dataset.orc",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "provider" = "${getS3Provider()}",
            "format" = "orc");
        """

    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp"""

    // exec export
    sql """
        EXPORT TABLE ${table_export_name} TO "file://${outFilePath}/"
        PROPERTIES(
            "label" = "${uuid}",
            "format" = "orc"
        );
    """

    try {
        while (true) {
            def res = sql """ show export where label = "${uuid}" """
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
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_export_name}")
        delete_files.call("${outFilePath}")
    }
}
