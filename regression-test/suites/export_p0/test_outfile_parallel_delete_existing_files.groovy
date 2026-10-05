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

suite("test_outfile_parallel_delete_existing_files", "p0") {
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        strBuilder.append(" https://" + context.config.feHttpAddress + "/rest/v1/config/fe")
        strBuilder.append(" --cert " + context.config.otherConfigs.get("trustCert")
                + " --cacert " + context.config.otherConfigs.get("trustCACert")
                + " --key " + context.config.otherConfigs.get("trustCAKey"))
    } else {
        strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")
    }

    def process = strBuilder.toString().execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    boolean enableDeleteExistingFiles = false
    for (Object conf : response.data.rows) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "enable_delete_existing_files") {
            enableDeleteExistingFiles = ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
        }
    }
    if (!enableDeleteExistingFiles) {
        logger.warn("Please set enable_delete_existing_files to true to run test_outfile_parallel_delete_existing_files")
        return
    }

    String ak = getS3AK()
    String sk = getS3SK()
    String s3Endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName")
    String provider = getS3Provider()
    String tableName = "test_outfile_parallel_delete_existing_files"
    String uuid = UUID.randomUUID().toString()
    String outFilePath = "${bucket}/outfile/parallel_delete_existing_files/${uuid}/exp_"

    def exportToS3 = { String filterSql, boolean deleteExistingFiles ->
        String deleteProperty = deleteExistingFiles ? "\"delete_existing_files\" = \"true\"," : ""
        sql """
            SELECT * FROM ${tableName} ${filterSql}
            INTO OUTFILE "s3://${outFilePath}"
            FORMAT AS csv
            PROPERTIES (
                ${deleteProperty}
                "column_separator" = ",",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key" = "${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${provider}"
            );
        """
    }

    try {
        sql """ set enable_parallel_outfile = true """
        sql """ set parallel_pipeline_task_num = 8 """

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                id INT NOT NULL,
                name STRING NOT NULL,
                score INT NOT NULL
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 16
            PROPERTIES("replication_num" = "1");
        """

        sql """
            INSERT INTO ${tableName}
            SELECT
                number AS id,
                concat('name_', cast(number AS string)) AS name,
                cast(number % 97 AS int) AS score
            FROM numbers("number" = "20000");
        """

        def expected = sql """ SELECT count(*), sum(id), sum(score) FROM ${tableName}; """

        exportToS3("WHERE id < 5000", false)
        exportToS3("", true)

        def actual = sql """
            SELECT count(*), sum(id), sum(score) FROM S3(
                "uri" = "s3://${outFilePath}*",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${region}",
                "s3.secret_key" = "${sk}",
                "s3.access_key" = "${ak}",
                "provider" = "${provider}",
                "format" = "csv",
                "column_separator" = ",",
                "csv_schema" = "id:int;name:string;score:int"
            );
        """

        assertEquals(expected[0][0], actual[0][0])
        assertEquals(expected[0][1], actual[0][1])
        assertEquals(expected[0][2], actual[0][2])
    } finally {
        try_sql(""" set enable_parallel_outfile = false """)
    }
}
