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

suite("test_export_big_data", "p2") {
    String ak = getS3AK()
    String sk = getS3SK()
    String s3_endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = context.config.otherConfigs.get("s3BucketName");

    def table_export_name = "test_export_big_data"
    // create table and insert
    sql """ DROP TABLE IF EXISTS ${table_export_name} """
    sql """
        CREATE TABLE IF NOT EXISTS ${table_export_name} (
        `id` int(11) NULL,
        `name` string NULL,
        `age` largeint(11) NULL,
        `dt` date NULL,
        `dt2` datev2 NULL,
        `dtime` datetime NULL,
        `dtime2` datetimev2 NULL
        )
        DISTRIBUTED BY HASH(id) PROPERTIES("replication_num" = "1");
    """

    sql """ INSERT INTO ${table_export_name} select * from s3(
            "uri" = "https://${bucket}.${s3_endpoint}/regression/export_p2/export_orc/test_export_big_data_dataset.csv",
            "s3.access_key"= "${ak}",
            "s3.secret_key" = "${sk}",
            "format" = "csv");
        """

    def uuid = UUID.randomUUID().toString()
    def outFilePath = """${bucket}/regression/export_p2/export_data/exp_"""
    // exec export
    sql """
        EXPORT TABLE ${table_export_name} TO "s3://${outFilePath}"
        PROPERTIES(
            "label" = "${uuid}",
            "format" = "orc",
            "column_separator"=",",
            "delete_existing_files"="true"
        )
        WITH s3 (
            "s3.endpoint" = "${s3_endpoint}",
            "s3.region" = "${region}",
            "s3.secret_key"="${sk}",
            "s3.access_key" = "${ak}"
        );
    """

    while (true) {
        def res = sql """ show export where label = "${uuid}" """
        logger.info("export state: " + res[0][2])
        if (res[0][2] == "FINISHED") {
            def json = parseJson(res[0][11])
            assert json instanceof List
            assertEquals("1", json.fileNumber[0])
            log.info("outfile_path: ${json.url[0]}")
            return json.url[0];
        } else if (res[0][2] == "CANCELLED") {
            throw new IllegalStateException("""export failed: ${res[0][10]}""")
        } else {
            sleep(5000)
        }
    }
}