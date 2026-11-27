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

import org.apache.doris.regression.util.ExportTestHelper

suite("test_export_big_data", "p2") {
    // open nereids
    sql """ set enable_nereids_planner=true """
    sql """ set enable_fallback_to_original_planner=false """

    def helper = new ExportTestHelper(delegate)

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
            "uri" = "${helper.uri('regression/export_p2/export_orc/test_export_big_data_dataset.orc')}",
            ${helper.storageProperties()},
            "format" = "orc");
        """

    def uuid = UUID.randomUUID().toString()
    def outFilePath = "regression/test_export_big_data/${uuid}"
    try {
        // exec export
        sql """
            EXPORT TABLE ${table_export_name} TO "${helper.uri(outFilePath)}/"
            PROPERTIES(
                "label" = "${uuid}",
                "format" = "orc"
            ) WITH S3 (${helper.storageProperties()});
        """
        while (true) {
            def res = sql """ show export where label = "${uuid}" """
            logger.info("export state: " + res[0][2])
            if (res[0][2] == "FINISHED") {
                def json = parseJson(res[0][11])
                assert json instanceof List
                assertEquals("1", json.fileNumber[0][0])
                logger.info("outfile_path: ${json.url[0][0]}")
                return json.url[0][0];
            } else if (res[0][2] == "CANCELLED") {
                throw new IllegalStateException("""export failed: ${res[0][10]}""")
            } else {
                sleep(5000)
            }
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${table_export_name}")
        helper.deleteFiles("${outFilePath}/")
    }
}
