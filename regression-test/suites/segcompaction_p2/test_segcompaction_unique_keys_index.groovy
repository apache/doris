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

suite("test_segcompaction_unique_keys_index") {
    def tableName = "segcompaction_unique_keys_index_regression_test"
    String ak = getS3AK()
    String sk = getS3SK()
    String endpoint = getS3Endpoint()
    String region = getS3Region()
    String bucket = getS3BucketName()


    try {
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        boolean disableAutoCompaction = true
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
        }

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `col_0` BIGINT NOT NULL,`col_1` VARCHAR(20),`col_2` VARCHAR(20),`col_3` VARCHAR(20),`col_4` VARCHAR(20),
                `col_5` VARCHAR(20),`col_6` VARCHAR(20),`col_7` VARCHAR(20),`col_8` VARCHAR(20),`col_9` VARCHAR(20),
                `col_10` VARCHAR(20),`col_11` VARCHAR(20),`col_12` VARCHAR(20),`col_13` VARCHAR(20),`col_14` VARCHAR(20),
                `col_15` VARCHAR(20),`col_16` VARCHAR(20),`col_17` VARCHAR(20),`col_18` VARCHAR(20),`col_19` VARCHAR(20),
                `col_20` VARCHAR(20),`col_21` VARCHAR(20),`col_22` VARCHAR(20),`col_23` VARCHAR(20),`col_24` VARCHAR(20),
                `col_25` VARCHAR(20),`col_26` VARCHAR(20),`col_27` VARCHAR(20),`col_28` VARCHAR(20),`col_29` VARCHAR(20),
                `col_30` VARCHAR(20),`col_31` VARCHAR(20),`col_32` VARCHAR(20),`col_33` VARCHAR(20),`col_34` VARCHAR(20),
                `col_35` VARCHAR(20),`col_36` VARCHAR(20),`col_37` VARCHAR(20),`col_38` VARCHAR(20),`col_39` VARCHAR(20),
                `col_40` VARCHAR(20),`col_41` VARCHAR(20),`col_42` VARCHAR(20),`col_43` VARCHAR(20),`col_44` VARCHAR(20),
                `col_45` VARCHAR(20),`col_46` VARCHAR(20),`col_47` VARCHAR(20),`col_48` VARCHAR(20),`col_49` VARCHAR(20),
                INDEX index_col_0 (`col_0`) USING INVERTED COMMENT '',
                INDEX index_col_2 (`col_2`) USING INVERTED COMMENT ''
                )
            UNIQUE KEY(`col_0`) DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false"
            );
        """

        def uuid = UUID.randomUUID().toString().replace("-", "0")
        def path = "oss://$bucket/regression/segcompaction_test/segcompaction_test.orc"

        def columns = "col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, col_22, col_23, col_24, col_25, col_26, col_27, col_28, col_29, col_30, col_31, col_32, col_33, col_34, col_35, col_36, col_37, col_38, col_39, col_40, col_41, col_42, col_43, col_44, col_45, col_46, col_47, col_48, col_49"
        String columns_str = ("$columns" != "") ? "($columns)" : "";

        qt_select_default """ SELECT * FROM ${tableName} WHERE col_0=47 order by col_1; """
        qt_select_default """ SELECT COUNT(*) FROM ${tableName} WHERE col_1 MATCH_ANY 'lemon'; """
        qt_select_default """ SELECT COUNT(*) FROM ${tableName} WHERE col_2 MATCH_ANY 'Lemon'; """

        String[][] tablets = sql """ show tablets from ${tableName}; """
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
