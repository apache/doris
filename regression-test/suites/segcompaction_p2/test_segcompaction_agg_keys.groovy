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

suite("test_segcompaction_agg_keys") {
    def tableName = "segcompaction_agg_keys_regression_test"
    def tableName2 = "segcompaction_agg_keys_regression_test_big_table"
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
                `col_0` BIGINT NOT NULL,`col_1` VARCHAR(20) REPLACE,`col_2` VARCHAR(20) REPLACE,`col_3` VARCHAR(20) REPLACE,`col_4` VARCHAR(20) REPLACE,
                `col_5` VARCHAR(20) REPLACE,`col_6` VARCHAR(20) REPLACE,`col_7` VARCHAR(20) REPLACE,`col_8` VARCHAR(20) REPLACE,`col_9` VARCHAR(20) REPLACE,
                `col_10` VARCHAR(20) REPLACE,`col_11` VARCHAR(20) REPLACE,`col_12` VARCHAR(20) REPLACE,`col_13` VARCHAR(20) REPLACE,`col_14` VARCHAR(20) REPLACE,
                `col_15` VARCHAR(20) REPLACE,`col_16` VARCHAR(20) REPLACE,`col_17` VARCHAR(20) REPLACE,`col_18` VARCHAR(20) REPLACE,`col_19` VARCHAR(20) REPLACE,
                `col_20` VARCHAR(20) REPLACE,`col_21` VARCHAR(20) REPLACE,`col_22` VARCHAR(20) REPLACE,`col_23` VARCHAR(20) REPLACE,`col_24` VARCHAR(20) REPLACE,
                `col_25` VARCHAR(20) REPLACE,`col_26` VARCHAR(20) REPLACE,`col_27` VARCHAR(20) REPLACE,`col_28` VARCHAR(20) REPLACE,`col_29` VARCHAR(20) REPLACE,
                `col_30` VARCHAR(20) REPLACE,`col_31` VARCHAR(20) REPLACE,`col_32` VARCHAR(20) REPLACE,`col_33` VARCHAR(20) REPLACE,`col_34` VARCHAR(20) REPLACE,
                `col_35` VARCHAR(20) REPLACE,`col_36` VARCHAR(20) REPLACE,`col_37` VARCHAR(20) REPLACE,`col_38` VARCHAR(20) REPLACE,`col_39` VARCHAR(20) REPLACE,
                `col_40` VARCHAR(20) REPLACE,`col_41` VARCHAR(20) REPLACE,`col_42` VARCHAR(20) REPLACE,`col_43` VARCHAR(20) REPLACE,`col_44` VARCHAR(20) REPLACE,
                `col_45` VARCHAR(20) REPLACE,`col_46` VARCHAR(20) REPLACE,`col_47` VARCHAR(20) REPLACE,`col_48` VARCHAR(20) REPLACE,`col_49` VARCHAR(20) REPLACE
                )
            AGGREGATE KEY(`col_0`) DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1" );
        """

        def uuid = UUID.randomUUID().toString().replace("-", "0")
        def path = "oss://$bucket/regression/segcompaction_test/segcompaction_test.orc"

        def columns = "col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, col_22, col_23, col_24, col_25, col_26, col_27, col_28, col_29, col_30, col_31, col_32, col_33, col_34, col_35, col_36, col_37, col_38, col_39, col_40, col_41, col_42, col_43, col_44, col_45, col_46, col_47, col_48, col_49"
        String columns_str = ("$columns" != "") ? "($columns)" : "";

        sql """
            LOAD LABEL $uuid (
                DATA INFILE("s3://$bucket/regression/segcompaction/segcompaction.orc")
                INTO TABLE $tableName
                FORMAT AS "ORC"
                $columns_str
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "$endpoint",
                "AWS_REGION" = "$region",
                "provider" = "${getS3Provider()}"
            )
            properties(
                "use_new_load_scan_node" = "true"
            )
            """

        def max_try_milli_secs = 3600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$uuid" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                logger.info("Load FINISHED " + " $uuid")
                break;
            }
            if (result[0][2].equals("CANCELLED")) {
                logger.info("Load CANCELLED " + " $uuid")
                break;
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if(max_try_milli_secs <= 0) {
                assertTrue(1 == 2, "load Timeout: $uuid")
            }
        }

        qt_select_default """ SELECT * FROM ${tableName} WHERE col_0=47; """

        String[][] tablets = sql """ show tablets from ${tableName}; """

        def count_src = sql " select count() from ${tableName}; "
        assertTrue(count_src[0][0] > 0)
        logger.info("got rows: ${count_src[0][0]}")

        // test partial update
        sql """ DROP TABLE IF EXISTS ${tableName2} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName2} (
                `col_0` BIGINT NOT NULL,`col_1` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_2` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_3` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_4` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_5` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_6` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_7` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_8` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_9` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_10` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_11` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_12` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_13` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_14` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_15` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_16` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_17` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_18` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_19` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_20` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_21` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_22` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_23` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_24` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_25` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_26` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_27` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_28` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_29` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_30` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_31` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_32` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_33` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_34` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_35` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_36` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_37` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_38` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_39` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_40` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_41` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_42` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_43` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_44` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_45` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_46` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_47` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_48` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_49` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_50` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_51` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_52` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_53` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_54` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_55` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_56` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_57` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_58` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_59` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_60` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_61` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_62` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_63` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_64` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_65` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_66` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_67` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_68` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_69` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_70` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_71` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_72` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_73` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_74` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_75` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_76` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_77` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_78` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_79` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_80` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_81` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_82` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_83` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_84` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_85` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_86` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_87` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_88` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_89` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_90` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_91` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_92` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_93` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_94` VARCHAR(20) REPLACE_IF_NOT_NULL,
                `col_95` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_96` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_97` VARCHAR(20) REPLACE_IF_NOT_NULL,`col_98` VARCHAR(20) REPLACE_IF_NOT_NULL
                )
            AGGREGATE KEY(`col_0`) DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1" );
        """
        sql "set enable_agg_key_partial_update=true;"
        sql "set enable_insert_strict = false;"
        sql "sync;"
        def columns2 = "col_0, col_50, col_51, col_52, col_53, col_54, col_55, col_56, col_57, col_58, col_59, col_60, col_61, col_62, col_63, col_64, col_65, col_66, col_67, col_68, col_69, col_70, col_71, col_72, col_73, col_74, col_75, col_76, col_77, col_78, col_79, col_80, col_81, col_82, col_83, col_84, col_85, col_86, col_87, col_88, col_89, col_90, col_91, col_92, col_93, col_94, col_95, col_96, col_97, col_98"
        sql "insert into ${tableName2}(${columns}) select ${columns} from ${tableName};"
        sql "insert into ${tableName2}(${columns2}) select ${columns} from ${tableName};"
        def count_dest = sql " select count() from ${tableName2}; "
        logger.info("got rows: ${count_dest[0][0]}")
        assertEquals(count_src[0][0], count_dest[0][0])
        qt_select_default2 """ SELECT * FROM ${tableName2} WHERE col_0=47; """
        sql "set enable_agg_key_partial_update=false;"
        sql "set enable_insert_strict = true;"
        qt_checksum1 """SELECT MD5(Group_Concat(concat_ws('|', col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10) ORDER BY col_0 ASC)) AS checksum FROM ${tableName2};"""
        qt_checksum2 """SELECT MD5(Group_Concat(concat_ws('|', col_0, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20) ORDER BY col_0 ASC)) AS checksum FROM ${tableName2};"""
        qt_checksum3 """SELECT MD5(Group_Concat(concat_ws('|', col_0, col_81, col_82, col_83, col_84, col_85, col_86, col_87, col_88, col_89, col_90) ORDER BY col_0 ASC)) AS checksum FROM ${tableName2};"""
        qt_checksum4 """SELECT MD5(Group_Concat(concat_ws('|', col_0, col_91, col_92, col_93, col_94, col_95, col_96, col_97, col_98) ORDER BY col_0 ASC)) AS checksum FROM ${tableName2};"""

        sql "sync;"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        try_sql("DROP TABLE IF EXISTS ${tableName2}")
    }
}
