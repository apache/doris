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

import org.apache.http.client.methods.RequestBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.text.SimpleDateFormat

suite("test_stream_load_2pc", "p0") {

    // dynamic parition is according to current date to create partition, so we just use current time to test it, if passed long time, the test may be failed.

    // for test dup, mow, uniq, agg tables, we use sql statement concat these parameters
    // due to dynamic partition is different from others, it's the reason why we concat create sql statement 
    def tables = ["stream_load_dup_tbl_basic", "stream_load_mow_tbl_basic", "stream_load_uniq_tbl_basic", "stream_load_agg_tbl_basic"]

    def columns_stream_load = [
        """k00, k01, k02, k03, k04, k05, k06, k07, k08, k09, k10, k11, k12, k13, k14, k15, k16, k17, k18""",
        """k00, k01, k02, k03, k04, k05, k06, k07, k08, k09, k10, k11, k12, k13, k14, k15, k16, k17, k18""",
        """k00, k01, k02, k03, k04, k05, k06, k07, k08, k09, k10, k11, k12, k13, k14, k15, k16, k17, k18""",
        """k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18,k19=to_bitmap(k05), k20=HLL_HASH(k05), k21=TO_QUANTILE_STATE(k04, 1.0), kd19=to_bitmap(k05), kd20=HLL_HASH(k04), kd21=TO_QUANTILE_STATE(k04, 1.0)"""
    ]


    def create_table_sql = [
        """
        CREATE TABLE stream_load_dup_tbl_basic
        (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,

                INDEX idx_inverted_k104 (`k05`) USING INVERTED,
                INDEX idx_inverted_k110 (`k11`) USING INVERTED,
                INDEX idx_inverted_k113 (`k13`) USING INVERTED,
                INDEX idx_inverted_k114 (`k14`) USING INVERTED,
                INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
                INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),

                INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
                INDEX idx_bitmap_k110 (`kd01`) USING BITMAP

        )
        DUPLICATE KEY(k00)""",

        """
            CREATE TABLE stream_load_mow_tbl_basic
            (
                k00 INT             NOT NULL,
                k01 DATE            NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)            NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)         NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,

                INDEX idx_inverted_k104 (`k05`) USING INVERTED,
                INDEX idx_inverted_k110 (`k11`) USING INVERTED,
                INDEX idx_inverted_k113 (`k13`) USING INVERTED,
                INDEX idx_inverted_k114 (`k14`) USING INVERTED,
                INDEX idx_inverted_k117 (`k17`) USING INVERTED PROPERTIES("parser" = "english"),
                INDEX idx_ngrambf_k115 (`k15`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k116 (`k16`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256"),
                INDEX idx_ngrambf_k117 (`k17`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
            )
            UNIQUE KEY(k00,k01) 
        """,

        """
            CREATE TABLE stream_load_uniq_tbl_basic
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         NULL,
                k03 TINYINT         NULL,
                k04 SMALLINT        NULL,
                k05 INT             NULL,
                k06 BIGINT          NULL,
                k07 LARGEINT        NULL,
                k08 FLOAT           NULL,
                k09 DOUBLE          NULL,
                k10 DECIMAL(9,1)    NULL,
                k11 DECIMALV3(9,1)  NULL,
                k12 DATETIME        NULL,
                k13 DATEV2          NULL,
                k14 DATETIMEV2      NULL,
                k15 CHAR            NULL,
                k16 VARCHAR         NULL,
                k17 STRING          NULL,
                k18 JSON            NULL,
                kd01 BOOLEAN         NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         NOT NULL DEFAULT "1",
                kd03 SMALLINT        NOT NULL DEFAULT "2",
                kd04 INT             NOT NULL DEFAULT "3",
                kd05 BIGINT          NOT NULL DEFAULT "4",
                kd06 LARGEINT        NOT NULL DEFAULT "5",
                kd07 FLOAT           NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       NOT NULL DEFAULT "999999999",
                kd11 DATE            NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            NULL,

                INDEX idx_bitmap_k104 (`k02`) USING BITMAP,
                INDEX idx_bitmap_k110 (`kd01`) USING BITMAP,
                INDEX idx_bitmap_k113 (`k13`) USING BITMAP,
                INDEX idx_bitmap_k114 (`k14`) USING BITMAP,
                INDEX idx_bitmap_k117 (`k17`) USING BITMAP
            )
            UNIQUE KEY(k00,k01)
        """,

        """
            CREATE TABLE stream_load_agg_tbl_basic
            (
                k00 INT             NOT NULL,
                k01 DATE            NOT NULL,
                k02 BOOLEAN         REPLACE NULL,
                k03 TINYINT         SUM NULL,
                k04 SMALLINT        SUM NULL,
                k05 INT             SUM NULL,
                k06 BIGINT          SUM NULL,
                k07 LARGEINT        SUM NULL,
                k08 FLOAT           SUM NULL,
                k09 DOUBLE          SUM NULL,
                k10 DECIMAL(9,1)    SUM NULL,
                k11 DECIMALV3(9,1)  SUM NULL,
                k12 DATETIME        REPLACE NULL,
                k13 DATEV2          REPLACE NULL,
                k14 DATETIMEV2      REPLACE NULL,
                k15 CHAR(255)          REPLACE NULL,
                k16 VARCHAR(300)         REPLACE NULL,
                k17 STRING          REPLACE NULL,
                k18 JSON            REPLACE NULL,
                k19 BITMAP          BITMAP_UNION ,
                k20 HLL             HLL_UNION ,
                k21 QUANTILE_STATE  QUANTILE_UNION ,
                kd01 BOOLEAN         REPLACE NOT NULL DEFAULT "TRUE",
                kd02 TINYINT         SUM NOT NULL DEFAULT "1",
                kd03 SMALLINT        SUM NOT NULL DEFAULT "2",
                kd04 INT             SUM NOT NULL DEFAULT "3",
                kd05 BIGINT          SUM NOT NULL DEFAULT "4",
                kd06 LARGEINT        SUM NOT NULL DEFAULT "5",
                kd07 FLOAT           SUM NOT NULL DEFAULT "6.0",
                kd08 DOUBLE          SUM NOT NULL DEFAULT "7.0",
                kd09 DECIMAL         SUM NOT NULL DEFAULT "888888888",
                kd10 DECIMALV3       SUM NOT NULL DEFAULT "999999999",
                kd11 DATE            REPLACE NOT NULL DEFAULT "2023-08-24",
                kd12 DATETIME        REPLACE NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd13 DATEV2          REPLACE NOT NULL DEFAULT "2023-08-24",
                kd14 DATETIMEV2      REPLACE NOT NULL DEFAULT "2023-08-24 12:00:00",
                kd15 CHAR(255)       REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd16 VARCHAR(300)    REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd17 STRING          REPLACE NOT NULL DEFAULT "我能吞下玻璃而不伤身体",
                kd18 JSON            REPLACE NULL,
                kd19 BITMAP          BITMAP_UNION ,
                kd20 HLL             HLL_UNION ,
                kd21 QUANTILE_STATE  QUANTILE_UNION ,

                INDEX idx_bitmap_k104 (`k01`) USING BITMAP
            )
            AGGREGATE KEY(k00,k01)
        """
    ]

    def tbl_partitions = 
    [
        "",
        """
            PARTITION BY RANGE(k01)
            (

                PARTITION p1 VALUES [('2024-06-01'), ('2024-06-11')),
                PARTITION p2 VALUES [('2024-06-11'), ('2024-06-21')),
                PARTITION p3 VALUES [('2024-06-21'), ('2024-09-01'))
            )
        """,

        """
            PARTITION BY LIST(k01)
            (
                PARTITION p1 VALUES IN ('2024-06-08', '2024-06-19', '2024-06-15', '2024-06-20', '2024-06-18', '2024-06-06', '2024-06-07'),
                PARTITION p2 VALUES IN ('2024-06-11', '2024-06-14', '2024-06-09', '2024-06-21', '2024-06-10', '2024-06-12', '2024-06-13'),
                PARTITION p3 VALUES IN ('2024-06-16', '2024-06-22', '2024-06-23', '2024-06-24', '2024-06-17', '2024-06-25')
            )
        """,
        """
            PARTITION BY RANGE(k00, k01)
            (
                PARTITION p1 VALUES LESS THAN (20, '2024-06-01'),
                PARTITION p2 VALUES LESS THAN (40, '2024-06-11'),
                PARTITION p3 VALUES LESS THAN (60, '2024-06-21'),
                PARTITION p4 VALUES LESS THAN (80, '2024-06-30'),
                PARTITION other VALUES LESS THAN (MAXVALUE, MAXVALUE)
            )
        """,

        """
            PARTITION BY RANGE(k01) ()
        """

    ]

    def properties = [

        """
            "replication_num" = "1"
        """,

        """
            "bloom_filter_columns"="k05",
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        """,

        """
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"
        """,

        """
            "replication_num" = "1"
        """

    ]

    def dynamics = [
        "",
        "",
        "",
        '',
        """
        "dynamic_partition.enable" = "true",
        "dynamic_partition.time_unit" = "YEAR",
        "dynamic_partition.start" = "-10",
        "dynamic_partition.end" = "10",
        "dynamic_partition.prefix" = "p",
        "dynamic_partition.buckets" = "32",
        "dynamic_partition.create_history_partition" = "true"
        """]

    // for (int i = 0; i < tables.size(); ++i) {
    //     create_table_sql[i] = create_table_sql[i] + "\n" + tbl_partitions[i] + "\nDISTRUBUTED BY HASH(k00) BUCKETS 32\n" + properties[i]

    //     log.info("create table sql: ${create_table_sql[i]}")
    // }
    
    def tableName = "test_2pc_table"
    InetSocketAddress address = context.config.feHttpInetSocketAddress
    String user = context.config.feHttpUser
    String password = context.config.feHttpPassword
    String db = context.config.getDbNameByFile(context.file)

    def do_streamload_2pc_commit_by_label = { label, tbl ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H label:${label}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${db}/${tbl}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        log.info("http_stream 2pc result: ${out}".toString())
        def json2pc = parseJson(out)
        return json2pc
    }

    def do_streamload_2pc_commit_by_txn_id = { txnId, tbl ->
        def command = "curl -X PUT --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}" +
                " -H txn_id:${txnId}" +
                " -H txn_operation:commit" +
                " http://${context.config.feHttpAddress}/api/${db}/${tbl}/_stream_load_2pc"
        log.info("http_stream execute 2pc: ${command}")

        def process = command.execute()
        code = process.waitFor()
        out = process.text
        log.info("http_stream 2pc result: ${out}".toString())
        def json2pc = parseJson(out)
        return json2pc
    }

    try {
        // sql """ DROP TABLE IF EXISTS ${tableName} """
        // sql """
        //     CREATE TABLE IF NOT EXISTS ${tableName} (
        //         `k1` bigint(20) NULL DEFAULT "1",
        //         `k2` bigint(20) NULL ,
        //         `v1` tinyint(4) NULL,
        //         `v2` tinyint(4) NULL,
        //         `v3` tinyint(4) NULL,
        //         `v4` DATETIME NULL
        //     ) ENGINE=OLAP
        //     DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        //     PROPERTIES ("replication_allocation" = "tag.location.default: 1");
        // """

        def drop_table = {  ->
            sql """ DROP TABLE IF EXISTS ${tableName} """
        }

        def create_table = { partition ,dynamic->

            def distributed = "DISTRIBUTED BY HASH(k1) BUCKETS 3"
            if (dynamic != "") {
                dynamic = ", " + dynamic
                distributed = "DISTRIBUTED BY HASH(v5)"
            }

            // we direct set the 'v5' default is '2024-06-18' to test dynamic parition, due to may be not support '`v5` date default (curdate())'
            // so it will may be failed if  passed long time, but when support curdate() in create statement, we can success
            sql """
                CREATE TABLE IF NOT EXISTS ${tableName} (

                    `k1` bigint(20) NULL DEFAULT "1",
                    `k2` bigint(20) NULL ,
                    `v1` tinyint(4) NULL,
                    `v2` tinyint(4) NULL,
                    `v3` tinyint(4) NULL,
                    `v4` DATETIME NULL,
                    `v5` date default current_date
                ) ENGINE=OLAP
                $partition
                $distributed
                PROPERTIES ("replication_allocation" = "tag.location.default: 1"
                             $dynamic );
            """
        }

        def partitions = [
            "",  
            """PARTITION BY RANGE(k1)
            (
                PARTITION p1 VALUES LESS THAN (30),
                PARTITION p2 VALUES LESS THAN (60),
                PARTITION p3 VALUES LESS THAN (90),
                PARTITION other VALUES LESS THAN (MAXVALUE)
            )""",
            """PARTITION BY RANGE(k1, k2)
            (
                PARTITION p1 VALUES LESS THAN (1, 10),
                PARTITION p2 VALUES LESS THAN (2, 20),
                PARTITION p3 VALUES LESS THAN (3, 30),
                PARTITION other VALUES LESS THAN (MAXVALUE)
            )""",
            """PARTITION BY LIST(k1)
            (
                PARTITION p1 VALUES IN (1, 2, 3, 4, 5),
                PARTITION p2 VALUES IN (6, 7, 8, 9, 10),
                PARTITION p3 VALUES IN (11, 12, 13, 14, 15),
                PARTITION other VALUES IN (16, 17, 18, 19, 20)
            )""",
            """PARTITION BY LIST(k1, v1)
            (
                PARTITION p1 VALUES IN ((1, 50)),
                PARTITION p2 VALUES IN ((2, 10)),
                PARTITION p3 VALUES IN ((1, 1)),
                PARTITION other VALUES IN ((2, 1))
            )""",

            """PARTITION BY RANGE(v5) ()"""

        ]

        def dynamic_partition = ["", "", "", "", "",
        """
            "dynamic_partition.enable" = "true",
            "dynamic_partition.time_unit" = "YEAR",
            "dynamic_partition.start" = "-10",
            "dynamic_partition.end" = "5",
            "dynamic_partition.prefix" = "p",
            "dynamic_partition.buckets" = "32"
        """ 
        ]

        
        def tbl_2pc_expected = [1, 4, 4, 4, 4, 6]
        def i = 0;

        def streamLoadAction = { tbl, columns, filename, rowCount, expected ->
            def label = UUID.randomUUID().toString().replaceAll("-", "")
            def txnId;
            streamLoad {
                table "${tbl}"

                set 'label', "${label}"
                set 'column_separator', '|'
                set 'columns', columns
                set 'two_phase_commit', 'true'

                file "${filename}"

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(rowCount, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                    assertEquals(0, json.NumberUnselectedRows)
                    txnId = json.TxnId
                }
            }

            streamLoad {
                table "${tbl}"

                set 'label', "${label}"
                set 'column_separator', '|'
                set 'columns', columns
                set 'two_phase_commit', 'true'

                file "${filename}"

                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("label already exists", json.Status.toLowerCase())
                    assertTrue(json.Message.contains("has already been used, relate to txn"))
                }
            }

            def json2pc = do_streamload_2pc_commit_by_label.call(label, tbl)
            assertEquals("success", json2pc.status.toLowerCase())

            def part = sql """show partitions from ${tbl}"""
            log.info("partitions: ${part}")
            assertEquals(part.size(), expected)

            def count = 0
            while (true) {
                res = sql "select count(*) from ${tbl}"
                if (res[0][0] > 0) {
                    break
                }
                if (count >= 60) {
                    log.error("stream load commit can not visible for long time")
                    assertEquals(rowCount, res[0][0])
                    break
                }
                sleep(1000)
                count++
            }
            def orderby = ""
            if (tbl == "test_2pc_table") {
                orderby = "order by k1"
            } else {
                orderby = "order by k00, k01"
            }

            qt_sql_2pc_commit "select count(*) from ${tbl}"
            json2pc = do_streamload_2pc_commit_by_txn_id.call(txnId, tbl)
            assertTrue(json2pc.msg.contains("is already visible, not pre-committed"))
        }

        for (String partition in partitions) {
            drop_table.call()
            create_table.call(partition, dynamic_partition[i])
            streamLoadAction.call(tableName, 'k1, k2, v1, v2, v3', "test_two_phase_commit.csv", 2, tbl_2pc_expected[i])
            i++
            
        }

        // mow, uniq, agg tables range,list dynamic paritition test
        i = 0

        def concat_sql = { create, partition, property, dynamic ->
            if (dynamic != "")  {
                dynamic = ",\n" + dynamic
            }
            return create + "\n" + partition + "\nDISTRIBUTED BY HASH(k01) BUCKETS 32\n"+ "PROPERTIES($property $dynamic)"
        }
        def expected = [1, 3, 3, 5, 21] 
        // we recreate table for each partition, then load data with stream load and check the result
        for (i = 0; i < tables.size(); ++i) {
            if (isCloudMode() && tables[i] == "stream_load_mow_tbl_basic") {
                log.info("Skip stream load mow table in cloud mode")
                continue;
            }
            def j = 0
            for (String paritition in tbl_partitions) {

                String sqlStr = concat_sql.call(create_table_sql[i], paritition, properties[i], dynamics[j])
                sql """drop table if exists ${tables[i]}"""
                sql """${sqlStr}"""
                
                streamLoadAction.call(tables[i], columns_stream_load[i], "two_phase_commit_basic_data.csv", 20, expected[j++])
            }
        }
    } finally {
        sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
        for (table in tables) {
            sql """ DROP TABLE IF EXISTS ${table} FORCE"""
        }
    }

}

