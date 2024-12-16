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

suite("test_s3_load", "load_p0") {
    def s3BucketName = getS3BucketName()
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    sql "create workload group if not exists broker_load_test properties ( 'cpu_share'='1024'); "

    sql "set workload_group=broker_load_test;"

    def table = "dup_tbl_basic"

    sql """ DROP TABLE IF EXISTS ${table} """

    sql """
        CREATE TABLE ${table}
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
            k10 DECIMAL(9,1)           NULL,
            k11 DECIMALV3(9,1)         NULL,
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
            kd12 DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
            kd13 DATEV2          NOT NULL DEFAULT "2023-08-24",
            kd14 DATETIMEV2      NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
            DUPLICATE KEY(k00)
        PARTITION BY RANGE(k01)
        (
            PARTITION p1 VALUES [('2023-08-01'), ('2023-08-11')),
            PARTITION p2 VALUES [('2023-08-11'), ('2023-08-21')),
            PARTITION p3 VALUES [('2023-08-21'), ('2023-09-01'))
        )
        DISTRIBUTED BY HASH(k00) BUCKETS 32
        PROPERTIES (
            "bloom_filter_columns"="k05",
            "replication_num" = "1"
        );
    """

    def attributesList = [

    ]

    /* ========================================================== normal ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18)",
            "", "", "", "", ""))

    /* ========================================================== error ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data_with_errors.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", "", true))

    /* ========================================================== wrong column sep ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"csv\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", "", true))


    /* ========================================================== wrong line delim ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\t\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", "", true))


    /* ========================================================== strict mode ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data_with_errors.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "","", true).addProperties("strict_mode", "true"))

    /* ========================================================== timezone ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "Asia/Shanghai"))


    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "SET (k00=unix_timestamp('2023-09-01 12:00:00'))", "","").addProperties("timezone", "America/Chicago"))

    /* ========================================================== compress type ========================================================== */
    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.gz",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))


    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.bz2",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))


    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.lz4",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))


    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.gz",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.bz2",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.csv.lz4",
            "${table}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))


    /*========================================================== order by ==========================================================*/


    /*========================================================== json ==========================================================*/

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.json",
            "${table}", "", "", "FORMAT AS \"json\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", "PROPERTIES(\"strip_outer_array\" = \"true\", \"fuzzy_parse\" = \"true\")"))


    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data_by_line.json",
            "${table}", "", "", "FORMAT AS \"JSON\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", "PROPERTIES(\"read_json_by_line\" = \"true\")"))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.parq",
            "${table}", "", "", "FORMAT AS \"parquet\"", "(K00,K01,K02,K03,K04,K05,K06,K07,K08,K09,K10,K11,K12,K13,K14,K15,K16,K17,K18)",
            "", "", "", "", ""))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/basic_data.orc",
            "${table}", "", "", "FORMAT AS \"orc\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
            "", "", "", "", ""))


    def ak = getS3AK()
    def sk = getS3SK()

    def i = 0
    for (LoadAttributes attributes : attributesList) {
        def label = "test_s3_load_" + UUID.randomUUID().toString().replace("-", "_") + "_" + i
        attributes.label = label
        def prop = attributes.getPropertiesStr()

        def sql_str = """
            LOAD LABEL $label (
                $attributes.dataDesc.mergeType
                DATA INFILE("$attributes.dataDesc.path")
                INTO TABLE $attributes.dataDesc.tableName
                $attributes.dataDesc.columnTermClause
                $attributes.dataDesc.lineTermClause
                $attributes.dataDesc.formatClause
                $attributes.dataDesc.columns
                $attributes.dataDesc.columnsFromPathClause
                $attributes.dataDesc.columnMappingClause
                $attributes.dataDesc.precedingFilterClause
                $attributes.dataDesc.orderByClause
                $attributes.dataDesc.whereExpr
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${s3Endpoint}",
                "AWS_REGION" = "${s3Region}",
                "use_path_style" = "$attributes.usePathStyle",
                "provider" = "${getS3Provider()}"
            )
            ${prop}
            """
        logger.info("submit sql: ${sql_str}");
        sql """${sql_str}"""
        logger.info("Submit load with lable: $label, table: $attributes.dataDesc.tableName, path: $attributes.dataDesc.path")

        def max_try_milli_secs = 600000
        while (max_try_milli_secs > 0) {
            String[][] result = sql """ show load where label="$attributes.label" order by createtime desc limit 1; """
            if (result[0][2].equals("FINISHED")) {
                if (attributes.isExceptFailed) {
                    assertTrue(false, "load should be failed but was success: $result")
                }
                logger.info("Load FINISHED " + attributes.label + ": $result")
                break
            }
            if (result[0][2].equals("CANCELLED")) {
                if (attributes.isExceptFailed) {
                    logger.info("Load FINISHED " + attributes.label)
                    break
                }
                assertTrue(false, "load failed: $result")
                break
            }
            Thread.sleep(1000)
            max_try_milli_secs -= 1000
            if (max_try_milli_secs <= 0) {
                assertTrue(false, "load Timeout: $attributes.label")
            }
        }
        qt_select """ select count(*) from $attributes.dataDesc.tableName """
        ++i
    }

    qt_select """ select count(*) from ${table} """
}

class DataDesc {
    public String mergeType = ""
    public String path
    public String tableName
    public String lineTermClause
    public String columnTermClause
    public String formatClause
    public String columns
    public String columnsFromPathClause
    public String precedingFilterClause
    public String columnMappingClause
    public String whereExpr
    public String orderByClause
}

class LoadAttributes {
    LoadAttributes(String path, String tableName, String lineTermClause, String columnTermClause, String formatClause,
                   String columns, String columnsFromPathClause, String precedingFilterClause, String columnMappingClause, String whereExpr, String orderByClause, boolean isExceptFailed = false) {
        this.dataDesc = new DataDesc()
        this.dataDesc.path = path
        this.dataDesc.tableName = tableName
        this.dataDesc.lineTermClause = lineTermClause
        this.dataDesc.columnTermClause = columnTermClause
        this.dataDesc.formatClause = formatClause
        this.dataDesc.columns = columns
        this.dataDesc.columnsFromPathClause = columnsFromPathClause
        this.dataDesc.precedingFilterClause = precedingFilterClause
        this.dataDesc.columnMappingClause = columnMappingClause
        this.dataDesc.whereExpr = whereExpr
        this.dataDesc.orderByClause = orderByClause

        this.isExceptFailed = isExceptFailed

        properties = new HashMap<>()
    }

    LoadAttributes addProperties(String k, String v) {
        properties.put(k, v)
        return this
    }

    String getPropertiesStr() {
        if (properties.isEmpty()) {
            return ""
        }
        String prop = "PROPERTIES ("
        properties.forEach (k, v) -> {
            prop += "\"${k}\" = \"${v}\","
        }
        prop = prop.substring(0, prop.size() - 1)
        prop += ")"
        return prop
    }

    LoadAttributes withPathStyle() {
        usePathStyle = "true"
        return this
    }

    public DataDesc dataDesc
    public Map<String, String> properties
    public String label
    public String usePathStyle = "false"
    public boolean isExceptFailed
}
