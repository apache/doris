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

import groovy.json.JsonSlurper

def getProfile = { id ->
        def dst = 'http://' + context.config.feHttpAddress
        def conn = new URL(dst + "/rest/v1/query_profile/$id").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

// ref https://github.com/apache/doris/blob/3525a03815814f66ec78aa2ad6bbd9225b0e7a6b/regression-test/suites/load_p0/broker_load/test_s3_load.groovy
suite('s3_load_profile_test') {
    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    sql "drop table if exists dup_tbl_basic;"
    sql """
    CREATE TABLE dup_tbl_basic
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
    def loadAttribute =new LoadAttributes("s3://${getS3BucketName()}/regression/load/data/basic_data.csv",
                "dup_tbl_basic", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \"|\"", "FORMAT AS \"CSV\"", "(k00,k01,k02,k03,k04,k05,k06,k07,k08,k09,k10,k11,k12,k13,k14,k15,k16,k17,k18)",
                "", "", "", "", "")

    def ak = getS3AK()
    def sk = getS3SK()

    sql "set enable_profile=true;"    
    
    def label = "test_s3_load_" + UUID.randomUUID().toString().replace("-", "_")
    loadAttribute.label = label
    def prop = loadAttribute.getPropertiesStr()

    def sql_str = """
        LOAD LABEL $label (
            $loadAttribute.dataDesc.mergeType
            DATA INFILE("$loadAttribute.dataDesc.path")
            INTO TABLE $loadAttribute.dataDesc.tableName
            $loadAttribute.dataDesc.columnTermClause
            $loadAttribute.dataDesc.lineTermClause
            $loadAttribute.dataDesc.formatClause
            $loadAttribute.dataDesc.columns
            $loadAttribute.dataDesc.columnsFromPathClause
            $loadAttribute.dataDesc.columnMappingClause
            $loadAttribute.dataDesc.precedingFilterClause
            $loadAttribute.dataDesc.orderByClause
            $loadAttribute.dataDesc.whereExpr
        )
        WITH S3 (
            "AWS_ACCESS_KEY" = "$ak",
            "AWS_SECRET_KEY" = "$sk",
            "AWS_ENDPOINT" = "${s3Endpoint}",
            "AWS_REGION" = "${s3Region}",
            "use_path_style" = "$loadAttribute.usePathStyle",
            "provider" = "${getS3Provider()}"
        )
        ${prop}
        """
    logger.info("submit sql: ${sql_str}");
    sql """${sql_str}"""
    logger.info("Submit load with lable: $label, table: $loadAttribute.dataDesc.tableName, path: $loadAttribute.dataDesc.path")

    def max_try_milli_secs = 600000
    def jobId = -1
    while (max_try_milli_secs > 0) {
        String[][] result = sql """ show load where label="$loadAttribute.label" order by createtime desc limit 1; """
        if (result[0][2].equals("FINISHED")) {
            if (loadAttribute.isExceptFailed) {
                assertTrue(false, "load should be failed but was success: $result")
            }
            jobId = result[0][0].toLong()
            logger.info("Load FINISHED " + loadAttribute.label + ": $result" + " loadId: $jobId")
            break
        }
        if (result[0][2].equals("CANCELLED")) {
            if (loadAttribute.isExceptFailed) {
                logger.info("Load FINISHED " + loadAttribute.label)
                break
            }
            assertTrue(false, "load failed: $result")
            break
        }
        Thread.sleep(1000)
        max_try_milli_secs -= 1000
        if (max_try_milli_secs <= 0) {
            assertTrue(false, "load Timeout: $loadAttribute.label")
        }
    }

    qt_select """ select count(*) from $loadAttribute.dataDesc.tableName """

    def profileString = getProfile(jobId)
    profileJson = new JsonSlurper().parseText(profileString)
    assertEquals(0, profileJson.code)
    profileDataString = profileJson.data
    logger.info("profileDataString:" + profileDataString)
    def taskStateIdx = profileDataString.indexOf("Task&nbsp;&nbsp;State:&nbsp;&nbsp;FINISHED")
    assertFalse(taskStateIdx == -1)
    def executionProfileIdx = profileDataString.indexOf("Execution&nbsp;&nbsp;Profile")
    assertFalse(executionProfileIdx == -1)
    assertTrue(profileDataString.contains("NumScanners"))
    assertTrue(profileDataString.contains("RowsProduced"))
    assertTrue(profileDataString.contains("RowsRead"))
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
        properties.put("use_new_load_scan_node", "true")
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
