
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

suite("test_csv_with_enclose_and_escapeS3_load", "load_p0") {

    def tableName = "test_csv_with_enclose_and_escape"
    def s3BucketName = getS3BucketName()

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}(
            `k1` VARCHAR(44) NOT NULL,
            `k2`  VARCHAR(44) NOT NULL,
            `v1` VARCHAR(44) NOT NULL,
            `v2` VARCHAR(44) NOT NULL,
            `v3` VARCHAR(44) NOT NULL,
            `v4` VARCHAR(44) NOT NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 3
        PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """

    def normalCases = [
        'enclose_normal',
        'enclose_with_escape',
        'enclose_wrong_position',
        'enclose_empty_values'
    ]

    def attributesList = [

    ]

    for (i in 0..<normalCases.size()) {
        attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/${normalCases[i]}.csv",
                "${tableName}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
                "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\", \"trim_double_quotes\" = \"true\")"))
    }

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/enclose_incomplete.csv",
        "${tableName}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
        "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\", \"trim_double_quotes\" = \"true\")").addProperties("max_filter_ratio", "0.5"))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/enclose_without_escape.csv",
        "${tableName}", "LINES TERMINATED BY \"\n\"", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
        "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\", \"trim_double_quotes\" = \"true\")"))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/enclose_multi_char_delimiter.csv",
        "${tableName}", "LINES TERMINATED BY \"\$\$\$\"", "COLUMNS TERMINATED BY \"@@\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
        "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\", \"trim_double_quotes\" = \"true\")"))

    attributesList.add(new LoadAttributes("s3://${s3BucketName}/regression/load/data/enclose_not_trim_quotes.csv",
        "${tableName}", "", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
        "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\")").addProperties("trim_double_quotes", "false"))

    def ak = getS3AK()
    def sk = getS3SK()


    def i = 0
    for (LoadAttributes attributes : attributesList) {
        def label = "test_s3_load_escape_enclose" + UUID.randomUUID().toString().replace("-", "_") + "_" + i
        attributes.label = label
        def prop = attributes.getPropertiesStr()

        sql """
            LOAD LABEL $label (
                DATA INFILE("$attributes.dataDesc.path")
                INTO TABLE $attributes.dataDesc.tableName
                $attributes.dataDesc.columnTermClause
                $attributes.dataDesc.lineTermClause
                $attributes.dataDesc.formatClause
                $attributes.dataDesc.columns
                $attributes.dataDesc.whereExpr
            )
            WITH S3 (
                "AWS_ACCESS_KEY" = "$ak",
                "AWS_SECRET_KEY" = "$sk",
                "AWS_ENDPOINT" = "${getS3Endpoint()}",
                "AWS_REGION" = "${getS3Region()}",
                "provider" = "${getS3Provider()}"
            )
            ${prop}
            """

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
                if (attributes.dataDesc.path.split("/")[-1] == "enclose_incomplete.csv" || attributes.dataDesc.path.split("/")[-1] == "enclose_without_escape.csv") {
                    break
                }
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
    }
    sql "sync"
    qt_select """
        SELECT * FROM ${tableName} ORDER BY k1, k2 
    """
}

class DataDesc {
    public String mergeType = ""
    public String path
    public String tableName
    public String lineTermClause
    public String columnTermClause
    public String formatClause
    public String columns
    public String whereExpr
}

class LoadAttributes {
    LoadAttributes(String path, String tableName, String lineTermClause, String columnTermClause, String formatClause,
                   String columns, String whereExpr, boolean isExceptFailed = false) {
        this.dataDesc = new DataDesc()
        this.dataDesc.path = path
        this.dataDesc.tableName = tableName
        this.dataDesc.lineTermClause = lineTermClause
        this.dataDesc.columnTermClause = columnTermClause
        this.dataDesc.formatClause = formatClause
        this.dataDesc.columns = columns
        this.dataDesc.whereExpr = whereExpr

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
