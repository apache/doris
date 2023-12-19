
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

suite("test_s3_load_with_load_parallelism", "load_p0") {

    def tableName = "test_load_parallelism"

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName}(
            `col_0` BIGINT NOT NULL,`col_1` VARCHAR(20),`col_2` VARCHAR(20),`col_3` VARCHAR(20),`col_4` VARCHAR(20),
            `col_5` VARCHAR(20),`col_6` VARCHAR(20),`col_7` VARCHAR(20),`col_8` VARCHAR(20),`col_9` VARCHAR(20),
            `col_10` VARCHAR(20),`col_11` VARCHAR(20),`col_12` VARCHAR(20),`col_13` VARCHAR(20),`col_14` VARCHAR(20),
            `col_15` VARCHAR(20),`col_16` VARCHAR(20),`col_17` VARCHAR(20),`col_18` VARCHAR(20),`col_19` VARCHAR(20),
            `col_20` VARCHAR(20),`col_21` VARCHAR(20),`col_22` VARCHAR(20),`col_23` VARCHAR(20),`col_24` VARCHAR(20),
            `col_25` VARCHAR(20),`col_26` VARCHAR(20),`col_27` VARCHAR(20),`col_28` VARCHAR(20),`col_29` VARCHAR(20),
            `col_30` VARCHAR(20),`col_31` VARCHAR(20),`col_32` VARCHAR(20),`col_33` VARCHAR(20),`col_34` VARCHAR(20),
            `col_35` VARCHAR(20),`col_36` VARCHAR(20),`col_37` VARCHAR(20),`col_38` VARCHAR(20),`col_39` VARCHAR(20),
            `col_40` VARCHAR(20),`col_41` VARCHAR(20),`col_42` VARCHAR(20),`col_43` VARCHAR(20),`col_44` VARCHAR(20),
            `col_45` VARCHAR(20),`col_46` VARCHAR(20),`col_47` VARCHAR(20),`col_48` VARCHAR(20),`col_49` VARCHAR(20)
        ) ENGINE=OLAP
            DUPLICATE KEY(`col_0`) DISTRIBUTED BY HASH(`col_0`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1" );
    """

    def attributesList = [

    ]

    // attributesList.add(new LoadAttributes("s3://doris-build-1308700295/regression/load/data/enclose_not_trim_quotes.csv",
    //     "${tableName}", "", "COLUMNS TERMINATED BY \",\"", "FORMAT AS \"CSV\"", "(k1,k2,v1,v2,v3,v4)", 
    //     "PROPERTIES (\"enclose\" = \"\\\"\", \"escape\" = \"\\\\\")").addProperties("trim_double_quotes", "false"))
    
    attributesList.add(new LoadAttributes("s3://test-for-student-1308700295/regression/segcompaction/segcompaction.orc",
        "${tableName}", "", "", "FORMAT AS \"ORC\"", "(col_0, col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10, col_11, col_12, col_13, col_14, col_15, col_16, col_17, col_18, col_19, col_20, col_21, col_22, col_23, col_24, col_25, col_26, col_27, col_28, col_29, col_30, col_31, col_32, col_33, col_34, col_35, col_36, col_37, col_38, col_39, col_40, col_41, col_42, col_43, col_44, col_45, col_46, col_47, col_48, col_49)", "").addProperties("load_parallelism", "3"))

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
                "AWS_ENDPOINT" = "cos.ap-beijing.myqcloud.com",
                "AWS_REGION" = "ap-beijing"
           )
            ${prop}
            """
     //   "AWS_ENDPOINT" = "cos.ap-beijing.myqcloud.com",
    //   "AWS_ACCESS_KEY" = "AKIDd9RVMzIOI0V7Wlnbr9JG0WrhJk28zc2H",
    //   "AWS_SECRET_KEY"="4uWxMhqnW3Plz97sPjqlSUXO1RhokRuO",
    //   "AWS_REGION" = "ap-beijing"
 
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
    qt_sql """
        SELECT count(*) FROM ${tableName}
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
