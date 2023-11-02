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

import org.apache.doris.regression.suite.Suite

// create table with nested data type, now default complex data include array, map, struct
Suite.metaClass.create_table_with_nested_type = { int maxDepth, String tbName /* param */ ->
    Suite suite = delegate as Suite

    maxDepth = maxDepth > 9 ? 9 : maxDepth

    def dataTypeArr = ["boolean", "tinyint(4)", "smallint(6)", "int(11)", "bigint(20)", "largeint(40)", "float",
                       "double", "decimal(20, 3)", "decimalv3(20, 3)", "date", "datetime", "datev2", "datetimev2(0)",
                       "char(15)", "varchar(100)", "text"]
    def colNameArr = ["c_bool", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_largeint", "c_float",
                      "c_double", "c_decimal", "c_decimalv3", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]
    def complexDataTypeArr = ["array", "map", "struct"]
//    def tbName = "test"
    def colCount = 1
    def memo = new String[20]
    def r = new Random()

    def getDataType
    getDataType = { dataType, level ->
        if (memo[level] != null) {
            return memo[level];
        }

        StringBuilder res = new StringBuilder();
        def data_r = r.nextInt(3);

        if (level == 1) {
            if (data_r == 0) {
                res.append(complexDataTypeArr[data_r]+"<"+dataType+">");
            } else if (data_r == 1) {
                res.append(complexDataTypeArr[data_r]+"<"+dataType+"," +dataType+">");
            } else if (data_r == 2) {
                res.append(complexDataTypeArr[data_r]+"<col_"+colCount+":" + dataType +">");
                colCount++;
            }
        } else {
            level--;
            if (data_r == 0) {
                res.append(complexDataTypeArr[data_r]+"<"+getDataType(dataType, level)+">");
            } else if (data_r == 1) {
//                String str = getDataType(dataType, level);
                res.append(complexDataTypeArr[data_r]+"<"+getDataType(dataType, level)+"," +getDataType(dataType, level)+">")
            } else if (data_r == 2) {
                res.append(complexDataTypeArr[data_r]+"<col_"+colCount+":" + getDataType(dataType, level) +">");
                colCount++;
            }
        }
        memo[level] = res.toString()
        return memo[level];
    }

    def stmt = "CREATE TABLE IF NOT EXISTS " + tbName + "(\n" +
            "`k1` bigint(11) NULL,\n"
    String strTmp = "`" + colNameArr[0] + "` " + getDataType(dataTypeArr[0], maxDepth) + " NULL,\n";
    stmt += strTmp
    for (int i = 1; i < dataTypeArr.size(); i++) {
        String changeDataType = strTmp.replaceAll(colNameArr[0], colNameArr[i])
        changeDataType = changeDataType.replaceAll(dataTypeArr[0], dataTypeArr[i])
        stmt += changeDataType
    }
    stmt = stmt.substring(0, stmt.length()-2)
    stmt += ") ENGINE=OLAP\n" +
            "DUPLICATE KEY(`k1`)\n" +
            "COMMENT 'OLAP'\n" +
            "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n" +
            "PROPERTIES(\"replication_num\" = \"1\");"
    logger.info(stmt)
    return stmt
}

logger.info("Added 'create_table_with_nested_type' function to Suite")

