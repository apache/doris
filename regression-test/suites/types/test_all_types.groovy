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

import groovy.sql.Sql

suite("test_all_types", "types") {

    def dbUrl = "jdbc:mysql://127.0.0.1:9030"
    def dbUser = "root"
    def dbPassword = "mypassword"

    def sql1 = Sql.newInstance(dbUrl, dbUser, dbPassword, "com.mysql.cj.jdbc.Driver")

    def tableName = "all_data_types"

    def dataTypes = sql1.rows("SHOW data types")

    def createTableSQL = "CREATE TABLE IF NOT EXISTS ${tableName} ("
    def subSQL = ""

    def index = 0

    def masterKey = ""

    dataTypes.each { row ->
        def dataType = row.TypeName
        println(dataType)
        // createTableSQL += "${dataType.toLowerCase()},"
        index++
        if (dataType == "NULL_TYPE" || dataType == "QUANTILE_STATE") {
            return
        } else if (dataType == "ARRAY" || dataType == "MAP" || dataType == "TIMEV2" || dataType == "VARIANT" || dataType == "TIME") {
            return
        } else if (dataType == "DECIMAL128" || dataType == "DECIMAL32" || dataType == "DECIMAL64" || dataType == "DECIMALV2") {
            return
        } else if (dataType == "JSONB") {
            subSQL += "k${index} ${dataType} REPLACE, "
        } else if (dataType == "STRING") {
            subSQL += "k${index} ${dataType} REPLACE, "
        } else if (dataType == "FLOAT") {
            subSQL += "k${index} ${dataType} MAX, "
        } else if (dataType == "DOUBLE") {
            subSQL += "k${index} ${dataType} MAX, "
        } else if (dataType == "HLL") {
            subSQL += "k${index} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "BITMAP") {
            subSQL += "k${index} ${dataType} ${dataType}_UNION, "
        } else if (dataType == "QUANTILE_STATE") {
            subSQL += "k${index} ${dataType} QUANTILE_UNION, "
        } else {
            masterKey += "k${index}, "
            createTableSQL += "k${index} ${dataType}, "
        }
    }
    subSQL = subSQL.substring(0, subSQL.length() - 2)
    masterKey = masterKey.substring(0, masterKey.length() - 2)
    createTableSQL = createTableSQL.substring(0, createTableSQL.length() - 2)
    createTableSQL += "," + subSQL
    createTableSQL += ")"

    createTableSQL += """AGGREGATE KEY(${masterKey})
    DISTRIBUTED BY HASH(k2) BUCKETS 5 properties("replication_num" = "1");"""
    

    sql "${createTableSQL}"

    streamLoad {
        table 'all_data_types'

        set 'column_separator', ','

        set 'columns', 'c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, k3=to_bitmap(c13), k16=hll_hash(c16)'

        file 'streamload_input.csv'

        time 10000 
    }
    // sql """ insert into ${tableName} (k3, k14, k15, k16, k18, k24) values (to_bitmap(17), 1, 1, hll_hash(17), '"a"', 'text1');
    // """

    qt_sql "select * from ${tableName}"

    sql "DROP TABLE ${tableName}"

    // 获取数据类型

    // def result = sql1.executeQuery('SHOW DATA TYPES')
    // List<Map<String, Object>> dataTypes = []
    // while (result.next()) {
    //     def row = [:]
    //     result.metaData.columnCount.times { i ->
    //         row[result.metaData.getColumnName(i+1)] = result.getObject(i+1)
    //     }
    //     dataTypes << row
    // }

    // def result = sql1.executeQuery('SHOW DATA TYPES')
    // List<String> dataTypes = []
    // int times = 29
    // while (times--) {
    //     String dataType = result.toString()
    //     dataTypes << dataType
    // }
    // result.close()

    // def dataTypes = sql1.rows("SHOW DATA TYPES")


    // dataTypes.collectIndexed { index, dataType ->
    //     println ${dataType.TypeName}
    // }

    // // 动态生成表结构和列名
    // def columns = dataTypes.collectIndexed { index, dataType ->
    //     if (dataTypes == "NULL_TYPE") {
    //         return
    //     } else if (dataTypes == "JSONB") {
    //         "k${index + 1} ${dataTypes} REPLACE"
    //     } else if (dataTypes == "STRING") {
    //         "k${index + 1} ${dataTypes} REPLACE"
    //     } else if (dataTypes == "FLOAT") {
    //         "k${index + 1} ${dataTypes} MAX"
    //     } else if (dataTypes == "DOUBLE") {
    //         "k${index + 1} ${dataTypes} MAX"
    //     } else if (dataTypes == "HLL") {
    //         "k${index + 1} ${dataTypes} ${dataTypes}_UNION"
    //     } else if (dataTypes == "BITMAP") {
    //         "k${index + 1} ${dataTypes} ${dataTypes}_UNION"
    //     } else if (dataTypes == "QUANTILE_STATE") {
    //         "k${index + 1} ${dataTypes} ${dataTypes}_UNION"
    //     } else if (dataTypes == "ARRAY") {
    //         return
    //     } else {
    //         "k${index + 1} ${dataTypes}"
    //     }
    // }.join(", ")

    // // 创建表
    // sql """CREATE TABLE IF NOT EXISTS ${tableName} (id INT PRIMARY KEY, $columns)
    // AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k7,k8,k9,k10,k11,k12,k13,k14,k16,k18,k23,k25,k26,k27,k28,k29)
    // DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1");
    // """

    // qt_sql """DESCRIBE ${tableName}"""

    // sql """DROP TABLE IF EXISTS ${tableName}"""
}
