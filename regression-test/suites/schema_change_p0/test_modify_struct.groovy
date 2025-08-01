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
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite ("test_modify_struct") {
    def waitUntilSchemaChangeDone = { tableName, insert_sql, canceled=false ->
        if (canceled) {
            Awaitility.await().atMost(300, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
                def jobStateResult = sql """ SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1  """
                if (jobStateResult[0][9].toString().toUpperCase() == "CANCELLED") {
                    return true
                }
                return false
            })
        } else {
            waitForSchemaChangeDone({
                sql " SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1 "
                time 600
            }, insert_sql)
        }
    }

    def tableNamePrefix = "test_struct_add_sub_column"
    def tableName = tableNamePrefix
    List<String> tableNames = new ArrayList<String>()
    List<String> mv_query_sql = new ArrayList<String>()
    List<String> mvNames = new ArrayList<String>()

    try {

        // 1. create table with duplicate key, agg key, unique key
        def suffixTypes = ["duplicate", "unique", "aggregate"]
        def defaultValues = ["NULL", "NULL", "REPLACE_IF_NOT_NULL"]
        for (int j = 0; j < suffixTypes.size(); j++) {
            String suffix = suffixTypes[j]
            String defaultValue = defaultValues[j]
            String notNullValue = j == 2 ? defaultValue : "NOT NULL"
            String c1DefaultValue = j == 2 ? defaultValue : "DEFAULT '10.5'"

            tableName = tableNamePrefix + "_" + suffix
            tableNames.add(tableName)
            sql "DROP TABLE IF EXISTS ${tableName} FORCE;"
            sql """
                CREATE TABLE IF NOT EXISTS `${tableName}`
                (
                    `c0` LARGEINT NOT NULL,
                    `c1` DECIMAL(10,2) ${c1DefaultValue},
                    `c_s_not_null`  STRUCT<col:VARCHAR(10)> ${notNullValue},
                    `c_s` STRUCT<col:VARCHAR(10)> ${defaultValue},
                )
                ${suffix.toUpperCase()} KEY(`c0`)
                DISTRIBUTED BY HASH(c0) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                );
            """
            // we can create MV from original table
            // CREATE MATERIALIZED VIEW for complex type is forbidden, which defined MaterializedViewHandler::checkAndPrepareMaterializedView
            mvNames.add("${tableName}_mv")
            def query_sql = "SELECT c1, c_s FROM ${tableName}"
            mv_query_sql.add(query_sql)
            def mvNotSupportedMsg = "errCode = 2"
            expectExceptionLike({
                sql """ CREATE MATERIALIZED VIEW ${tableName}_mv AS ${query_sql} """
            }, mvNotSupportedMsg)

            // 2. insert some data before modify struct column
            sql """ insert into $tableName values
                    (0, 13.7, named_struct('col','commiter'), named_struct('col','commiter'));
                """
            sql """ insert into $tableName values
                    (1, 14.9, named_struct('col','commiter'), named_struct('col','amory'));
                """
            // more than 10 characters
            test {
                sql """ insert into $tableName values
                    (11, 111.111, named_struct('col','commiter'), named_struct('col','amoryIsBetter'));
                """
                exception "Insert has filtered data in strict mode"
            }

            order_qt_sc_before """ select * from ${tableName} order by c0; """

            // 3. modify struct column
                //  Positive Test Case
                //    3.1 add sub-column
                //    3.2 add sub-columns
                //    3.3 add sub-column + lengthen sub-varchar-column
                //
                //  Negative Test Case
                //    3.4 add sub-column + re-order struct-column
                //    3.5 reduce sub-column
                //    3.6 reduce sub-columns
                //    3.7 add sub-column + shorten sub-varchar-column
                //    3.8 change struct to other type
                //    3.9 add sub-column + duplicate sub-column name
                //    3.10 add sub-column + change origin sub-column name
                //    3.11 add sub-column + change origin sub-column type
                //    3.12 add sub-column with json/variant
            ////////////////////////////////==================   Positive Test Case =================//////////////////
            // 3.1 add sub-column
            def sub_columns = ["col1:INT", "col2:DECIMAL(10, 2)", "col3:DATETIME", "col4:ARRAY<STRING>", "col5:MAP<INT, STRING>", "col6:STRUCT<a:INT, b:STRING>"]
            def sub_column_values = ["'col1', 1", "'col2', 1.1", "'col3', '2021-01-01 00:00:01'", "'col4', ['a', 'b']", "'col5', {1:'a', 2:'b'}", "'col6', {1, 'a'}"]
            String sub_col = "Struct<col: VARCHAR(10)>"
            for (int i = 0; i < sub_columns.size(); i++) {
                // we should replace > with , in sub_col
                sub_col = sub_col[0..<sub_col.length()-1] + ", " + sub_columns[i] + ">"
                sql """ alter table ${tableName} modify column c_s ${sub_col} ${defaultValue}"""
                String add_columns = ""
                for (int k = 0 ; k <= i; k++) {
                    add_columns += ", " + sub_column_values[k]
                }
                def insert_sql = "insert into ${tableName} values (" + (i+2).toString() + ", 21.12, named_struct('col','commiter'), named_struct('col','amory2'${add_columns}))"
                logger.info(insert_sql)
                waitUntilSchemaChangeDone.call(tableName, insert_sql)
                // check result
                qt_sql_after """ select * from ${tableName} order by c0; """
            }

            // 3.2 add sub-columns
            def all_sub_column = "col11:INT, col12:DECIMAL(10, 2), col13:DATETIME, col14:ARRAY<STRING>, col15:MAP<INT, STRING>, col16:STRUCT<a:INT, b:STRING>"
            sql """ alter table ${tableName} modify column c_s STRUCT<col:VARCHAR(10), col1:INT, col2:DECIMAL(10, 2), col3:DATETIME, col4:ARRAY<STRING>, col5:MAP<INT, STRING>, col6:STRUCT<a:INT, b:STRING>, ${all_sub_column}> ${defaultValue}"""
            def insert_sql1 = "insert into ${tableName} values (8, 31.13, named_struct('col','commiter'), named_struct('col','amory3','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}))"
            waitUntilSchemaChangeDone.call(tableName, insert_sql1)
            // check result
            qt_sql_after1 """ select * from ${tableName} order by c0; """

            // 3.3 add sub-column + lengthen sub-varchar-column
            sql """ alter table ${tableName} add column c_s_1 STRUCT<col:VARCHAR(20)> ${defaultValue}"""
            def insert_sql11 = "insert into ${tableName} values (9, 41.14, named_struct('col','commiter'), named_struct('col','amory4','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), named_struct('col','amoryIsBetter'))"
            waitUntilSchemaChangeDone.call(tableName, insert_sql11)
            // check new create struct column
            qt_sql_after2 """ select c_s_1 from ${tableName} where c0 = 9; """
            // add sub-column + lengthen sub-varchar-column
            sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT> ${defaultValue}"""
            def insert_sql12 = "insert into ${tableName} values (10, 51.15, named_struct('col','commiter'), named_struct('col','amory5','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), named_struct('col','amoryIsMoreMoreThan30Better', 'col1', 1))"
            waitUntilSchemaChangeDone.call(tableName, insert_sql12)
            // check new create struct column
            qt_sql_after3 """ select c_s_1 from ${tableName} where c0 = 10; """

            //////////////////==================   Negative Test Case =================//////////////////
            // 3.4 add sub-column + re-order struct-column
            // add a scala column
            sql """ alter table ${tableName} add column str_col STRING ${defaultValue}"""
            // insert data
            sql """ insert into ${tableName} values (11, 61.16, named_struct('col','commiter'), named_struct('col','amory6','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}),  named_struct('col','amoryIsBetter', 'col1', 1), 'amory6')"""
            // check new create string column
            qt_sql_after4 """ select * from ${tableName} where c0 = 11; """
            // add sub-column + re-order struct column: which sc task will failed
            sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT, col2:decimal(10,2)> ${defaultValue} after str_col """
            def insert_sql13 = "insert into ${tableName} values (12, 71.17, named_struct('col','commiter'), named_struct('col','amory7','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory7', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 12.2))"
            waitUntilSchemaChangeDone.call(tableName, insert_sql13, true)
            // check data
            qt_sql_after5 """ select * from ${tableName} where c0 = 12; """
            // then we add subcolumn then re-order struct column
            sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT, col2:decimal(10,2)> ${defaultValue} """
            waitUntilSchemaChangeDone.call(tableName, "")
            sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT, col2:decimal(10,2)> ${defaultValue} after str_col """
            waitUntilSchemaChangeDone.call(tableName, insert_sql13)
            // check data
            qt_sql_after51 """ select * from ${tableName} where c0 = 12; """

            // desc for c_s_1
            String[][] res = sql """ desc ${tableName} """
            logger.info(res[5][1])
            assertEquals(res[5][1].toLowerCase(),"struct<col:varchar(30),col1:int,col2:decimal(10,2)>")

            // 3.5 reduce sub-column
            def reduceErrMsg="errCode = 2, detailMessage = Cannot reduce struct fields from"
            expectExceptionLike({
                sql """ alter  table ${tableName} MODIFY  column c_s_1 STRUCT<col:VARCHAR(30), col1:INT>  ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },reduceErrMsg)
            // 3.6 reduce sub-columns
            expectExceptionLike({
                sql """ alter  table ${tableName} MODIFY  column c_s_1 STRUCT<col:VARCHAR(30)> ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },reduceErrMsg)
            // 3.7 add sub-column + shorten sub-varchar-column
            def shortenErrMsg="errCode = 2, detailMessage = Shorten type length is prohibited"
            expectExceptionLike({
                sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(10), col1:INT, col2:DECIMAL(10,2), col3:DATETIME> ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },shortenErrMsg)
            // 3.8 change struct to other type
            def changeErrMsg="errCode = 2, detailMessage = Can not change"
            for (String type : ["STRING", "INT", "DECIMAL(10, 2)", "DATETIME", "ARRAY<STRING>", "MAP<INT, STRING>"]) {
                expectExceptionLike({
                    sql """ alter table ${tableName} modify column c_s_1 ${type} ${defaultValue} """
                    waitUntilSchemaChangeDone.call(tableName, "")
                },changeErrMsg)
            }
            // 3.9 add sub-column + duplicate sub-column name; when in DataType::validateCatalogDataType will throw exception
            def duplicateErrMsg="errCode = 2, detailMessage = Duplicate field name"
            expectExceptionLike({
                sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT, col2:DECIMAL(10,2), col1:INT> ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },duplicateErrMsg)
            // 3.10 add sub-column + change origin sub-column name
            def changeNameErrMsg="errCode = 2, detailMessage = Cannot rename"
            expectExceptionLike({
                sql """ alter table ${tableName} modify column c_s_1 STRUCT<col4:VARCHAR(30), col1:INT, col2:DECIMAL(10,2), col3:INT> ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },changeNameErrMsg)
            // 3.11 add sub-column + change origin sub-column type
            def changeTypeErrMsg="errCode = 2, detailMessage = Cannot change"
            expectExceptionLike({
                sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:STRING, col2:DECIMAL(10,2), col3:VARCHAR(10)> ${defaultValue} """
                waitUntilSchemaChangeDone.call(tableName, "")
            },changeTypeErrMsg)
            // 3.12 add sub-column with json/variant; when in DataType::validateNestedType will throw exception
            def jsonVariantErrMsg="errCode = 2, detailMessage = STRUCT unsupported sub-type"
            for (String type : ["JSON", "VARIANT"]) {
                expectExceptionLike({
                    sql """ alter table ${tableName} modify column c_s_1 STRUCT<col:VARCHAR(30), col1:INT, col2:DECIMAL(10,2), col3:${type}> ${defaultValue} """
                    waitUntilSchemaChangeDone.call(tableName, "")
                },jsonVariantErrMsg)
            }

            // add column with some array cases
            // 1. add array<struct<a1:int>> then modify to array<struct<a1:int, a2:int>>
            // 2. add struct<a1:array<struct<a1:int>>> then modify to struct<a1:array<struct<a1:int, a2:int>>>
            // add column with some struct cases
            // 3. add struct<a1:int, a2:struct<a1:int>> then modify struct<a1:int, a2: struct<a1:int, a2:string, a3:int>>
            // 4. add struct<a1:struct<a1:int>, a2:struct<a1:int>> then modify struct<a1:struct<a1:int, a2:int>, a2: struct<a1:int,a2:string>>
            def insert_sql14 = "insert into ${tableName} values (14, 81.18, named_struct('col','commiter'), named_struct('col','amory8','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory8', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 12.2), array(named_struct('a1', 1)))"
            sql """ alter table ${tableName} add column c_a ARRAY<STRUCT<a1:INT>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql14)
            qt_sql_after6 """ select * from ${tableName} where c0 = 14; """

            def insert_sql15 = "insert into ${tableName} values (15, 91.19, named_struct('col','commiter'), named_struct('col','amory9','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory9', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 1, 'a2', 2)))"
            sql """ alter table ${tableName} modify column c_a ARRAY<STRUCT<a1:INT, a2:INT>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql15)
            qt_sql_after7 """ select * from ${tableName} where c0 = 15; """

            def insert_sql16 = "insert into ${tableName} values (16, 100.01, named_struct('col','commiter'), named_struct('col','amory10','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory10', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 3, 'a2', 4)), named_struct('a1', array(named_struct('a1', 3))))"
            sql """ alter table ${tableName} add column c_s_a STRUCT<a1:ARRAY<STRUCT<a1:INT>>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql16)
            qt_sql_after8 """ select * from ${tableName} where c0 = 16; """

            def insert_sql17 = "insert into ${tableName} values (17, 110.11, named_struct('col','commiter'), named_struct('col','amory11','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory11', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 3, 'a2', 4)), named_struct('a1', array(named_struct('a1', 3, 'a2', 4))))"
            sql """ alter table ${tableName} modify column c_s_a STRUCT<a1:ARRAY<STRUCT<a1:INT, a2:INT>>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql17)
            qt_sql_after9 """ select * from ${tableName} where c0 = 17; """

            def insert_sql18 = "insert into ${tableName} values (18, 120.21, named_struct('col','commiter'), named_struct('col','amory12','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory12', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 5, 'a2', 6)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5)))"
            sql """ alter table ${tableName} ADD COLUMN c_s_s STRUCT<a1:INT, a2:STRUCT<a1:INT>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql18)
            qt_sql_after10 """ select * from ${tableName} where c0 = 18; """

            def insert_sql19 = "insert into ${tableName} values (19, 130.31, named_struct('col','commiter'), named_struct('col','amory13','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory13', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 5, 'a2', 6)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)))"
            sql """ alter table ${tableName} modify column c_s_s struct<a1:int, a2: struct<a1:int, a2:string, a3:int>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql19)
            qt_sql_after11 """ select * from ${tableName} where c0 = 19; """

            def insert_sql20 = "insert into ${tableName} values (20, 140.41, named_struct('col','commiter'), named_struct('col','amory14','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory14', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7), 'a2', named_struct('a1', 8)))"
            sql """ alter table ${tableName} add column c_s_2 STRUCT<a1:STRUCT<a1:INT>, a2:STRUCT<a1:INT>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql20)
            qt_sql_after12 """ select * from ${tableName} where c0 = 20; """

            def insert_sql21 = "insert into ${tableName} values (21, 150.51, named_struct('col','commiter'), named_struct('col','amory15','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory15', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 1), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"
            sql """ alter table ${tableName} modify column c_s_2 STRUCT<a1:STRUCT<a1:INT, a2:INT>, a2: STRUCT<a1:INT,a2:STRING>> ${defaultValue}"""
            waitUntilSchemaChangeDone.call(tableName, insert_sql21)
            qt_sql_after13 """ select * from ${tableName} where c0 = 21; """

        }

        // test nullable to not nullable
        // desc for c_s_1
        for (int idx = 0; idx < tableNames.size() - 1; idx++) {
            String table_name = tableNames[idx]
            String[][] descRes = sql """ desc ${table_name} """
            logger.info(descRes[5][2])
            assertEquals(descRes[5][2].toString().toLowerCase(), "yes")
            def insert_sql_22 = "insert into ${table_name} values (22, 160.61, named_struct('col','commiter'), named_struct('col','amory16','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory16', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 12.21), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"
            def changeNull2NotNull="errCode = 2, detailMessage = Can not change from nullable to non-nullable"
            expectExceptionLike({
                sql """ alter table ${table_name} modify column c_s_1 struct<col:varchar(30),col1:int,col2:decimal(10,2)> NOT NULL """
                waitUntilSchemaChangeDone.call(table_name, insert_sql_22)
            },changeNull2NotNull)
            waitUntilSchemaChangeDone.call(table_name, insert_sql_22)
        }

        // but we support not nullable to nullable
        for (int idx = 0; idx < tableNames.size()-1; idx++) {
            String table_name = tableNames[idx]
            String[][] descRes = sql """ desc ${table_name} """
            logger.info(descRes[2][2])
            assertEquals(descRes[2][2].toString().toLowerCase(), "no")
            // sc do not support new struct type with not null.
            def changeNotNullErr = "Struct type column default value just support null"
            expectExceptionLike({
                sql """ alter table ${table_name} add column c_s_not_null1 struct<col:varchar(30),col1:int,col2:decimal(10,2)> NOT NULL default '{}'"""
                waitUntilSchemaChangeDone.call(table_name, "")
            }, changeNotNullErr)

            // alter origin table column struct not_null to noll
            // insert null data
            def insert_sql_23  = "insert into ${table_name} values (23, 160.61, null, named_struct('col','amory16','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory16', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 12.21), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"
            sql """ alter table ${table_name} modify column c_s_not_null STRUCT<col:VARCHAR(10)> NULL"""
            waitUntilSchemaChangeDone.call(table_name, insert_sql_23)
            qt_sql_after14 """ select * from ${table_name} where c0 = 23; """
            descRes = sql """ desc ${table_name} """
            logger.info(descRes[2][2])
            assertEquals(descRes[2][2].toString().toLowerCase(), "yes")
            // insert some data
            sql "insert into ${table_name} values (24, 160.61, named_struct('col','amory'), named_struct('col','amory16','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory16', named_struct('col','amoryIsBetter', 'col1', 1, 'col2', 12.21), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"
            qt_sql_after15 """ select * from ${table_name} where c0 = 24; """
        }

        // test agg table for not change agg type
        // desc for c_s_1
        String[][] res = sql """ desc ${tableNames[2]} """
        logger.info(res[5][1])
        assertEquals(res[5][1].toLowerCase(),"struct<col:varchar(30),col1:int,col2:decimal(10,2)>")

        test {
            sql """ alter table ${tableNames[2]} modify column c_s_1 struct<col:varchar(30),col1:int,col2:decimal(10,2)> REPLACE """
            exception "Can not change aggregation type"
        }


        /////////////// compaction behavior ///////////////
        for (int idx = 0; idx < tableNames.size(); idx++) {
            String table_name = tableNames[idx]
            String[][] descRes = sql """ desc ${table_name} """
            logger.info(descRes[0][1])
            assertEquals(descRes[0][1].toLowerCase(),"largeint")
            logger.info(descRes[3][1])
            assertEquals(descRes[3][1].toLowerCase(),"struct<col:varchar(10),col1:int,col2:decimal(10,2),col3:datetime,col4:array<text>,col5:map<int,text>,col6:struct<a:int,b:text>,col11:int,col12:decimal(10,2),col13:datetime,col14:array<text>,col15:map<int,text>,col16:struct<a:int,b:text>>")
            logger.info(descRes[4][1])
            assertEquals(descRes[4][1].toLowerCase(),"text")
            logger.info(descRes[5][1])
            assertEquals(descRes[5][1].toLowerCase(),"struct<col:varchar(30),col1:int,col2:decimal(10,2)>")
            // 1. insert more data
            sql """ insert into ${table_name} values (25, 81.18, named_struct('col','amory'), named_struct('col','amory8','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory8', named_struct('col','amoryMoreMore30Better', 'col1', 1, 'col2', 1.1), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"""
            sql """ insert into ${table_name} values (26, 91.19, named_struct('col','amory'), named_struct('col','amory9','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory9', named_struct('col','amoryMoreMore30Better', 'col1', 1, 'col2', 1.1), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"""
            sql """ insert into ${table_name} values (27, 10.01, named_struct('col','amory'),named_struct('col','amory10','col1', 1, 'col2', 1.1, 'col3', '2021-01-01 00:00:00', 'col4', ['a', 'b'], 'col5', {1:'a', 2:'b'}, 'col6', {1, 'a'}, 'col11', 2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 'col14', ['a', 'b'], 'col15', {1:'a', 2:'b'}, 'col16', {1, 'a'}), 'amory10', named_struct('col','amoryMoreMore30Better', 'col1', 1, 'col2', 1.1), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory')))"""
            sql """ insert into ${table_name} 
                                                    values 
                                                      (
                                                        28, 
                                                        11.11, 
                                                        named_struct('col','amory'),
                                                        named_struct(
                                                          'col', 'amory11', 'col1', 1, 'col2', 
                                                          1.1, 'col3', '2021-01-01 00:00:00', 
                                                          'col4', [ 'a', 'b' ], 'col5', {1 : 'a', 
                                                          2 : 'b' }, 'col6', {1, 'a' }, 'col11', 
                                                          2, 'col12', 2.1, 'col13', '2021-01-01 00:00:00', 
                                                          'col14', [ 'a', 'b' ], 'col15', {1 : 'a', 
                                                          2 : 'b' }, 'col16', {1, 'a' }
                                                        ), 
                                                        'amory11', 
                                                        named_struct(
                                                          'col', 'amoryMoreMore30Better', 'col1', 1,
                                                          'col2', 1.1
                                                        ), array(named_struct('a1', 7, 'a2', 8)), named_struct('a1', array(named_struct('a1', 5, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory'))
                                                      ), 
                                                      (29, 12.21, null, null, null, null, null, null, null, null), 
                                                      (30, 13.31, named_struct('col','amory'), named_struct(
                                                          'col', null, 'col1', null, 'col2', 
                                                           null, 'col3', null, 
                                                          'col4', null, 'col5', null, 'col6', null, 'col11', 
                                                          null, 'col12', null, 'col13', null, 
                                                          'col14', null, 'col15', null, 'col16', null
                                                      ), 
                                                      "", 
                                                      named_struct(
                                                          'col', null, 'col1', null, 'col2', 
                                                          null
                                                      ),  array(named_struct('a1', null, 'a2', 8)), named_struct('a1', array(named_struct('a1', null, 'a2', 6))), named_struct('a1', 5, 'a2', named_struct('a1', 5, 'a2', 'amory', 'a3', 6)), named_struct('a1', named_struct('a1', 7, 'a2', 8), 'a2', named_struct('a1', 8, 'a2', 'amory'))), 
                                                      (31, 14.41, null, null, "amory14",
                                                      named_struct(
                                                          'col', 'amoryMoreMore30Better', 'col1', null, 'col2',
                                                          null
                                                      ), array(named_struct('a1', 9, 'a2', 10)), null, null, null) """
            // 2. check insert res
            qt_sql_after6 """ select * from ${table_name} order by c0; """
            // 3. check compaction
            //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            // trigger compactions for all tablets in ${tableName}
            trigger_and_wait_compaction(tableName, "cumulative")
            int rowCount = 0
            for (def tablet in tablets) {
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    rowCount += Integer.parseInt(rowset.split(" ")[1])
                }
            }
            logger.info("rowCount: " + rowCount)
            // check res
            qt_sql_after7 """ select * from ${table_name} order by c0; """

        }

    } finally {
//        for (String tb : tableNames) {
//            try_sql("DROP TABLE IF EXISTS ${tb}")
//        }
    }

}
