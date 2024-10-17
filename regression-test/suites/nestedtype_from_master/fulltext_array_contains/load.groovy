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

suite("load") {
    sql """ ADMIN SET FRONTEND CONFIG ("enable_create_inverted_index_for_array" = "true"); """
    def test_basic_tables=["articles_uk_array", "articles_dk_array"]
    def test_fulltext_tables=["fulltext_t1_uk_array", "fulltext_t1_dk_array"]

    def test_join_tables_t1=["join_t1_uk_array", "join_t1_dk_array"]
    def test_join_tables_t2=["join_t2_uk_array", "join_t2_dk_array"]

    def test_large_records_tables_1=["large_records_t1_uk_array", "large_records_t1_dk_array"]
    def test_large_records_tables_2=["large_records_t2_uk_array", "large_records_t2_dk_array"]
    def test_large_records_tables_3=["large_records_t3_uk_array", "large_records_t3_dk_array"]
    def test_large_records_tables_4=["large_records_t4_uk_array", "large_records_t4_dk_array"]

    for (String table in test_basic_tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in test_basic_tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_basic_tables) {
        sql """ INSERT INTO ${table} VALUES
                (1, ['MySQL Tutorial'], ['DBMS stands for DataBase ...']),
                (2, ['How To Use MySQL Well'], ['After you went through a ...']),
                (3, ['Optimizing MySQL'], ['In this tutorial we will show ...']),
                (4, ['1001 MySQL Tricks'], ['1. Never run mysqld as root. 2. ...']),
                (5, ['MySQL vs. YourSQL'], ['In the following database comparison ...']),
                (6, ['MySQL Security'], ['When configured properly, MySQL ...']);
            """
    }

    for (String table in test_fulltext_tables) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in test_fulltext_tables) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_fulltext_tables) {
        sql """ INSERT INTO ${table} VALUES
                ('MySQL has now support', ['for full-text search']),
                ('Full-text indexes', ['are called collections']),
                ('Only MyISAM tables', ['support collections']),
                ('Function MATCH ... AGAINST()', ['is used to do a search']),
                ('Full-text search in MySQL', ['implements vector space model']);
            """
    }

    for (String table in test_join_tables_t1) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in test_join_tables_t1) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_join_tables_t2) {
        sql """ DROP TABLE IF EXISTS $table """
    }

    for (String table in test_join_tables_t2) {
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_join_tables_t1) {
        sql """ INSERT INTO ${table} VALUES(1, 'a1', '2003-05-23 19:30:00'); """
        sql """ INSERT INTO ${table} VALUES(null, 'a2', '2003-05-23 19:30:00'); """
    }

    for (String table in test_join_tables_t2) {
        sql """ INSERT INTO ${table} VALUES(1, ['aberdeen town hall']),
                (2, ['glasgow royal concert hall']),
                (3, ["queen\'s hall, edinburgh"]); """
    }

    for (String table in test_large_records_tables_1) {
        sql """ DROP TABLE IF EXISTS $table """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_large_records_tables_2) {
        sql """ DROP TABLE IF EXISTS $table """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_large_records_tables_3) {
        sql """ DROP TABLE IF EXISTS $table """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String table in test_large_records_tables_4) {
        sql """ DROP TABLE IF EXISTS $table """
        sql new File("""${context.file.parent}/ddl/${table}.sql""").text
    }

    for (String tableName in test_large_records_tables_1) {
       streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'timeout', '72000'
            set 'columns', 'FTS_DOC_ID,a,tmpb,b=split_by_string(tmpb,\'\t\')'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file 'fts_input_data1.csv'
            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    for (String tableName in test_large_records_tables_2) {
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'timeout', '72000'
            set 'columns', 'FTS_DOC_ID,a,tmpb,b=split_by_string(tmpb,\' \')'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file 'fts_input_data2.csv'
            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    for (String tableName in test_large_records_tables_3) {
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'timeout', '72000'
            set 'columns', 'FTS_DOC_ID,a,tmpb,b=split_by_string(tmpb,\' \')'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file 'fts_input_data3.csv'
            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    for (String tableName in test_large_records_tables_4) {
        streamLoad {
            // you can skip db declaration, because a default db has already been
            // specified in ${DORIS_HOME}/conf/regression-conf.groovy
            // db 'regression_test'
            table tableName

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', ','
            set 'timeout', '72000'
            set 'columns', 'FTS_DOC_ID,a,tmpb,b=split_by_string(tmpb,\' \')'
            // relate to ${DORIS_HOME}/regression-test/data/demo/streamload_input.csv.
            // also, you can stream load a http stream, e.g. http://xxx/some.csv
            file 'fts_input_data4.csv'
            time 10000 // limit inflight 10s

            // stream load action will check result, include Success status, and NumberTotalRows == NumberLoadedRows

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }
}
