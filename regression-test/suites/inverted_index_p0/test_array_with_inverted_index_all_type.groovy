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

suite("test_array_with_inverted_index_all_type"){
    // 1. mor|dup|mow
    // 2. index with english or standard parser (only string | variant type support english parser now)
    // 3. all types
    // prepare test table
    def indexTblNames = ["ai_all_type_dup_with_standard_parser", "ai_all_type_dup_with_english_parser",
                                        "ai_all_type_mor_with_standard_parser", "ai_all_type_mor_with_english_parser",
                                        "ai_all_type_mow_with_standard_parser", "ai_all_type_mow_with_english_parser"]
    def colNameArr = ['c_bool',
                      "c_tinyint", "c_smallint", "c_int", "c_bigint",
                       "c_decimal", "c_date", "c_datetime", "c_datev2", "c_datetimev2",
                      "c_char", "c_varchar", "c_string"]
    def colNameArrAgg = ["c_date", "c_char", "c_varchar", "c_string"]
    def dataFile = """test_array_with_inverted_index_all_type.json"""
    def dataFileAgg = """test_array_with_inverted_index_all_type_agg.json"""
    sql """ set enable_profile = true;"""
    // If we use common expr pass to inverted index , we should set enable_common_expr_pushdown = true
    sql """ set enable_common_expr_pushdown = true; """
    sql """ set enable_common_expr_pushdown_for_inverted_index = true; """

    // duplicate key table with all type using standard parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[0]}"
    sql """
	CREATE TABLE IF NOT EXISTS `${indexTblNames[0]}` (
                k1 INT,
                c_bool ARRAY<BOOLEAN>,
                c_tinyint ARRAY<TINYINT>,
                c_smallint ARRAY<SMALLINT>,
                c_int ARRAY<INT>,
                c_bigint ARRAY<BIGINT>,
                c_decimal ARRAY<DECIMAL(20, 3)>,
                c_date ARRAY<DATE>,
                c_datetime ARRAY<DATETIME>,
                c_datev2 ARRAY<DATEV2>,
                c_datetimev2 ARRAY<DATETIMEV2>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_bool(c_bool) USING INVERTED COMMENT 'c_bool index',
                INDEX index_inverted_c_tinyint(c_tinyint) USING INVERTED COMMENT 'c_tinyint index',
                INDEX index_inverted_c_smallint(c_smallint) USING INVERTED COMMENT 'c_smallint index',
                INDEX index_inverted_c_int(c_int) USING INVERTED COMMENT 'c_int index',
                INDEX index_inverted_c_bigint(c_bigint) USING INVERTED COMMENT 'c_bigint index',
                INDEX index_inverted_c_decimal(c_decimal) USING INVERTED COMMENT 'c_decimal index',
                INDEX index_inverted_c_date(c_date) USING INVERTED COMMENT 'c_date index',
                INDEX index_inverted_c_datetime(c_datetime) USING INVERTED COMMENT 'c_datetime index',
                INDEX index_inverted_c_datev2(c_datev2) USING INVERTED COMMENT 'c_datev2 index',
                INDEX index_inverted_c_datetimev2(c_datetimev2) USING INVERTED COMMENT 'c_datetimev2 index',
                INDEX index_inverted_c_char(c_char) USING INVERTED COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED COMMENT 'c_string index'
    ) ENGINE=OLAP
    DUPLICATE KEY(`k1`)
    COMMENT 'OLAP'
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """
    
    // duplicate key table with all type using english parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[1]}"
    sql """ 
            CREATE TABLE IF NOT EXISTS `${indexTblNames[1]}` (
                k1 INT,
                c_date ARRAY<DATE>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_char(c_char) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_string index'
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 3
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "is_being_synced" = "false",
            "storage_format" = "V2",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false"
            );
    """

    // mor key table with all type using standard parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[2]}"
    sql """
    CREATE TABLE IF NOT EXISTS `${indexTblNames[2]}` (
                k1 INT,
                c_bool ARRAY<BOOLEAN>,
                c_tinyint ARRAY<TINYINT>,
                c_smallint ARRAY<SMALLINT>,
                c_int ARRAY<INT>,
                c_bigint ARRAY<BIGINT>,
                c_decimal ARRAY<DECIMAL(20, 3)>,
                c_date ARRAY<DATE>,
                c_datetime ARRAY<DATETIME>,
                c_datev2 ARRAY<DATEV2>,
                c_datetimev2 ARRAY<DATETIMEV2>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_bool(c_bool) USING INVERTED COMMENT 'c_bool index',
                INDEX index_inverted_c_tinyint(c_tinyint) USING INVERTED COMMENT 'c_tinyint index',
                INDEX index_inverted_c_smallint(c_smallint) USING INVERTED COMMENT 'c_smallint index',
                INDEX index_inverted_c_int(c_int) USING INVERTED COMMENT 'c_int index',
                INDEX index_inverted_c_bigint(c_bigint) USING INVERTED COMMENT 'c_bigint index',
                INDEX index_inverted_c_decimal(c_decimal) USING INVERTED COMMENT 'c_decimal index',
                INDEX index_inverted_c_date(c_date) USING INVERTED COMMENT 'c_date index',
                INDEX index_inverted_c_datetime(c_datetime) USING INVERTED COMMENT 'c_datetime index',
                INDEX index_inverted_c_datev2(c_datev2) USING INVERTED COMMENT 'c_datev2 index',
                INDEX index_inverted_c_datetimev2(c_datetimev2) USING INVERTED COMMENT 'c_datetimev2 index',
                INDEX index_inverted_c_char(c_char) USING INVERTED COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED COMMENT 'c_string index'
    ) ENGINE=OLAP
    UNIQUE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """
    
    // mor key table with all type using english parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[3]}"
    sql """
    CREATE TABLE IF NOT EXISTS `${indexTblNames[3]}` (
                k1 INT,
                c_date ARRAY<DATE>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_char(c_char) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_string index'
    ) ENGINE=OLAP
    UNIQUE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false"
    );
    """
    
    // mow key table with all type using standard parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[4]}"
    sql """
    CREATE TABLE IF NOT EXISTS `${indexTblNames[4]}` (
                k1 INT,
                c_bool ARRAY<BOOLEAN>,
                c_tinyint ARRAY<TINYINT>,
                c_smallint ARRAY<SMALLINT>,
                c_int ARRAY<INT>,
                c_bigint ARRAY<BIGINT>,
                c_decimal ARRAY<DECIMAL(20, 3)>,
                c_date ARRAY<DATE>,
                c_datetime ARRAY<DATETIME>,
                c_datev2 ARRAY<DATEV2>,
                c_datetimev2 ARRAY<DATETIMEV2>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_bool(c_bool) USING INVERTED COMMENT 'c_bool index',
                INDEX index_inverted_c_tinyint(c_tinyint) USING INVERTED COMMENT 'c_tinyint index',
                INDEX index_inverted_c_smallint(c_smallint) USING INVERTED COMMENT 'c_smallint index',
                INDEX index_inverted_c_int(c_int) USING INVERTED COMMENT 'c_int index',
                INDEX index_inverted_c_bigint(c_bigint) USING INVERTED COMMENT 'c_bigint index',
                INDEX index_inverted_c_decimal(c_decimal) USING INVERTED COMMENT 'c_decimal index',
                INDEX index_inverted_c_date(c_date) USING INVERTED COMMENT 'c_date index',
                INDEX index_inverted_c_datetime(c_datetime) USING INVERTED COMMENT 'c_datetime index',
                INDEX index_inverted_c_datev2(c_datev2) USING INVERTED COMMENT 'c_datev2 index',
                INDEX index_inverted_c_datetimev2(c_datetimev2) USING INVERTED COMMENT 'c_datetimev2 index',
                INDEX index_inverted_c_char(c_char) USING INVERTED COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED COMMENT 'c_string index'
    ) ENGINE=OLAP
    UNIQUE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "enable_unique_key_merge_on_write" = "true"
    );
    """
    
    // mow key table with all type using english parser for inverted index
    sql "DROP TABLE IF EXISTS ${indexTblNames[5]}"
    sql """
    CREATE TABLE IF NOT EXISTS `${indexTblNames[5]}` (
                k1 INT,
                c_date ARRAY<DATE>,
                c_char ARRAY<CHAR>,
                c_varchar ARRAY<VARCHAR(1)>,
                c_string ARRAY<STRING>,
                INDEX index_inverted_c_char(c_char) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_char index',
                INDEX index_inverted_c_varchar(c_varchar) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_varchar index',
                INDEX index_inverted_c_string(c_string) USING INVERTED PROPERTIES("parser"="none") COMMENT 'c_string index'
    ) ENGINE=OLAP
    UNIQUE KEY(`k1`)
    DISTRIBUTED BY HASH(`k1`) BUCKETS 3
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "enable_unique_key_merge_on_write" = "true"
    );
    """
    
          
    
    
    def StreamLoad = { tableName, agg ->
        streamLoad {
            table tableName
            file agg ? dataFileAgg : dataFile
            time 60000
            set 'read_json_by_line', 'true'
            set 'format', 'json'

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals(101, json.NumberTotalRows)
                assertEquals(101, json.NumberLoadedRows)
            }
        }
    }

    // stream load with table
    for (int i = 0; i < 6; i++) {
        if (i % 2 == 0) {
            StreamLoad.call(indexTblNames[i], false)
        } else {
            StreamLoad.call(indexTblNames[i], true)
        }
    }

    // query test
    sql """ set enable_common_expr_pushdown = true """

    for (int i = 0; i < 6; i+=1) {
        def indexTblName = indexTblNames[i]
        // count
        qt_sql_count """ select count() from ${indexTblName};"""
        if (i % 2 == 0) {
            // array_contains
            for (String col : colNameArr) {
                def res = sql """select ${col}[32] from ${indexTblName} order by k1"""
                logger.info("res ${res}[1]")
                def param = res[1][0]
                qt_sql_array_contains """ select k1, array_position($col, '$param') from ${indexTblName} where array_contains(${col}, '${param}') order by k1; """
                qt_sql_array_contains_null """ select k1, array_position($col, '$param') from ${indexTblName} where array_contains(${col}, null) order by k1; """
            }
        } else {
            // array_contains
            for (String col : colNameArrAgg) {
                def res = sql """select ${col}[32] from ${indexTblName} order by k1"""
                logger.info("res ${res}[1]")
                def param = res[1][0]
                qt_sql_array_contains """ select k1, array_position($col, '$param') from ${indexTblName} where array_contains($col, '$param') order by k1; """
                qt_sql_array_contains_null """ select k1, array_position($col, '$param') from ${indexTblName} where array_contains($col, null) order by k1; """
            }
        }
        // date
        def result = sql """select c_date[7] from ${indexTblName} order by k1"""
        logger.info("result ${result}[1]")
        def param = result[1][0]
        // or | and | not cases
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where array_contains(c_date, '${param}') and array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where array_contains(c_date, '${param}') or array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where !array_contains(c_date, '${param}') order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where !array_contains(c_date, '${param}') and array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where !array_contains(c_date, '${param}') and array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where !array_contains(c_date, '${param}') or array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where (array_contains(c_date, '${param}') and array_count(x->x IS NULL, c_string)>60) or array_count(x->x IS NULL, c_string)>60 order by k1; """
        order_qt_sql """ select c_date, array_count(x->x IS NULL, c_string) from ${indexTblName} where (array_contains(c_date, '${param}') and array_count(x->x IS NULL, c_string)>60) or !array_count(x->x IS NULL, c_string)>60 order by k1; """
    }
}
