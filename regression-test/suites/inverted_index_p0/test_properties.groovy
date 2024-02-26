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


suite("test_properties", "p0"){
    // prepare test table
    def indexTblName = "test_properties"

    sql "DROP TABLE IF EXISTS ${indexTblName}"
    def success = false;

    def create_table_with_inverted_index_properties = { create_sql, error_msg->
        try {
            sql create_sql
            success = true;
        } catch(Throwable ex) {
            logger.info("create_table_with_inverted_index_properties result: " + ex)
            if (ex != null) {
                def msg = ex.toString()
                assertTrue(msg != null && msg.contains(error_msg), "Expect exception msg contains '${error_msg}', but meet '${msg}'")
            }
            success = false;
        } finally {
            sql "DROP TABLE IF EXISTS ${indexTblName}"
        }
    }

    def empty_parser = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
        	`id`int(11)NULL,
        	`c` text NULL,
        	INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
        	"replication_allocation" = "tag.location.default: 1"
        );
        """
    create_table_with_inverted_index_properties(empty_parser, "Invalid inverted index 'parser' value")
    assertEquals(success, false)

    def wrong_parser = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
        	`id`int(11)NULL,
        	`c` text NULL,
        	INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="german") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
        	"replication_allocation" = "tag.location.default: 1"
        );
        """
    create_table_with_inverted_index_properties(wrong_parser, "Invalid inverted index 'parser' value")
    assertEquals(success, false)

    def wrong_parser_mode = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "parser_mode"="fine_grained") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(wrong_parser_mode, "parser_mode is only available for chinese parser")
    assertEquals(success, false)

    def valid_parser_and_mode = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="chinese", "parser_mode"="fine_grained") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(valid_parser_and_mode, "")
    assertEquals(success, true)

    def missing_char_filter_pattern = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "char_filter_type"="char_replace") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(missing_char_filter_pattern, "Missing 'char_filter_pattern' for 'char_replace' filter type")
    assertEquals(success, false)

    def invalid_property_key = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("invalid_key"="value") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_property_key, "Invalid inverted index property key:")
    assertEquals(success, false)

    def invalid_property_key2 = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "invalid_key"="value") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_property_key2, "Invalid inverted index property key:")
    assertEquals(success, false)

    def invalid_ignore_above = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "ignore_above"="-1") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_ignore_above, "Invalid inverted index 'ignore_above' value")
    assertEquals(success, false)

    def non_numeric_ignore_above = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("ignore_above"="non_numeric") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(non_numeric_ignore_above, "ignore_above must be integer")
    assertEquals(success, false)

    def invalid_lower_case = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "lower_case"="invalid") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_lower_case, "Invalid inverted index 'lower_case' value")
    assertEquals(success, false)

    def invalid_support_phrase = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "support_phrase"="invalid") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_support_phrase, "Invalid inverted index 'support_phrase' value")
    assertEquals(success, false)

    def invalid_support_phrase2 = """
        CREATE TABLE IF NOT EXISTS ${indexTblName}(
            `id` int(11) NULL,
            `c` text NULL,
            INDEX c_idx(`c`) USING INVERTED PROPERTIES("parser"="english", "support_phase"="true") COMMENT ''
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES(
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    create_table_with_inverted_index_properties(invalid_support_phrase2, "Invalid inverted index property key: support_phase")
    assertEquals(success, false)
}
