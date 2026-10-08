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

// Verify SNII indexes written through the VARIANT V2 writer.
suite("test_variant_v2_snii_index", "p0,nonConcurrent") {
    sql "SET enable_common_expr_pushdown=true"
    sql "SET enable_common_expr_pushdown_for_inverted_index=true"
    setFeConfigTemporary([enable_variant_v2: true]) {
        def tblName = "variant_v2_snii_index"
        def typedInventors = "cast(inventors['inventors'] as array<text>)"

        // Match the session subcolumn limit to parse_to_variant() output.
        sql """ set default_variant_max_subcolumns_count = 0 """
        sql """ set default_variant_enable_doc_mode = false """

        sql "DROP TABLE IF EXISTS ${tblName}"
        sql """
            CREATE TABLE ${tblName} (
                apply_date date NULL,
                id varchar(60) NOT NULL,
                inventors variant< MATCH_NAME 'inventors' : array<text> > NULL,
                INDEX idx_inventors(inventors) USING INVERTED
                    PROPERTIES("field_pattern" = "inventors", "support_phrase" = "true",
                               "lower_case" = "true") COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(apply_date, id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "storage_format" = "V2",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "SNII"
            );
        """

        // parse_to_variant() selects the V2 writer.
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'a', parse_to_variant('{"inventors":["alpha","beta"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'b', parse_to_variant('{"inventors":["gamma"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'c', parse_to_variant('{"inventors":["alpha","delta"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2019-01-01', 'd', parse_to_variant('{"inventors":["epsilon"]}')) """

        qt_row_count """ SELECT count() FROM ${tblName} """

        // The debug point rejects queries that fall back to row scanning.
        def withIndexOnlyEnforced = { Closure body ->
            try {
                GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
                sql "sync"
                body()
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
            }
        }

        // Check matching, nonmatching, and overlapping terms.
        withIndexOnlyEnforced {
            order_qt_alpha """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'alpha') ORDER BY id """
            order_qt_gamma """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'gamma') ORDER BY id """
            order_qt_absent """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'zzz') ORDER BY id """
            order_qt_overlap """ SELECT id FROM ${tblName} WHERE arrays_overlap(${typedInventors}, cast(['beta','epsilon'] as array<text>)) ORDER BY id """
        }

        sql "DROP TABLE IF EXISTS ${tblName}"
    }
}
