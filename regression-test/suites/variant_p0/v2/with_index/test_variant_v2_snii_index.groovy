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

// A SNII inverted index on a VARIANT column written through the V2 writer.
//
// The other SNII variant suites all go through the V1 writer: DataTypeVariant::create_column()
// returns a ColumnVariant, and it is the column's runtime type -- not a config -- that selects
// V1 or V2 (variant_writer_helpers.cpp, classify_variant_writer_input). Only a ColumnVariantV2,
// which parse_to_variant() produces, reaches VariantV2ColumnWriter. So without this suite the V2
// writer had no SNII coverage at all.
//
// No BE code is SNII-specific on that path: V2 prepares its sub-column writers through the same
// variant_writer_helpers::prepare_subcolumn_writer_target as V1, which calls
// variant_util::inherit_index and hands the shared IndexFileWriter down, and every
// write_inverted_index() ends at IndexColumnWriter::create, which routes on storage format. This
// suite pins that the shared path really does serve SNII, rather than leaving it inferred.
suite("test_variant_v2_snii_index", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        def tblName = "variant_v2_snii_index"
        def typedInventors = "cast(inventors['inventors'] as array<text>)"

        // parse_to_variant() yields a variant with no materialised sub-columns, so the session
        // default has to agree or the insert fails with
        // "Conversion from Variant(max subcolumns count = 0) to Variant(... = 2048)".
        sql """ set default_variant_max_subcolumns_count = 0 """
        sql """ set default_variant_enable_doc_mode = false """
        sql """ set enable_segment_limit_pushdown = true """

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

        // parse_to_variant() is what puts this table on the V2 writer; a plain JSON literal would
        // silently take the V1 path and this suite would stop testing what it claims to.
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'a', parse_to_variant('{"inventors":["alpha","beta"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'b', parse_to_variant('{"inventors":["gamma"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2017-01-01', 'c', parse_to_variant('{"inventors":["alpha","delta"]}')) """
        sql """ INSERT INTO ${tblName} VALUES('2019-01-01', 'd', parse_to_variant('{"inventors":["epsilon"]}')) """

        qt_row_count """ SELECT count() FROM ${tblName} """

        // The debug point errors the query when any predicate survives index application
        // (segment_iterator.cpp, "it is failed to apply inverted index"). A query that returns at
        // all therefore proves the SNII index answered it -- a row-scan fallback would produce the
        // same rows and prove nothing, which is exactly the hole this suite exists to close.
        def withIndexOnlyEnforced = { Closure body ->
            try {
                GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
                sql "sync"
                body()
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
            }
        }

        // 'alpha' appears in two rows, 'gamma' in one, and 'zzz' in none: a broken index that
        // returned everything, or nothing, fails at least one of the three.
        withIndexOnlyEnforced {
            order_qt_alpha """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'alpha') ORDER BY id """
            order_qt_gamma """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'gamma') ORDER BY id """
            order_qt_absent """ SELECT id FROM ${tblName} WHERE array_contains(${typedInventors}, 'zzz') ORDER BY id """
            order_qt_overlap """ SELECT id FROM ${tblName} WHERE arrays_overlap(${typedInventors}, cast(['beta','epsilon'] as array<text>)) ORDER BY id """
        }

        sql "DROP TABLE IF EXISTS ${tblName}"
    }
}
