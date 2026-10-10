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

// Force multiple CLucene partitions while keeping query bitmaps in the segment document domain.
suite("test_search_segment_document_ids", "p0,nonConcurrent") {
    sql "SET default_variant_doc_materialization_min_rows = 0"
    setBeConfigTemporary([inverted_index_max_buffered_docs: "2"]) {
        for (String format : ["V2", "SNII"]) {
            sql "DROP TABLE IF EXISTS test_search_segment_document_ids"
            sql """
                CREATE TABLE test_search_segment_document_ids (
                    id INT NOT NULL,
                    title STRING,
                    body STRING,
                    props VARIANT<PROPERTIES("variant_max_subcolumns_count"="0")>,
                    INDEX idx_title(title) USING INVERTED PROPERTIES(
                        "parser"="unicode", "support_phrase"="true"),
                    INDEX idx_body(body) USING INVERTED PROPERTIES(
                        "parser"="unicode", "support_phrase"="true"),
                    INDEX idx_props(props) USING INVERTED PROPERTIES(
                        "parser"="unicode", "support_phrase"="true")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num"="1",
                    "inverted_index_storage_format"="${format}"
                )
            """
            sql """
                INSERT INTO test_search_segment_document_ids VALUES
                (0, 'fleabag premiere', 'other', parse_to_variant('{"known":"other"}')),
                (1, 'other title', 'other', parse_to_variant('{"known":"other"}')),
                (2, 'history text', NULL, parse_to_variant('{"known":"other"}')),
                (3, 'fleabag finale', 'selected', parse_to_variant('{"known":"selected"}'))
            """
            sql "SYNC"

            quickTest("${format}_and", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:fleabag AND body:selected', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_or", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:fleabag OR body:selected', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_not_or", """
                SELECT id FROM test_search_segment_document_ids
                WHERE NOT search('title:fleabag OR body:selected', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_missing_or", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:fleabag OR props.missing:value', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_missing_and", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:fleabag AND props.missing:value', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_missing_not_and", """
                SELECT id FROM test_search_segment_document_ids
                WHERE NOT search('title:fleabag AND props.missing:value', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_phrase", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:"fleabag finale"', '{"mode":"standard"}') ORDER BY id
            """)
            quickTest("${format}_variant_and", """
                SELECT id FROM test_search_segment_document_ids
                WHERE search('title:fleabag AND props.known:selected', '{"mode":"standard"}') ORDER BY id
            """)
        }
    }
}
