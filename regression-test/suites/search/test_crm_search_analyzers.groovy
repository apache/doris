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

suite("test_crm_search_analyzers") {
    // Chapters X and XI: a normalized keyword index and a full-text index.
    sql "DROP TABLE IF EXISTS crm_search_analyzers"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS crm_doc_text"
    sql "DROP INVERTED INDEX ANALYZER IF EXISTS crm_doc_exact"
    sql "DROP INVERTED INDEX TOKEN_FILTER IF EXISTS crm_doc_nfkc"
    sql """CREATE INVERTED INDEX TOKEN_FILTER crm_doc_nfkc
        PROPERTIES("type"="icu_normalizer", "name"="nfkc")"""
    sql """CREATE INVERTED INDEX ANALYZER crm_doc_text
        PROPERTIES("tokenizer"="standard", "token_filter"="crm_doc_nfkc,lowercase")"""
    sql """CREATE INVERTED INDEX ANALYZER crm_doc_exact
        PROPERTIES("tokenizer"="keyword", "token_filter"="crm_doc_nfkc,lowercase")"""
    // Analyzer policies reach BE asynchronously, as in the existing multi-analyzer suites.
    sleep(10000)
    sql """CREATE TABLE crm_search_analyzers (
        id BIGINT, name TEXT, title TEXT,
        INDEX idx_name_text(name) USING INVERTED
            PROPERTIES("analyzer"="crm_doc_text", "support_phrase"="true"),
        INDEX idx_name_exact(name) USING INVERTED PROPERTIES("analyzer"="crm_doc_exact"),
        INDEX idx_title_text(title) USING INVERTED
            PROPERTIES("analyzer"="crm_doc_text", "support_phrase"="true")
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num"="1", "inverted_index_storage_format"="V2")"""
    sql """INSERT INTO crm_search_analyzers VALUES
        (1,'John Smith','Senior Software Engineer'),
        (2,'ＪＯＨＮ Smith','machine learning'),
        (3,'John Smith Junior','software'),
        (4,'Other','john smith'),(5,NULL,NULL)"""
    order_qt_document_10_exact """
        SELECT id FROM crm_search_analyzers
        WHERE name MATCH 'John Smith' USING ANALYZER crm_doc_exact
    """
    order_qt_document_11_single """
        SELECT id FROM crm_search_analyzers
        WHERE search('title@crm_doc_text:"machine learning"')
    """
    order_qt_document_11_mixed """
        SELECT id FROM crm_search_analyzers
        WHERE search('name@crm_doc_exact:"John Smith" AND title@crm_doc_text:software')
    """
    order_qt_document_11_fields """
        SELECT id FROM crm_search_analyzers
        WHERE search('john smith', '{"fields":["name@crm_doc_exact","title@crm_doc_text"]}')
    """
    // Chapter XI's same-field dual-analyzer example is outside the per-field P2 scope.
    test {
        sql """SELECT id FROM crm_search_analyzers
            WHERE search('name@crm_doc_text:John AND name@crm_doc_exact:"John Smith"')"""
        exception "one analyzer per field"
    }
    order_qt_document_11_separate_search """
        SELECT id FROM crm_search_analyzers
        WHERE search('name@crm_doc_text:John')
          AND search('name@crm_doc_exact:"John Smith"')
    """
    test {
        sql """SELECT id FROM crm_search_analyzers WHERE search('name@does_not_exist:John')"""
        exception "No inverted index found for SEARCH analyzer"
    }
    // Chapter XIII is outside P0-P2. Ordinary IN must keep SQL equality semantics.
    order_qt_document_13_ordinary_in """
        SELECT id FROM crm_search_analyzers WHERE name IN ('John Smith','Mason Jackson')
    """
    test {
        sql """SELECT id FROM crm_search_analyzers
            WHERE name@crm_doc_exact IN ('John Smith','Mason Jackson')"""
        exception "mismatched input '@'"
    }
    test {
        sql """SELECT id FROM crm_search_analyzers
            WHERE name USING analyzer 'crm_doc_exact' IN ('John Smith','Mason Jackson')"""
        exception "mismatched input 'USING'"
    }
    // A quoted @ remains part of the physical field name.
    sql "DROP TABLE IF EXISTS crm_search_literal_fields"
    sql """CREATE TABLE crm_search_literal_fields (id INT, `name@literal` TEXT, v VARIANT,
        INDEX idx_name(`name@literal`) USING INVERTED PROPERTIES("parser"="english"),
        INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="english"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_literal_fields VALUES
        (1,'john',parse_to_variant('{"email@work":"john"}')),(2,'jane',parse_to_variant('{"email@work":"jane"}'))"""
    order_qt_literal_column_at """
        SELECT id FROM crm_search_literal_fields WHERE search('"name@literal":john')
    """
    order_qt_literal_variant_at """
        SELECT id FROM crm_search_literal_fields WHERE search('"v.email@work":john')
    """

}
