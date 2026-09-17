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

suite("test_crm_search_variant_topn") {
    // The pipeline randomizes these defaults. A VARIANT subcolumn stored in the sparse or doc column has no
    // inverted index, so SEARCH finds nothing in it and MATCH fails without enable_match_without_inverted_index.
    sql "set default_variant_enable_doc_mode = false"
    sql "set default_variant_enable_typed_paths_to_sparse = false"
    sql "set default_variant_max_subcolumns_count = 0"
    // Chapter XII. More than LIMIT matching rows and absent payload paths.
    sql "DROP TABLE IF EXISTS crm_search_products"
    sql """CREATE TABLE crm_search_products (id BIGINT, v VARIANT,
        INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="english"))
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_products
        SELECT number, parse_to_variant(CONCAT('{"name":"apple","price":', CAST(number AS STRING),
            ',"a":"payload-',CAST(number AS STRING),'","b":',CAST(number*2 AS STRING),'}'))
        FROM numbers("number"="240")"""
    sql """INSERT INTO crm_search_products VALUES
        (240,parse_to_variant('{"name":"banana","price":-100,"a":"excluded"}')),
        (241,parse_to_variant('{"name":"apple","price":-2}')),
        (242,parse_to_variant('{"name":"apple","price":-1,"a":null}'))"""
    // The source's raw VARIANT sort key has no SQL ordering contract.
    // Preserve the original error and make the numeric ordering explicit below.
    test {
        sql """SELECT v['price'], v['a'], v['b'] FROM crm_search_products
            WHERE v['name'] MATCH_ANY 'apple' ORDER BY v['price'] ASC LIMIT 100"""
        exception "variant column must use with specific function"
    }
    qt_document_12_numeric_order """
        SELECT v['price'], v['a'], v['b'] FROM crm_search_products
        WHERE v['name'] MATCH_ANY 'apple' ORDER BY CAST(v['price'] AS BIGINT) ASC LIMIT 100
    """
    def typedQuery = """
        SELECT id, CAST(v['price'] AS BIGINT), CAST(v['a'] AS STRING), CAST(v['b'] AS BIGINT) FROM crm_search_products
        WHERE CAST(v['name'] AS STRING) MATCH_ANY 'apple'
        ORDER BY CAST(v['price'] AS BIGINT), id LIMIT 100
    """
    explain {
        sql typedQuery
        contains "MaterializeNode"
    }
    qt_document_12_typed typedQuery
    qt_document_12_typed_eager """
        SELECT /*+ SET_VAR(topn_lazy_materialization_threshold=-1) */
            id, CAST(v['price'] AS BIGINT), CAST(v['a'] AS STRING), CAST(v['b'] AS BIGINT)
        FROM crm_search_products WHERE CAST(v['name'] AS STRING) MATCH_ANY 'apple'
        ORDER BY CAST(v['price'] AS BIGINT), id LIMIT 100
    """
}
