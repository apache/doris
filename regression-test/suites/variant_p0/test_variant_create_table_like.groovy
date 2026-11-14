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

// CREATE TABLE LIKE prints the source as DDL text and parses it again. The text of a nested group VARIANT
// column used to carry variant_max_sparse_column_statistics_size = 0, which does not parse, so the statement
// failed. The properties the text leaves out, such as a disabled doc mode, come from the default_variant_*
// session variables, which take effect only with debug points enabled, as in the tests; the fuzzy runs pick
// them at random, so set them to their defaults.
suite("test_variant_create_table_like", "p0") {
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_enable_nested_group = false """
    sql """ DROP TABLE IF EXISTS variant_create_like_src """
    sql """
        CREATE TABLE variant_create_like_src (
            id INT,
            v VARIANT<MATCH_NAME 'a.b':INT, PROPERTIES(
                "variant_max_subcolumns_count" = "3",
                "variant_enable_typed_paths_to_sparse" = "false",
                "variant_max_sparse_column_statistics_size" = "10000",
                "variant_sparse_hash_shard_count" = "1")>,
            d VARIANT<PROPERTIES(
                "variant_enable_doc_mode" = "true",
                "variant_doc_materialization_min_rows" = "0",
                "variant_doc_hash_shard_count" = "4")>,
            n VARIANT<PROPERTIES("variant_enable_nested_group" = "true")>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    qt_desc_src """ desc variant_create_like_src """

    sql """ DROP TABLE IF EXISTS variant_create_like_dst """
    sql """ CREATE TABLE variant_create_like_dst LIKE variant_create_like_src """
    qt_desc_dst """ desc variant_create_like_dst """
}
