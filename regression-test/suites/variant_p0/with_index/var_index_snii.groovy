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

import org.apache.doris.regression.action.ProfileAction

suite("regression_test_variant_var_index_snii", "p0, nonConcurrent"){
    sql """ set default_variant_enable_typed_paths_to_sparse = false """
    sql """ set default_variant_enable_doc_mode = false """
    // The tables here hold a handful of rows, so a matching row is a large fraction of its segment
    // and the BKD reader would decline the index as unselective (INVERTED_INDEX_BYPASS), leaving
    // the filtered-row assertions below at 0. A zero threshold turns that selectivity bypass off,
    // so the assertions measure index participation rather than how the rows happen to be laid out.
    sql """ set inverted_index_skip_threshold = 0 """

    sql "set enable_profile = true"
    sql "set profile_level = 2"
    sql "set enable_condition_cache = false"
    sql "set enable_sql_cache = false"

    // Declared sub-column types: FE validates each path type through
    // CreateTableInfo, and the BE gets subcolumn_indexes already initialized.
    def typed_table = "var_index_snii_typed"
    sql "DROP TABLE IF EXISTS ${typed_table}"
    sql """
        CREATE TABLE IF NOT EXISTS ${typed_table} (
            k bigint,
            s string,
            v variant<'a' : int, 'b' : string, 'c' : int>,
            INDEX idx_s(s) USING INVERTED PROPERTIES("parser" = "english") COMMENT '',
            INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true",
                   "inverted_index_storage_format" = "SNII");
    """
    // Separate writes exercise the index check across multiple rowsets.
    sql """insert into ${typed_table} values
               (1, 'alpha plain', parse_to_variant('{"a" : 123, "b" : "xxxyyy", "c" : 111999111}')),
               (2, 'beta plain', parse_to_variant('{"a" : 18811, "b" : "hello world", "c" : 1181111}'))"""
    sql """insert into ${typed_table} values
               (3, 'alpha other', parse_to_variant('{"a" : 18811, "b" : "hello wworld", "c" : 11111}')),
               (4, 'gamma plain', parse_to_variant('{"a" : 1234, "b" : "hello xxx world", "c" : 8181111}'))"""

    sql """set enable_match_without_inverted_index = false"""
    qt_typed_match """select k from ${typed_table} where cast(v["b"] as string) match 'hello' order by k"""
    qt_typed_range """select k from ${typed_table} where cast(v["a"] as int) > 1000 order by k"""
    // Check the completed query profile, since the debug point runs once per segment
    // and its cumulative counter may not yet include all rowsets.
    def profileToken = "snii_typed_range_" + UUID.randomUUID().toString()
    sql """select /* ${profileToken} */ k from ${typed_table}
           where cast(v["a"] as int) > 1000 order by k"""
    def profile = new ProfileAction(context).getProfileBySql(profileToken, ["RowsInvertedIndexFiltered"])
    def matches = (profile =~ /RowsInvertedIndexFiltered:\s*([0-9,]+)/)
    long filteredRows = 0
    while (matches.find()) {
        filteredRows += Long.parseLong(matches.group(1).replace(",", ""))
    }
    assertTrue(filteredRows > 0, "Expected inverted index to filter rows, profile: ${profile}")
    qt_typed_both """select k from ${typed_table} where cast(v["a"] as int) > 123 and cast(v["b"] as string) match 'hello' and cast(v["c"] as int) > 1024 order by k"""
    // A plain column and a variant column carry SNII indexes in one table: the plain one is
    // keyed by its own index id, the variant sub-columns by suffix path under theirs.
    qt_typed_plain_col """select k from ${typed_table} where s match 'alpha' order by k"""
    qt_typed_mixed """select k from ${typed_table} where s match 'alpha' and cast(v["b"] as string) match 'hello' order by k"""
    sql """set enable_match_without_inverted_index = true"""

    // Dynamically discovered sub-columns: the BE inherits the parent index per
    // sub-column through variant_util::inherit_index instead.
    def dynamic_table = "var_index_snii_dynamic"
    sql "DROP TABLE IF EXISTS ${dynamic_table}"
    sql """
        CREATE TABLE IF NOT EXISTS ${dynamic_table} (
            k bigint,
            v variant,
            INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true",
                   "inverted_index_storage_format" = "SNII");
    """
    sql """insert into ${dynamic_table} values(1, parse_to_variant('{"a" : 123, "b" : "xxxyyy"}'))"""
    sql """insert into ${dynamic_table} values(2, parse_to_variant('{"a" : 18811, "b" : "hello world"}'))"""
    sql """insert into ${dynamic_table} values(3, parse_to_variant('{"a" : 1234, "b" : "hello xxx world"}'))"""

    sql """set enable_match_without_inverted_index = false"""
    qt_dynamic_match """select k from ${dynamic_table} where cast(v["b"] as string) match 'hello' order by k"""
    qt_dynamic_range """select k from ${dynamic_table} where cast(v["a"] as int) > 1000 order by k"""
    // Deliberately no filtered_rows assertion here, unlike the typed table above. Measured
    // directly: for a dynamically discovered numeric sub-column, both SNII and V3 report
    // filtered_rows: 0 for this same query -- the index contributes nothing and the result is
    // produced by the row-level residual predicate instead. That is a property of dynamically
    // discovered sub-columns in general (parity between SNII and V3), not a SNII gap, so it is
    // not asserted as a contract here: pinning "filtered_rows: 0" would make this suite fail the
    // day dynamic sub-column indexing improves, which is backwards. The qt_dynamic_range result
    // above still proves the query is answered correctly either way.
    sql """set enable_match_without_inverted_index = true"""

    sql "DROP TABLE IF EXISTS ${typed_table}"
    sql "DROP TABLE IF EXISTS ${dynamic_table}"
}
