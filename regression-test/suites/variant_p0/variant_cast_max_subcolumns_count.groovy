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

suite("variant_cast_max_subcolumns_count", "p0") {
    def t1 = "variant_cast_src"
    def t2 = "variant_cast_dst"
    def t3 = "variant_cast_typed_mismatch"
    def t4 = "variant_cast_sparse_shard_mismatch"
    def t5 = "variant_cast_increase_no_sparse_src"
    def t6 = "variant_cast_increase_no_sparse_dst"
    def t7 = "variant_cast_decrease_src"
    def t8 = "variant_cast_decrease_dst"
    def t9 = "variant_cast_unlimited_src"
    def t10 = "variant_cast_unlimited_dst"
    def t11 = "variant_cast_sparse_stats_mismatch"
    def t12 = "variant_cast_large_src"
    def t13 = "variant_cast_large_dst"
    def t14 = "variant_cast_wide_src"
    def t15 = "variant_cast_wide_dst"
    def t16 = "variant_cast_complex_src"
    def t17 = "variant_cast_complex_dst"

    sql "DROP TABLE IF EXISTS ${t1}"
    sql "DROP TABLE IF EXISTS ${t2}"
    sql "DROP TABLE IF EXISTS ${t3}"
    sql "DROP TABLE IF EXISTS ${t4}"
    sql "DROP TABLE IF EXISTS ${t5}"
    sql "DROP TABLE IF EXISTS ${t6}"
    sql "DROP TABLE IF EXISTS ${t7}"
    sql "DROP TABLE IF EXISTS ${t8}"
    sql "DROP TABLE IF EXISTS ${t9}"
    sql "DROP TABLE IF EXISTS ${t10}"
    sql "DROP TABLE IF EXISTS ${t11}"
    sql "DROP TABLE IF EXISTS ${t12}"
    sql "DROP TABLE IF EXISTS ${t13}"
    sql "DROP TABLE IF EXISTS ${t14}"
    sql "DROP TABLE IF EXISTS ${t15}"
    sql "DROP TABLE IF EXISTS ${t16}"
    sql "DROP TABLE IF EXISTS ${t17}"

    sql """
        CREATE TABLE ${t1} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "1",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t2} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """INSERT INTO ${t1} VALUES (1, '{"a": 1, "b": 2}'), (2, '{"b": 3}')"""
    sql """
        INSERT INTO ${t2}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t1}
    """
    qt_sql_ok """SELECT v['a'], v['b'], * FROM ${t2} ORDER BY k"""

    sql """
        CREATE TABLE ${t3} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "true")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    test {
        sql """
            INSERT INTO ${t3}
            SELECT k,
                   CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                                "variant_enable_typed_paths_to_sparse" = "true")>)
            FROM ${t1}
        """
        exception "cannot cast"
    }

    sql """
        CREATE TABLE ${t4} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false",
                                 "variant_sparse_hash_shard_count" = "3")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    test {
        sql """
            INSERT INTO ${t4}
            SELECT k,
                   CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                                "variant_enable_typed_paths_to_sparse" = "false",
                                                "variant_sparse_hash_shard_count" = "3")>)
            FROM ${t1}
        """
        exception "cannot cast"
    }

    sql """
        CREATE TABLE ${t5} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "5",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t6} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "10",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """INSERT INTO ${t5} VALUES (1, '{"a": 10, "b": 20}'), (2, '{"b": 30}')"""
    sql """
        INSERT INTO ${t6}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "10",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t5}
    """
    qt_increase_no_sparse """SELECT v['a'], v['b'], * FROM ${t6} ORDER BY k"""

    sql """
        CREATE TABLE ${t7} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "5",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t8} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "1",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """INSERT INTO ${t7} VALUES (1, '{"a": 1, "b": 2, "c": 3}'), (2, '{"b": 3, "c": 4}')"""
    sql """
        INSERT INTO ${t8}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "1",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t7}
    """
    qt_decrease_max """SELECT v['a'], v['b'], v['c'], k FROM ${t8} ORDER BY k"""

    sql """
        CREATE TABLE ${t9} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "1",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t10} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "0",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """INSERT INTO ${t9} VALUES (1, '{"a": 1, "b": 2, "c": 3}'), (2, '{"b": 3, "c": 4}')"""
    sql """
        INSERT INTO ${t10}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "0",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t9}
    """
    qt_unlimited """SELECT v['a'], v['b'], v['c'], k FROM ${t10} ORDER BY k"""

    sql """
        CREATE TABLE ${t11} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false",
                                 "variant_max_sparse_column_statistics_size" = "9999")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """
    test {
        sql """
            INSERT INTO ${t11}
            SELECT k,
                   CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                                "variant_enable_typed_paths_to_sparse" = "false",
                                                "variant_max_sparse_column_statistics_size" = "9999")>)
            FROM ${t1}
        """
        exception "cannot cast"
    }

    sql """
        CREATE TABLE ${t12} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "1",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t13} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "3",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    def largeValues = (1..10000).collect { i ->
        def a = i % 2 == 0 ? i : "null"
        def b = i
        def c = i % 5 == 0 ? i : "null"
        return "(${i}, '{\"a\": ${a}, \"b\": ${b}, \"c\": ${c}}')"
    }.join(",")
    sql "INSERT INTO ${t12} VALUES ${largeValues}"
    sql """
        INSERT INTO ${t13}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "3",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t12}
    """
    qt_large_rows """SELECT COUNT(*) FROM ${t13}"""
    qt_large_sample """SELECT v['a'], v['b'], v['c'], k FROM ${t13} WHERE k IN (1,2,5,10,10000) ORDER BY k"""

    sql """
        CREATE TABLE ${t14} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "0",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t15} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "5",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    def wideValues = (1..10).collect { row ->
        def fields = (1..100).collect { col ->
            def key = String.format("c%03d", col)
            def value = row * 1000 + col
            return "\"${key}\": ${value}"
        }.join(", ")
        return "(${row}, '{${fields}}')"
    }.join(",")
    sql "INSERT INTO ${t14} VALUES ${wideValues}"
    sql """
        INSERT INTO ${t15}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "5",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t14}
    """
    qt_wide_rows """SELECT COUNT(*) FROM ${t15}"""
    qt_wide_sample """SELECT v['c001'], v['c050'], v['c100'], k FROM ${t15} WHERE k IN (1,10) ORDER BY k"""

    sql """
        CREATE TABLE ${t16} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "0",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    sql """
        CREATE TABLE ${t17} (
            k int,
            v variant<properties("variant_max_subcolumns_count" = "8",
                                 "variant_enable_typed_paths_to_sparse" = "false")>
        )
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES("replication_num" = "1");
    """

    def complexValues = (1..10).collect { row ->
        def flatFields = (1..80).collect { col ->
            def key = String.format("c%03d", col)
            def value = row * 1000 + col
            return "\"${key}\": ${value}"
        }
        def objFields = (1..20).collect { col ->
            def key = String.format("o%03d", col)
            def value = row * 100 + col
            return "\"${key}\": ${value}"
        }
        def nestedObj = "\"obj\": {${objFields.join(', ')}}"
        def arr = "\"arr\": [{\"x\": ${row}, \"y\": ${row + 1}}, {\"x\": ${row + 2}, \"y\": ${row + 3}}]"
        def nums = "\"nums\": [${row}, ${row + 1}, ${row + 2}]"
        def types = [
            "\"str\": \"row_${row}\"",
            "\"flag\": ${row % 2 == 0}",
            "\"f\": ${row + 0.5}",
            "\"n\": null",
            "\"obj2\": {\"inner\": {\"k\": ${row}}}",
            "\"mix\": {\"a\": ${row}, \"b\": [${row}, ${row + 1}], \"c\": {\"d\": \"x${row}\"}}"
        ]
        def fields = flatFields + [nestedObj, arr, nums] + types
        return "(${row}, '{${fields.join(', ')}}')"
    }.join(",")
    sql "INSERT INTO ${t16} VALUES ${complexValues}"
    sql """
        INSERT INTO ${t17}
        SELECT k,
               CAST(v AS variant<properties("variant_max_subcolumns_count" = "8",
                                            "variant_enable_typed_paths_to_sparse" = "false")>)
        FROM ${t16}
    """
    qt_complex_rows """SELECT COUNT(*) FROM ${t17}"""
    qt_complex_sample """SELECT v['c001'], v['c080'], v['obj']['o001'], v['obj']['o020'],
        v['obj2']['inner']['k'], v['str'], size(cast(v['nums'] as array<int>)), k
        FROM ${t17} WHERE k IN (1,10) ORDER BY k"""
    qt_complex_samples """SELECT k, v['arr'], v['mix'], v['flag'], v['f'], v['obj']['o005']
        FROM ${t17} WHERE k IN (1,5,10) ORDER BY k"""
    qt_complex_full """SELECT k, CAST(v AS STRING) FROM ${t17} WHERE k = 1"""
}
