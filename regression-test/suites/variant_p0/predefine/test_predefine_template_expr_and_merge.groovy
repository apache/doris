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

// A parse_to_variant / try_parse_to_variant call reaches a Schema Template with a DECIMAL and a
// LARGEINT path through more than a bare INSERT ... SELECT: under an expression that returns one of
// its arguments (IF, CASE WHEN, COALESCE, NVL, NULLABLE), through MERGE INTO, which wraps every
// value it assigns in an IF of its own, and through UPDATE. Each must read a number on a declared path from
// its text, like the bare call: 1.005 and 0.125 round from the text on decimal(10, 2) (1.01 and
// 0.13; a binary64 gives 1.00 and 0.12 or 0.13), 188118222.011121933 keeps its digits, and an
// integer beyond 64 bits loads instead of failing the document.
suite("test_predefine_template_expr_and_merge", "p0") {
    // the p0 pipelines fuzz this session variable, and a doc-mode VARIANT refuses a DECIMAL template field
    sql """ set default_variant_enable_doc_mode = false """
    def docs = [
        [1, '{"amount":1.005,"price":188118222.011121933,"id":18446744073709551616}'],
        [2, '{"amount":0.125,"price":0.123456789,"id":170141183460469231731687303715884105727}'],
        // the same numbers beside an integer every parser reads
        [3, '{"amount":1.005,"price":188118222.011121933,"id":7}'],
        // 1.50 keeps the scale of its text where it is read from the text, and is 1.5 as a binary64
        [4, '{"amount":1.50,"price":2.50,"id":8}'],
    ]
    def template = """variant<
                match_name 'amount' : decimal(10, 2),
                match_name 'price' : decimal(20, 9),
                match_name 'id' : largeint"""
    def read = { table_name ->
        """select id, cast(v['amount'] as decimal(10, 2)), cast(v['price'] as decimal(20, 9)),
                cast(v['id'] as largeint) from ${table_name} order by id"""
    }

    sql "DROP TABLE IF EXISTS test_predefine_template_src"
    sql """
        CREATE TABLE `test_predefine_template_src` (
            `id` bigint NOT NULL,
            `j` string NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
    """
    sql "insert into test_predefine_template_src values " +
            docs.collect { doc -> "(${doc[0]}, '${doc[1]}')" }.join(", ")

    // a call under an expression that returns one of its arguments
    sql "DROP TABLE IF EXISTS test_predefine_template_expr"
    sql """
        CREATE TABLE `test_predefine_template_expr` (
            `id` bigint NOT NULL,
            `v` ${template}
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_expr
            select id, if(id > 0, parse_to_variant(j), null) from test_predefine_template_src"""
    sql """insert into test_predefine_template_expr
            select id + 100, case when id = 1 then parse_to_variant(j) when id = 2 then try_parse_to_variant(j)
                    else parse_to_variant(j) end from test_predefine_template_src"""
    sql """insert into test_predefine_template_expr
            select id + 200, coalesce(try_parse_to_variant(j), parse_to_variant('{}'))
            from test_predefine_template_src"""
    sql """insert into test_predefine_template_expr
            select id + 300, nvl(try_parse_to_variant(j), parse_to_variant('{}'))
            from test_predefine_template_src"""
    // a column of a subquery in such an argument: the call behind it is typed where it is computed
    sql """insert into test_predefine_template_expr
            select id + 400, if(id > 0, v, parse_to_variant('{}'))
            from (select id, parse_to_variant(j) as v from test_predefine_template_src) s"""
    sql """insert into test_predefine_template_expr
            select id + 500, case id when 1 then parse_to_variant(j) else try_parse_to_variant(j) end
            from test_predefine_template_src"""
    sql """insert into test_predefine_template_expr
            select id + 600, nullable(parse_to_variant(j)) from test_predefine_template_src"""
    quickTest("expr", read("test_predefine_template_expr"), false)

    // MERGE INTO, both branches, then UPDATE
    sql "DROP TABLE IF EXISTS test_predefine_template_merge"
    sql """
        CREATE TABLE `test_predefine_template_merge` (
            `id` bigint NOT NULL,
            `v` ${template}
            > NULL
        ) ENGINE=OLAP UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_merge values
            (1, parse_to_variant('{"amount":9.99,"price":1,"id":1}')),
            (5, parse_to_variant('{"amount":4.44,"price":4,"id":4}'))"""
    sql """
        MERGE INTO test_predefine_template_merge t USING test_predefine_template_src s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET v = parse_to_variant(s.j)
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, parse_to_variant(s.j))
    """
    quickTest("merge", read("test_predefine_template_merge"), false)
    sql """update test_predefine_template_merge
            set v = parse_to_variant('{"amount":2.675,"price":0.25,"id":18446744073709551617}')
            where id = 5"""
    quickTest("update", read("test_predefine_template_merge"), false)

    // The call is typed where it is computed, so a filter between the call and the table reads the
    // value the template reads, the value the column stores: '1.50' for row 4. The same SELECT with
    // no table to load reads a binary64, '1.5'.
    sql "DROP TABLE IF EXISTS test_predefine_template_filter"
    sql """
        CREATE TABLE `test_predefine_template_filter` (
            `id` bigint NOT NULL,
            `v` ${template}
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    def parsed = "(select id, try_parse_to_variant(j) as v from test_predefine_template_src) s"
    quickTest("filter_select", """select id from ${parsed}
            where cast(v['amount'] as string) = '1.5' order by id""", false)
    sql """insert into test_predefine_template_filter select id, v from ${parsed}
            where cast(v['amount'] as string) = '1.5'"""
    quickTest("filter_insert_binary64_text", read("test_predefine_template_filter"), false)
    sql """insert into test_predefine_template_filter select id, v from ${parsed}
            where cast(v['amount'] as string) = '1.50'"""
    quickTest("filter_insert_template_text", read("test_predefine_template_filter"), false)

    // typed paths kept in the sparse column
    sql "DROP TABLE IF EXISTS test_predefine_template_sparse"
    sql """
        CREATE TABLE `test_predefine_template_sparse` (
            `id` bigint NOT NULL,
            `v` ${template},
                properties("variant_max_subcolumns_count" = "1", "variant_enable_typed_paths_to_sparse" = "true")
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_sparse
            select id, parse_to_variant(j) from test_predefine_template_src"""
    sql """insert into test_predefine_template_sparse
            select id + 1000, try_parse_to_variant(j) from test_predefine_template_src"""
    quickTest("sparse", read("test_predefine_template_sparse"), false)
}
