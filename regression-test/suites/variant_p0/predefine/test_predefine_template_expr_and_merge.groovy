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
// its arguments (IF, CASE WHEN, COALESCE, NVL, NULLABLE, NON_NULLABLE), through MERGE INTO, which
// wraps every value it assigns in an IF of its own, and through UPDATE. Each must read a number on a
// declared path from its text, like the bare call: 1.005 and 0.125 round from the text on
// decimal(10, 2) (1.01 and 0.13; a binary64 gives 1.00 and 0.12 or 0.13), 188118222.011121933 keeps
// its digits, and an integer beyond 64 bits loads instead of failing the document.
//
// The template applies to the value its column stores and to nothing else. When something else in
// the statement reads the call's value (a filter, a join condition, another column), the call is
// left as it is: that reader sees what it sees in a SELECT, and the column stores what it stored
// before.
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
    sql """insert into test_predefine_template_expr
            select id + 700, non_nullable(parse_to_variant(j)) from test_predefine_template_src"""
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

    // A SELECT reads a number through a binary64: 1.50 is '1.5'.
    def parsed = "(select id, try_parse_to_variant(j) as v from test_predefine_template_src)"
    quickTest("filter_select", """select id from ${parsed} s
            where cast(v['amount'] as string) = '1.5' order by id""", false)

    // The call is read with the template only when the templated column is the one reader of its
    // value. Documents 3 and 4 through four statements:
    sql "DROP TABLE IF EXISTS test_predefine_template_readers"
    sql """
        CREATE TABLE `test_predefine_template_readers` (
            `id` bigint NOT NULL,
            `v` ${template}
            > NULL,
            `raw` variant NULL,
            `txt` string NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    // the column is the one reader (the filter reads id): 1.005 is read from its text, 1.01
    sql """insert into test_predefine_template_readers (id, v)
            select id, v from ${parsed} s where id >= 3"""
    // A filter reads the value too, and sees what the SELECT above sees: '1.5' for document 4. The
    // column stores what it stored before, 1.00 for 1.005. To this read documents 1 and 2 are NULL,
    // their integers beyond 64 bits.
    sql """insert into test_predefine_template_readers (id, v)
            select id + 10, v from ${parsed} s where cast(v['amount'] as string) in ('1.005', '1.5')"""
    // a join condition reads it, against a second parse of the same text: the two still agree
    sql """insert into test_predefine_template_readers (id, v)
            select a.id + 20, a.v from ${parsed} a join ${parsed} b
            on cast(a.v['amount'] as string) = cast(b.v['amount'] as string) and a.id = b.id"""
    // two more columns of the table read it: they store what they stored before, and so does v
    sql """insert into test_predefine_template_readers
            select id + 30, v, v, cast(v['amount'] as string)
            from (select id, try_parse_to_variant(j) as v from test_predefine_template_src
                    where id >= 3 limit 10) s"""
    // Two more columns that parse the text themselves are no readers of v's call: v reads 1.005
    // from its text, and they store what they stored before.
    sql """insert into test_predefine_template_readers
            select id + 60, try_parse_to_variant(j), try_parse_to_variant(j),
                    cast(try_parse_to_variant(j)['amount'] as string)
            from test_predefine_template_src where id >= 3"""
    // the value is grouped before it is stored
    sql """insert into test_predefine_template_readers (id, v)
            select distinct id + 40, v from ${parsed} s where id >= 3"""
    quickTest("readers", """select id, cast(v['amount'] as decimal(10, 2)), cast(raw['amount'] as string), txt
            from test_predefine_template_readers order by id""", false)

    // MERGE INTO: a WHEN condition over the source's parsed value is a reader of it. It sees '1.5'
    // for document 4 and updates the row, and both rows store what they stored before.
    sql "DROP TABLE IF EXISTS test_predefine_template_merge_readers"
    sql """
        CREATE TABLE `test_predefine_template_merge_readers` (
            `id` bigint NOT NULL,
            `v` ${template}
            > NULL
        ) ENGINE=OLAP UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1",
                "enable_unique_key_merge_on_write" = "true", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_merge_readers values
            (4, parse_to_variant('{"amount":9.99,"price":1,"id":1}'))"""
    sql """
        MERGE INTO test_predefine_template_merge_readers t
        USING (select id, try_parse_to_variant(j) as p from test_predefine_template_src where id >= 3) s
        ON t.id = s.id
        WHEN MATCHED AND cast(s.p['amount'] as string) = '1.5' THEN UPDATE SET v = s.p
        WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.p)
    """
    quickTest("merge_readers", """select id, cast(v['amount'] as decimal(10, 2))
            from test_predefine_template_merge_readers order by id""", false)

    // A number below every decimal scale is 0 whatever the length of its exponent, 10^-39 is left
    // to the template's cast, which rounds it to 0, and a number too large for any decimal is NULL.
    // The text is rounded to the declared scale half away from zero, as a JSON string is, and the
    // two reads part at the edge of the precision. 9999999.995 on decimal(9, 2) is 10000000.00,
    // which the type does not hold, so it is NULL: the binary64 nearest it is a little below the
    // number and rounds into range. The other way round, 99999999.99499999999 on decimal(10, 2)
    // is 99999999.99: its binary64 is a little above 99999999.995 and rounds out of range.
    sql "DROP TABLE IF EXISTS test_predefine_template_small"
    sql """
        CREATE TABLE `test_predefine_template_small` (
            `id` bigint NOT NULL,
            `v` variant<
                match_name 'amount' : decimal(10, 2),
                match_name 'd9' : decimal(9, 2)
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_small values
            (1, parse_to_variant('{"amount":1e-3000000000}')),
            (2, parse_to_variant('{"amount":-1e-3000000000}')),
            (3, parse_to_variant('{"amount":5E-2147483648}')),
            (4, parse_to_variant('{"amount":1e-40}')),
            (5, parse_to_variant('{"amount":1234567890123456789012345678901234567890123e-3000000000}')),
            (6, parse_to_variant('{"amount":1e3000000000}')),
            (7, parse_to_variant('{"amount":1e-39}')),
            (8, parse_to_variant('{"d9":9999999.995}')),
            (9, parse_to_variant('{"amount":99999999.99499999999}')),
            (10, parse_to_variant('{"d9":9999999.994}'))"""
    quickTest("small", """select id, cast(v['amount'] as decimal(10, 2)), cast(v['d9'] as decimal(9, 2))
            from test_predefine_template_small order by id""", false)

    // A path is spelled and compared the way the shredder does it. A key under an empty key is the
    // path ".amount", which the field 'amount' does not name, and a key holding a NUL is compared
    // up to it: with 'k' declared a STRING before 'k*' a DECIMAL, "k\\u0000x" is the STRING
    // field's. A number on such a path is parsed as before: 1.50 is the double 1.5, and an integer
    // beyond 64 bits fails the document.
    sql "DROP TABLE IF EXISTS test_predefine_template_paths"
    sql """
        CREATE TABLE `test_predefine_template_paths` (
            `id` bigint NOT NULL,
            `v` variant<
                match_name 'amount' : decimal(10, 2),
                match_name 'id' : largeint,
                match_name 'k' : string,
                match_name_glob 'k*' : decimal(10, 2)
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_paths values
            (1, parse_to_variant('{"amount":1.50}')),
            (2, parse_to_variant('{"":{"amount":1.50}}')),
            (3, try_parse_to_variant('{"":{"id":18446744073709551616}}')),
            (4, try_parse_to_variant('{"id":18446744073709551616}')),
            (5, parse_to_variant('{"k\\\\u0000x":1.50}')),
            (6, parse_to_variant('{"kx":1.50}'))"""
    quickTest("paths", """select id, v is null, cast(v as string) like '%1.50%'
            from test_predefine_template_paths order by id""", false)

    // Two templates that differ only in MATCH_NAME against a glob are one type, so the table a
    // CREATE TABLE AS SELECT builds over a UNION ALL of the two keeps the template, and a load into
    // it reads 1.005 from its text.
    sql "DROP TABLE IF EXISTS test_predefine_template_ctas_glob"
    sql "DROP TABLE IF EXISTS test_predefine_template_ctas_name"
    sql "DROP TABLE IF EXISTS test_predefine_template_ctas"
    sql """
        CREATE TABLE `test_predefine_template_ctas_glob` (
            `id` bigint NOT NULL,
            `v` variant<'amount' : decimal(10, 2)> NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """
        CREATE TABLE `test_predefine_template_ctas_name` (
            `id` bigint NOT NULL,
            `v` variant<match_name 'amount' : decimal(10, 2)> NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_ctas_glob values (1, parse_to_variant('{"amount":1.005}'))"""
    sql """insert into test_predefine_template_ctas_name values (2, parse_to_variant('{"amount":0.125}'))"""
    sql """
        CREATE TABLE test_predefine_template_ctas PROPERTIES ("replication_num" = "1") AS
        select id, v from test_predefine_template_ctas_glob
        union all select id, v from test_predefine_template_ctas_name
    """
    def ctas = sql "show create table test_predefine_template_ctas"
    assertTrue(ctas[0][1].toLowerCase().contains("decimal"), ctas[0][1])
    sql """insert into test_predefine_template_ctas values (3, parse_to_variant('{"amount":1.005}'))"""
    quickTest("ctas", """select id, cast(v['amount'] as decimal(10, 2))
            from test_predefine_template_ctas order by id""", false)

    // A load parses the column once, with the template, and its derived columns and its WHERE name
    // the column itself. They read the value the template reads, '1.50' for 1.50, where the
    // statements above leave a second reader on the previous read.
    sql "DROP TABLE IF EXISTS test_predefine_template_load"
    sql """
        CREATE TABLE `test_predefine_template_load` (
            `id` bigint NOT NULL,
            `v` variant<match_name 'amount' : decimal(10, 2)> NULL,
            `txt` string NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    def load_docs = '{"id":1,"v":{"amount":1.005}}\n{"id":2,"v":{"amount":1.50}}\n'
    def stream_load = { String columns, String where ->
        streamLoad {
            table "test_predefine_template_load"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'jsonpaths', '["$.id", "$.v"]'
            set 'columns', columns
            if (where != null) {
                set 'where', where
            }
            inputText load_docs
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
            }
        }
    }
    // a derived column over the parsed column
    stream_load("tid, v, id = tid, txt = cast(v['amount'] as string)", null)
    // a WHERE over it: '1.5' selects neither document, '1.50' selects the second
    stream_load("tid, v, id = tid + 10", "cast(v['amount'] as string) = '1.5'")
    stream_load("tid, v, id = tid + 20", "cast(v['amount'] as string) = '1.50'")
    quickTest("load", """select id, cast(v['amount'] as decimal(10, 2)), txt
            from test_predefine_template_load order by id""", false)

    // collect_list keeps its state as JSON text between the phases of an aggregation, and the
    // column's type reads that text back with the template: the result of two phases is the result
    // of one, the declared DECIMAL and LARGEINT values exact in both.
    sql "DROP TABLE IF EXISTS test_predefine_template_collect"
    sql """
        CREATE TABLE `test_predefine_template_collect` (
            `id` bigint NOT NULL,
            `v` variant<
                match_name 'a' : decimal(38, 18),
                match_name 'id' : largeint
            > NULL
        ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true");
    """
    sql """insert into test_predefine_template_collect values
            (1, parse_to_variant('{"a":0.123456789012345678,"id":18446744073709551616}')),
            (3, parse_to_variant('{"a":0.123456789012345678,"id":18446744073709551616}'))"""
    def collected = "select cast(collect_list(v) as string) from test_predefine_template_collect"
    sql "set agg_phase = 1"
    quickTest("collect_one_phase", collected, false)
    sql "set agg_phase = 2"
    quickTest("collect_two_phases", collected, false)
    sql "set agg_phase = 0"

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
