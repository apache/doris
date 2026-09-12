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

// Checklist: A03 A06 C01 E02 G01 G05 H01 H02 H03.
suite("test_uuid_conversion_functions", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    List<String> texts = [null, '', 'bad', '00000000000000000000000000000000',
        '00000000-0000-0000-0000-000000000001', '00112233445566778899AABBCCDDEEFF',
        '550e8400-e29b-41d4-a716-446655440000', '018f0f59-1010-7abc-9234-001122334455',
        '7fffffffffffffffffffffffffffffff', '80000000000000000000000000000000',
        'ffffffff-ffff-ffff-ffff-ffffffffffff', ' 00112233445566778899aabbccddeeff',
        '00112233445566778899aabbccddeeff ', '{00112233-4455-6677-8899-aabbccddeeff}',
        '00112233445566778899aabbccddeeff0', '00112233-44556677-8899-aabbccddeeff',
        '00112233445566778899aabbccddeefg', '中文']
    List<Map> rows = texts.withIndex().collect { text, i ->
        [s: text == null ? 'CAST(NULL AS STRING)' : "CONCAT('${text}','')",
         d: matrix.rows()[i % matrix.rows().size()].u]
    }
    sql "DROP TABLE IF EXISTS uuid_conversion_functions"
    sql """CREATE TABLE uuid_conversion_functions (id INT,s STRING,d UUID)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_conversion_functions VALUES ${rows.withIndex().collect { r,i -> "(${i},${r.s},${r.d})" }.join(',')}"
    for (boolean strict : [false,true]) {
        sql "SET enable_strict_cast=${strict}"
        matrix.run(delegate, "parse_${strict}", 'uuid_conversion_functions', ['s'], { s ->
            [zero_value: "toUUIDOrZero(${s})", null_value: "toUUIDOrNull(${s})",
             default_zero: "toUUIDOrDefault(${s})"]
        }, [rows:rows])
        matrix.run(delegate, "default_${strict}", 'uuid_conversion_functions', ['s','d'], { s,d ->
            [default_value: "toUUIDOrDefault(${s},${d})"]
        }, [rows:rows])
    }
    sql "SET enable_strict_cast=false"
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        String query = """SELECT id,toUUIDOrZero(CONCAT('00112233445566778899aabbccddeeff','')),
                            toUUIDOrDefault(CONCAT('bad',''),d) FROM uuid_conversion_functions ORDER BY id"""
        explain {
            sql "verbose ${query}"
            if (mode == 'runtime') {
                contains 'to_uuid_or_zero('
            } else {
                notContains 'to_uuid_or_zero('
            }
        }
        qt_folding query
        for (boolean strict : [false,true]) {
            sql "SET enable_strict_cast=${strict}"
            for (String u : ['d'] + matrix.rows().collect { it.u }) {
                for (String name : ['to_uint128','toUInt128']) {
                    test {
                        sql "SELECT ${name}(${u}) FROM uuid_conversion_functions ORDER BY id"
                        exception 'Can not found function'
                    }
                }
                for (String expr : ["CAST(${u} AS LARGEINT)", "TRY_CAST(${u} AS LARGEINT)",
                        "ARRAY_MAP(x -> CAST(x AS LARGEINT),ARRAY(${u}))"]) {
                    test {
                        sql "SELECT ${expr} FROM uuid_conversion_functions ORDER BY id"
                        exception 'cannot cast UUID to LARGEINT'
                    }
                }
                test {
                    sql "SELECT CAST(ARRAY(${u}) AS ARRAY<LARGEINT>) FROM uuid_conversion_functions ORDER BY id"
                    exception 'can not cast from origin type ARRAY<UUID> to target type=ARRAY<LARGEINT>'
                }
            }
        }
        sql "SET enable_strict_cast=false"
    }
    qt_aliases """SELECT to_uuid_or_zero('bad'),to_uuid_or_null('bad'),
        to_uuid_or_default(NULL,CAST('00000000000000000000000000000001' AS UUID)),
        toUUIDOrNull(CONCAT('00112233445566778899aabbccddeeff',CHAR(0)))"""
}
