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

import org.apache.doris.regression.suite.Suite

// Loaded explicitly by each UUID matrix suite; this helper defines no global Suite methods.
class UuidMatrix {
    // A constant is an expression, so disabling folding exercises BE ColumnConst paths.
    // Each nullable domain includes ordinary values and its own boundary/NULL values.
    static List<Map> rows() {
        List<String> texts = [null, '00000000-0000-0000-0000-000000000000',
            '00000000-0000-0000-0000-000000000001', '550E8400E29B41D4A716446655440000',
            '018f0f59-1010-7abc-9234-001122334455', '7fffffff-ffff-ffff-ffff-ffffffffffff',
            '80000000-0000-0000-0000-000000000000', 'ffffffff-ffff-ffff-ffff-ffffffffffff',
            '00000000-0000-0000-ffff-ffffffffffff', '00000000-0000-0001-0000-000000000000']
        List<String> uuids = texts.collect { it == null ? 'CAST(NULL AS UUID)' : "CAST(CONCAT('${it}','') AS UUID)" }
        List<String> arrays = ['CAST(NULL AS ARRAY<UUID>)', 'CAST([] AS ARRAY<UUID>)',
            "ARRAY(${uuids[0]})", "ARRAY(${uuids[1]},${uuids[7]},${uuids[0]},${uuids[3]},${uuids[1]})",
            "ARRAY(${uuids[3]},${uuids[6]},${uuids[5]},${uuids[3]})", "ARRAY(${uuids[7]},${uuids[1]},${uuids[6]})",
            "ARRAY(${uuids[4]},${uuids[3]})", "ARRAY(${uuids[8]},${uuids[9]})", "ARRAY(${uuids[6]})",
            "ARRAY(${uuids[5]},${uuids[0]})"]
        (0..<texts.size()).collect { int i ->
            String u = uuids[i]
            // Preserve the domain while also matching normal values and NULL on both sides.
            String v = uuids[[0,2,1,3,4,6,5,8,7,9][i]]
            String a = arrays[i]
            [u: u, v: v, w: uuids[(i + 3) % uuids.size()],
             flag: ['TRUE','FALSE','CAST(NULL AS BOOLEAN)'][i % 3],
             idx: ['-1','0','1','2','100','NULL'][i % 6],
             num: ['0','1','2','NULL'][i % 4],
             a: a, a2: "REVERSE(${a})",
             flags: "ARRAY_MAP(x -> x > ${uuids[1]},${a})",
             m: i == 0 ? 'CAST(NULL AS MAP<UUID,UUID>)' : (i == 1 ? 'CAST(MAP() AS MAP<UUID,UUID>)'
                 : "MAP(${u},${v},${v},${uuids[0]})"),
             st: i == 0 ? 'CAST(NULL AS STRUCT<k:UUID,a:ARRAY<UUID>>)' : "NAMED_STRUCT('k',${u},'a',${a})"]
        }
    }

    static String schema() {
        'id INT, u UUID, v UUID, w UUID, flag BOOLEAN, idx INT, num INT, a ARRAY<UUID>, ' +
            'a2 ARRAY<UUID>, flags ARRAY<BOOLEAN>, m MAP<UUID,UUID>, st STRUCT<k:UUID,a:ARRAY<UUID>>'
    }

    static String values() {
        rows().withIndex().collect { row, i -> "(${i},${row.values().join(',')})" }.join(',\n')
    }

    // Enumerate every legal argument-position mask, not only column/constant in one direction.
    // aligned=true is for operators requiring equal-length arrays; each row is still tested.
    // Aggregate/window operators retain their own execution semantics under folding modes.
    static void run(Suite suite, String group, String table, List<String> columns,
                    Closure expressions, Map options = [:]) {
        // Repeated SQL must execute the selected folding/aggregation mode instead of reusing results.
        suite.sql "SET enable_sql_cache = false"
        suite.sql "SET enable_query_cache = false"
        List<Map> inputRows = options.rows ?: rows()
        int allColumns = (1 << columns.size()) - 1
        for (String mode : ['fe', 'be', 'runtime']) {
            suite.sql "SET debug_skip_fold_constant = ${mode == 'runtime'}"
            suite.sql "SET enable_fold_constant_by_be = ${mode == 'be'}"
            for (int mask = 0; mask <= allColumns; ++mask) {
                List<Integer> samples = mask == allColumns ? [0] : (0..<inputRows.size()).toList()
                for (int sample : samples) {
                    List<String> args = columns.withIndex().collect { column, i ->
                        (mask & (1 << i)) != 0 ? column : inputRows[sample][column]
                    }
                    String shape = columns.indices.collect { (mask & (1 << it)) != 0 ? 'v' : 'c' }.join('')
                    Map<String, String> values = expressions.call(*args)
                    String projection = values.collect { name, expression -> "${expression} AS `${name}`" }.join(',')
                    String predicate = options.aligned && mask != allColumns ? " WHERE id=${sample}" : ''
                    String grouping = options.groupBy ? " GROUP BY ${options.groupBy} ORDER BY ${options.groupBy}" : ''
                    if (options.groupBy) {
                        projection = "${options.groupBy},${projection}"
                    }
                    String lateral = options.lateral ? ' ' + options.lateral.call(*args) : ''
                    String orderBy = options.orderBy ?: 'id'
                    String query = options.aggregate
                        ? "SELECT ${projection} FROM ${table}${predicate}${grouping}"
                        : "SELECT id,${projection} FROM ${table}${lateral}${predicate} ORDER BY ${orderBy}"
                    String tag = "${group}_${mode}_${shape}_${sample}"
                    if (options.rowConverter != null) {
                        suite.quickRunTest(tag, query, false, options.rowConverter)
                    } else {
                        suite.quickTest(tag, query)
                    }
                }
            }
        }
    }
}

return UuidMatrix
