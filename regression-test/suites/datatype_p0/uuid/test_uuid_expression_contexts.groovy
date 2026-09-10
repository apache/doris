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
// Generic expression contexts missing from the UUID scalar and collection matrices.
suite("test_uuid_expression_contexts", "p0") {
    def matrix = this.evaluate(new File(context.file.parentFile, "uuid_matrix.groovy"))
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "DROP TABLE IF EXISTS uuid_expression_contexts"
    sql """CREATE TABLE uuid_expression_contexts (${matrix.schema()})
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_expression_contexts VALUES ${matrix.values()}"
    for (String mode : ['fe','be','runtime']) {
        sql "SET debug_skip_fold_constant=${mode == 'runtime'}"
        sql "SET enable_fold_constant_by_be=${mode == 'be'}"
        qt_subquery """SELECT id,u,
            u IN (SELECT v FROM uuid_expression_contexts WHERE id%2=0),
            u NOT IN (SELECT v FROM uuid_expression_contexts WHERE id%2=0),
            u IN (SELECT v FROM uuid_expression_contexts WHERE id<0),
            u NOT IN (SELECT v FROM uuid_expression_contexts WHERE id<0),
            (SELECT MAX(v) FROM uuid_expression_contexts WHERE id<0)
            FROM uuid_expression_contexts ORDER BY id"""
        qt_correlated """SELECT l.id,l.u,
            (SELECT MAX(r.v) FROM uuid_expression_contexts r WHERE r.id=l.id),
            EXISTS(SELECT 1 FROM uuid_expression_contexts r WHERE r.v=l.u),
            NOT EXISTS(SELECT 1 FROM uuid_expression_contexts r WHERE r.v<=>l.u)
            FROM uuid_expression_contexts l ORDER BY l.id"""
        qt_grouping """SELECT u,GROUPING(u),COUNT(*),MIN(v),MAX(v)
            FROM uuid_expression_contexts GROUP BY GROUPING SETS ((u),()) ORDER BY GROUPING(u),u"""
        for (String direction : ['ASC','DESC']) {
            for (String nulls : ['', 'NULLS FIRST','NULLS LAST']) {
                qt_sort "SELECT id,u FROM uuid_expression_contexts ORDER BY u ${direction} ${nulls},id"
            }
        }
    }
    matrix.run(delegate, 'subquery_matrix', 'uuid_expression_contexts', ['u','v'], { u,v ->
        [in_select: "${u} IN (SELECT ${v} FROM uuid_expression_contexts r)",
         not_in_select: "${u} NOT IN (SELECT ${v} FROM uuid_expression_contexts r)"]
    })
    matrix.run(delegate, 'unary_context', 'uuid_expression_contexts', ['u'], { u ->
        [identity: "+${u}", xor_value: "(${u}=CAST('00000000000000000000000000000001' AS UUID)) XOR (${u} IS NULL)"]
    })
    List<String> patterns = [null,'','%','550%','^018f','.*','80000000%','f.*',
        '00000000-0000-0000-ffff-ffffffffffff','_']
    List<Map> patternRows = matrix.rows().withIndex().collect { r,i ->
        String pattern = patterns[i]
        String regex = pattern == '' ? '^$' : pattern
        [u:r.u,p:pattern == null ? 'CAST(NULL AS STRING)' : "CONCAT('${pattern}','')",
         r:regex == null ? 'CAST(NULL AS STRING)' : "CONCAT('${regex}','')"]
    }
    sql "DROP TABLE IF EXISTS uuid_expression_patterns"
    sql """CREATE TABLE uuid_expression_patterns (id INT,u UUID,p STRING,r STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_expression_patterns VALUES ${patternRows.withIndex().collect { r,i -> "(${i},${r.u},${r.p},${r.r})" }.join(',')}"
    matrix.run(delegate, 'like_pattern', 'uuid_expression_patterns', ['u','p'], { u,p ->
        [like_value: "${u} LIKE ${p}", not_like: "${u} NOT LIKE ${p}"]
    }, [rows:patternRows])
    matrix.run(delegate, 'regex_pattern', 'uuid_expression_patterns', ['u','r'], { u,r ->
        [regex_value: "${u} REGEXP ${r}", not_regex: "${u} NOT RLIKE ${r}"]
    }, [rows:patternRows])
    // Empty REGEXP has a pre-existing constant/vector discrepancy for STRING itself.
    // Check UUID-to-STRING parity without freezing either inconsistent boolean as correct.
    List<Map> emptyRows = matrix.rows().withIndex().collect { r,i ->
        [u:r.u,p:i%2 == 0 ? 'CAST(NULL AS STRING)' : "CONCAT('','')"]
    }
    sql "DROP TABLE IF EXISTS uuid_expression_empty_regex"
    sql """CREATE TABLE uuid_expression_empty_regex (id INT,u UUID,p STRING)
           DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"""
    sql "INSERT INTO uuid_expression_empty_regex VALUES ${emptyRows.withIndex().collect { r,i -> "(${i},${r.u},${r.p})" }.join(',')}"
    matrix.run(delegate, 'empty_regex_parity', 'uuid_expression_empty_regex', ['u','p'], { u,p ->
        [uuid_regex: "${u} REGEXP ${p}", text_regex: "CAST(${u} AS STRING) REGEXP ${p}",
         uuid_not_regex: "${u} NOT RLIKE ${p}", text_not_regex: "CAST(${u} AS STRING) NOT RLIKE ${p}"]
    }, [rows:emptyRows, rowConverter: { row ->
        // Compare after execution so the optimizer cannot replace x <=> x with TRUE.
        [row[0], row[1] == row[2], row[3] == row[4]]
    }])
    test {
        sql "SELECT u IS TRUE FROM uuid_expression_contexts"
        exception 'cannot cast UUID to BOOLEAN'
    }
    for (String expr : ['u+1','u-1','u*2','u/2','u%2','u&1','u|1','u^1']) {
        test {
            sql "SELECT ${expr} FROM uuid_expression_contexts"
            exception 'numeric'
        }
    }
    test {
        sql "SELECT ~u FROM uuid_expression_contexts"
        exception 'cannot cast'
    }
    for (String expr : ['ABS(u)','SUM(u)','AVG(u)']) {
        test {
            sql "SELECT ${expr} FROM uuid_expression_contexts"
            exception 'signature'
        }
    }
    test {
        sql "SELECT u=1 FROM uuid_expression_contexts"
        exception 'unsupported comparison predicate'
    }
}
