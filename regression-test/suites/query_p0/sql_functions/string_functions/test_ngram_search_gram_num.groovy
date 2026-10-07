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

suite("test_ngram_search_gram_num") {
    sql "drop table if exists test_ngram_search_gram_num"
    sql """
        create table test_ngram_search_gram_num (
            k1 int not null,
            s string null
        ) distributed by hash (k1) buckets 1
        properties ("replication_num" = "1")
    """
    sql """insert into test_ngram_search_gram_num values (1, 'abc'), (2, 'ab'), (3, 'abc1313131'), (4, null)"""

    for (def foldOnBe in [false, true]) {
        sql "set enable_fold_constant_by_be = ${foldOnBe}"

        // constant expressions folded on FE are equivalent to the literal
        qt_literal "select ngram_search('abc', 'abc', 3)"
        qt_arithmetic "select ngram_search('abc', 'abc', 1 + 2)"
        qt_cast "select ngram_search('abc', 'abc', cast('3' as int))"
        qt_function "select ngram_search('abc', 'abc', abs(-3))"
        qt_nested "select ngram_search('abc', 'abc', cast(abs(-2) + 1 as int))"
        qt_null_text "select ngram_search(cast(null as string), 'abc', 1 + 2)"
        order_qt_rows """
            select k1, ngram_search(s, 'abc', 1 + 2), ngram_search(s, 'abc', cast('1' as int))
            from test_ngram_search_gram_num
        """

        // constant expressions that only BE can evaluate are executed and validated by BE
        qt_be_function "select ngram_search('abc', 'abc', crc32('abc') % 3 + 1)"
        qt_be_null_gram "select ngram_search('abc', 'abc', crc32('abc') % 0)"
        order_qt_be_rows """
            select k1, ngram_search(s, 'abc', crc32('abc') % 3 + 1), ngram_search(s, 'abc', crc32('abc') % 0)
            from test_ngram_search_gram_num
        """
        for (def gram in ["crc32('abc') % 3 - 3", "crc32('abc') % 3 - 4"]) {
            test {
                sql "select ngram_search('abc', 'abc', ${gram})"
                exception "gram_num must be a positive constant"
            }
            test {
                sql "select k1, ngram_search(s, 'abc', ${gram}) from test_ngram_search_gram_num"
                exception "gram_num must be a positive constant"
            }
        }

        for (def gram in ["0", "-1", "1 - 1", "1 - 2", "cast('0' as int)"]) {
            test {
                sql "select ngram_search('abc', 'abc', ${gram})"
                exception "gram_num must be a positive constant"
            }
            test {
                sql "select ngram_search(cast(null as string), 'abc', ${gram})"
                exception "gram_num must be a positive constant"
            }
            test {
                sql "select k1, ngram_search(s, 'abc', ${gram}) from test_ngram_search_gram_num"
                exception "gram_num must be a positive constant"
            }
        }
        for (def gram in ["'3'", "3.5", "null", "cast(null as int)", "cast(rand() as int)", "k1"]) {
            test {
                sql "select k1, ngram_search(s, 'abc', ${gram}) from test_ngram_search_gram_num"
                exception "gram_num support const value only"
            }
        }
        test {
            sql "select k1, ngram_search('abc', s, 3) from test_ngram_search_gram_num"
            exception "pattern support const value only"
        }
    }
}
