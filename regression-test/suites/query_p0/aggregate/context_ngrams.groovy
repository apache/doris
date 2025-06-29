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

suite("context_ngrams") {
    sql """ DROP TABLE IF EXISTS `context_ngrams_test` """

    sql """
        CREATE TABLE context_ngrams_test (
          `id` int,
          `para` array<array<String>>
        ) ENGINE=OLAP
        DUPLICATE KEY (id)
        DISTRIBUTED BY HASH(`id`) BUCKETS 4
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
        );
        """

    // no value
    // without group by
    qt_sql_empty_1 "select context_ngrams(para, 1, 1) from context_ngrams_test"
    qt_sql_empty_2 "select context_ngrams(para, [null], 1) from context_ngrams_test"
    // with group by
    qt_sql_empty_3 "select context_ngrams(para, 1, 1) from context_ngrams_test group by id"
    qt_sql_empty_4 "select context_ngrams(para, [null], 1) from context_ngrams_test group by id"

    sql """ TRUNCATE TABLE context_ngrams_test """

    // insert
    sql """
        INSERT INTO context_ngrams_test VALUES
        (1, null)
        """

    // null value
    qt_sql_null_1 "select context_ngrams(para, 1, 1) from context_ngrams_test"
    qt_sql_null_2 "select context_ngrams(para, [null], 1) from context_ngrams_test"

    // insert
    sql """
        INSERT INTO context_ngrams_test VALUES
        (2,
         [
           ["Apache", "Doris", "is", "an", "MPP-based", "real-time", "data", "warehouse"],
           ["Apache", "Doris", "returns", "results", "in", "sub-seconds"],
           ["Apache", "Doris", "supports", "high-concurrent", "point", "queries"]
         ]
        ),
        (3,
         [
           ["Apache", "Doris", "can", "be", "used", "for", "report", "analysis"],
           ["Apache", "Doris", "formerly", "known", "as", "Palo"],
           ["Apache", "Doris", "was", "officially", "open-sourced", "in", "2017"]
         ]
        ),
        (3,
         [
           ["Apache", "Doris", "graduated", "from", "the", "Apache", "incubator"],
           ["Apache", "Doris", "has", "a", "wide", "user", "base"]
         ]
        ),
        (4,
         [
           ["Apache", "Doris", "has", "been", "used", "in", "production", "environments"],
           ["Apache", "Doris", "has", "a", "wide", "user", "base"]
         ]
        ),
        (5,
         [
           ["Apache", "Doris", "is", "an", "MPP-based", "real-time", "data", "warehouse"],
           ["Apache", "Doris", "was", "officially", "open-sourced", "in", "2017"]
         ]
        )
        """

    // sql """set parallel_pipeline_task_num=1"""

    // no result
    qt_sql_no_result_1 "select context_ngrams(para, ['aaaaaaaa', 'bbbbbbbb', null], 3, 10) from context_ngrams_test"
    qt_sql_no_result_2 "select context_ngrams(para, ['aaaaaaaa', 'bbbbbbbb', null, null], 5, 10) from context_ngrams_test"

    // 1-gram
    qt_sql_unigram_1 "select context_ngrams(para, 1, 3) from context_ngrams_test"
    qt_sql_unigram_2 "select context_ngrams(para, 1, 5) from context_ngrams_test"
    qt_sql_unigram_3 "select context_ngrams(para, 1, 3, 10) from context_ngrams_test"
    qt_sql_unigram_4 "select context_ngrams(para, 1, 5, 10) from context_ngrams_test"
    qt_sql_unigram_5 "select context_ngrams(para, ['Apache', 'Doris', null], 3, 10) from context_ngrams_test"

    // 2-gram
    qt_sql_bigram_1 "select context_ngrams(para, 2, 3) from context_ngrams_test"
    qt_sql_bigram_2 "select context_ngrams(para, 2, 5) from context_ngrams_test"
    qt_sql_bigram_3 "select context_ngrams(para, 2, 3, 10) from context_ngrams_test"
    qt_sql_bigram_4 "select context_ngrams(para, 2, 5, 10) from context_ngrams_test"
    qt_sql_bigram_5 "select context_ngrams(para, ['Apache', 'Doris', null, null], 5, 10) from context_ngrams_test"

    // test where, group by and order by
    qt_sql_test_1 "select context_ngrams(para, ['Apache', 'Doris', null], 3, 10) from context_ngrams_test where id > 2"
    qt_sql_test_2 "select context_ngrams(para, ['Apache', 'Doris', null], 3, 10) from context_ngrams_test group by id"
    qt_sql_test_3 "select context_ngrams(para, ['Apache', 'Doris', null], 3, 10) from context_ngrams_test group by id order by id"
    qt_sql_test_4 "select context_ngrams(para, ['Apache', 'Doris', null], 3, 10) from context_ngrams_test where id > 2 group by id order by id"

    // exception test for context_nagrams
    // the first parameter must be array of array of string
    test{
        sql """select context_ngrams(1, 1, 1, 1)"""
        exception "context_ngrams requires array<array<string>> for first parameter"
    }

    // the second parameter must be integer or arrary of string contains at least one null
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], 1.5, 1, 1)"""
        exception "context_ngrams requires integer for n parameter"
    }
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], ["I", "Love"], 1, 1)"""
        exception "context_ngrams requires at least one null in context pattern"
    }

    // the third parameter must be integer and greater than zero
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], ["I", "Love", null], 1.5, 1)"""
        exception "context_ngrams requires integer for parameter 3"
    }
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], ["I", "Love", null], 0, 1)"""
        exception "context_ngrams requires the third parameter must greater than zero"
    }

    // the fourth parameter must be integer and greater than zero
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], ["I", "Love", null], 1, 1.5)"""
        exception "context_ngrams requires integer for parameter 4"
    }
    test{
        sql """select context_ngrams([["I", "Love", "Apache"], ["I", "Love", "Doris"]], ["I", "Love", null], 1, 0)"""
        exception "context_ngrams requires the fourth parameter must greater than zero"
    }
}