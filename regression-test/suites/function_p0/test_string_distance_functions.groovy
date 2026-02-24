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

suite("test_string_distance_functions") {

    sql """ set enable_nereids_planner=true; """
    sql """ set enable_fallback_to_original_planner=false; """

    // levenshtein
    qt_levenshtein_equal         """ SELECT levenshtein('abc', 'abc'); """
    qt_levenshtein_empty_to      """ SELECT levenshtein('', 'abc'); """
    qt_levenshtein_to_empty      """ SELECT levenshtein('abc', ''); """
    qt_levenshtein_basic         """ SELECT levenshtein('kitten', 'sitting'); """
    qt_levenshtein_no_transpose  """ SELECT levenshtein('ca', 'abc'); """
    qt_levenshtein_null          """ SELECT levenshtein(null, 'abc'); """

    // damerau_levenshtein — true DL (unrestricted transpositions)
    qt_damerau_equal             """ SELECT damerau_levenshtein('abc', 'abc'); """
    qt_damerau_empty_to          """ SELECT damerau_levenshtein('', 'abc'); """
    qt_damerau_single_swap       """ SELECT damerau_levenshtein('ab', 'ba'); """
    qt_damerau_transposition     """ SELECT damerau_levenshtein('ca', 'abc'); """
    qt_damerau_classic           """ SELECT damerau_levenshtein('kitten', 'sitting'); """
    qt_damerau_null              """ SELECT damerau_levenshtein(null, 'abc'); """

    // jaro_winkler
    qt_jaro_equal                """ SELECT jaro_winkler('abc', 'abc'); """
    qt_jaro_empty                """ SELECT jaro_winkler('', 'abc'); """
    qt_jaro_no_match             """ SELECT jaro_winkler('a', 'b'); """
    qt_jaro_martha               """ SELECT jaro_winkler('MARTHA', 'MARHTA'); """
    qt_jaro_null                 """ SELECT jaro_winkler(null, 'abc'); """

    // jaccard_similarity
    qt_jaccard_equal             """ SELECT jaccard_similarity('abc', 'abc'); """
    qt_jaccard_both_empty        """ SELECT jaccard_similarity('', ''); """
    qt_jaccard_short             """ SELECT jaccard_similarity('a', 'b'); """
    qt_jaccard_disjoint          """ SELECT jaccard_similarity('ab', 'cd'); """
    qt_jaccard_half              """ SELECT jaccard_similarity('abcd', 'abce'); """
    qt_jaccard_null              """ SELECT jaccard_similarity(null, 'abc'); """

    // error cases: input exceeds max allowed length
    test {
        sql """ SELECT levenshtein(repeat('a', 65536), 'b'); """
        exception "Input string too long"
    }
    test {
        sql """ SELECT damerau_levenshtein(repeat('a', 10001), 'b'); """
        exception "Input string too long"
    }
    test {
        sql """ SELECT jaro_winkler(repeat('a', 65536), 'b'); """
        exception "Input string too long"
    }
    test {
        sql """ SELECT jaccard_similarity(repeat('a', 65536), 'b'); """
        exception "Input string too long"
    }
}
