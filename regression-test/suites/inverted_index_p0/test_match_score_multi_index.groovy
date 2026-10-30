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

// score() over MATCH clauses that read different inverted indexes of one column. Each index has
// its own terms and lengths: the english index below folds case, so apple has df 3 there and df 1
// in the standard index. A clause scores with the statistics of its own index, so the combined
// score is the sum of what each clause scores alone; such a query used to fail with "Scoring
// predicates for one field select different inverted indexes".
suite("test_match_score_multi_index", "p0") {
    // MATCH needs index execution even when CI fuzzes these session defaults.
    sql "set enable_common_expr_pushdown = true"
    sql "set enable_inverted_index_query = true"

    for (def storageFormat : ["V2", "V3", "SNII"]) {
        sql "DROP TABLE IF EXISTS test_match_score_multi_index"
        sql """
            CREATE TABLE test_match_score_multi_index (
                id INT NOT NULL,
                name STRING NULL,
                INDEX idx_english(name) USING INVERTED
                    PROPERTIES("parser" = "english", "support_phrase" = "true", "lower_case" = "true"),
                INDEX idx_standard(name) USING INVERTED
                    PROPERTIES("parser" = "standard", "support_phrase" = "true", "lower_case" = "false")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default:1",
                "inverted_index_storage_format" = "${storageFormat}"
            )
        """
        sql """INSERT INTO test_match_score_multi_index VALUES
            (1, 'apple pie'), (2, 'Apple'), (3, 'APPLE tart today'), (4, 'pear')
        """
        sql "sync"

        for (def query : [
                ["english", "name MATCH 'apple' USING ANALYZER english"],
                ["standard", "name MATCH 'apple' USING ANALYZER standard"],
                // Row 1 matches both clauses and scores their sum.
                ["both_and", """name MATCH 'apple' USING ANALYZER english
                                AND name MATCH 'apple' USING ANALYZER standard"""],
                // Rows 2 and 3 match only the english clause and score as it does alone.
                ["both_or", """name MATCH 'apple' USING ANALYZER english
                               OR name MATCH 'apple' USING ANALYZER standard"""],
                // Two clauses on one index still share its statistics.
                ["same_index", """name MATCH 'apple' USING ANALYZER english
                                  OR name MATCH 'pear' USING ANALYZER english"""],
                ["phrase_and_term", """name MATCH_PHRASE 'apple pie' USING ANALYZER english
                                       AND name MATCH 'pie' USING ANALYZER standard"""]]) {
            quickTest("${storageFormat}_${query[0]}", """
                SELECT id, ROUND(s, 6) FROM (
                    SELECT id, score() AS s FROM test_match_score_multi_index
                    WHERE ${query[1]} ORDER BY s DESC LIMIT 10
                ) hits ORDER BY id
            """, true)
        }
    }
}
