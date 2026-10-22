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

suite("test_simple_case_lookup") {
    sql "SET short_circuit_evaluation = false"
    sql "DROP TABLE IF EXISTS simple_case_lookup"
    sql """CREATE TABLE simple_case_lookup (
        id INT NOT NULL, k INT NULL, k64 BIGINT NULL, s STRING NULL
    ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO simple_case_lookup
        SELECT number, if(number % 17 = 0, NULL, number - 1),
            if(number % 17 = 0, NULL, number - 1),
            if(number % 17 = 0, NULL, concat('key-', cast(number - 1 AS STRING)))
        FROM numbers('number'='260')"""
    sql """INSERT INTO simple_case_lookup VALUES
        (1023, 1023, 1023, 'key-1023'), (1024, 1024, 1024, 'key-1024')"""
    for (boolean simpleCase : [false, true]) {
        sql "SET enable_experimental_simple_case = ${simpleCase}"
        for (String input : ["k", "k64", "s"]) {
            def threshold = input == "k" ? 64 : input == "k64" ? 32 : 8
            for (int branches : [threshold - 1, threshold, 256, 257, 1024]) {
                for (String resultKind : ["integer", "nullable", "string", "no_else", "duplicate"]) {
                    def clauses = (0..<branches).collect { branch ->
                        def key = resultKind == "duplicate" && branch == branches - 1 ? 1 : branch
                        def literal = input == "s" ? "'key-${key}'" : "${key}"
                        def value = resultKind == "nullable" && branch % 3 == 0 ? "NULL" :
                            resultKind == "string" ? "concat('then-${branch}-', cast(id AS STRING))" :
                            "id + ${branch}"
                        "WHEN ${literal} THEN ${value}"
                    }.join(" ")
                    def otherwise = resultKind == "no_else" ? "" :
                        resultKind == "string" ? " ELSE 'miss'" : " ELSE -id"
                    def query = """SELECT id, CASE ${input} ${clauses}${otherwise} END AS result
                        FROM simple_case_lookup ORDER BY id"""
                    quickTest("simple_${simpleCase}_${input}_${branches}_${resultKind}", query)
                    explain {
                        sql query
                        contains "CASE "
                        if (simpleCase) {
                            notContains "CASE WHEN"
                        } else {
                            contains "CASE WHEN"
                        }
                    }
                }
            }
        }
    }
}
