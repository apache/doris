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

suite("test_compoundpredicate_explain") {
    sql "drop table if exists test_compoundpredicate_explain"
    sql """create table test_compoundpredicate_explain
         (k1 int, k2 int)
         distributed by hash(k1) buckets 3 properties('replication_num' = '1');"""

    sql """INSERT INTO test_compoundpredicate_explain (k1, k2) VALUES (500, 450), (1100, 400), (300, 600), (700, 650), (800, 800), (1500, 300);"""

    explain {
        sql "select * from test_compoundpredicate_explain where k1 > 500 and k2 < 700 or k1 < 3000"
        contains "(((k1[#0] > 500) AND (k2[#1] < 700)) OR (k1[#0] < 3000))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where k1 > 500 or k2 < 700 and k1 < 3000"
        contains "((k1[#0] > 500) OR ((k2[#1] < 700) AND (k1[#0] < 3000)))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where not (k1 > 500 and k2 < 700) or k1 < 3000"
        contains "((k1[#0] < 3000) OR (k2[#1] >= 700))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where k1 > 500 and (k2 < 700 or k1 < 3000)"
        contains "PREDICATES: ((k1[#0] > 500) AND ((k2[#1] < 700) OR (k1[#0] < 3000)))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where not (k1 > 500 or k2 < 700) and k1 < 3000"
        contains "PREDICATES: ((k1[#0] <= 500) AND (k2[#1] >= 700))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where (k1 > 500 and not k2 < 700) or k1 < 3000"
        contains "PREDICATES: (((k1[#0] > 500) AND (k2[#1] >= 700)) OR (k1[#0] < 3000))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where (k1 > 500 and k2 < 700) and (k1 < 3000 or k2 > 400)"
        contains "PREDICATES: (((k1[#0] > 500) AND (k2[#1] < 700)) AND ((k1[#0] < 3000) OR (k2[#1] > 400)))"
    }

    explain {
        sql  "select * from test_compoundpredicate_explain where not (k1 > 500 or (k2 < 700 and k1 < 3000))"
        contains "PREDICATES: ((k1[#0] <= 500) AND ((k2[#1] >= 700) OR (k1[#0] >= 3000)))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where k1 > 500 or not (k2 < 700 and k1 < 3000)"
        contains "PREDICATES: ((k1[#0] > 500) OR (k2[#1] >= 700))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where k1 < 1000 and (k2 < 700 or k1 > 500) and not (k2 > 300)"
        contains "PREDICATES: (((k1[#0] < 1000) AND ((k2[#1] < 700) OR (k1[#0] > 500))) AND (k2[#1] <= 300))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where not ((k1 > 500 and k2 < 700) or k1 < 3000)"
        contains "PREDICATES: (((k1[#0] <= 500) OR (k2[#1] >= 700)) AND (k1[#0] >= 3000))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where k1 > 500 and not (k2 < 700 or k1 < 3000)"
        contains "PREDICATES: ((k1[#0] >= 3000) AND (k2[#1] >= 700))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where (k1 > 500 or k2 < 700) and (k1 < 3000 and k2 > 200)"
        contains "PREDICATES: ((((k1[#0] > 500) OR (k2[#1] < 700)) AND (k1[#0] < 3000)) AND (k2[#1] > 200))"
    }

    explain {
        sql "select * from test_compoundpredicate_explain where (k1 > 500 and k2 < 700) or not (k1 < 3000 and k2 > 200)"
        contains "PREDICATES: ((((k1[#0] > 500) AND (k2[#1] < 700)) OR (k1[#0] >= 3000)) OR (k2[#1] <= 200))"
    }

    sql "drop table if exists test_compoundpredicate_explain"
}