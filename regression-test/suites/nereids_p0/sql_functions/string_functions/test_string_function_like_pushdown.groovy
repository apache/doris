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

suite("test_string_function_like_pushdown", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    sql "set enable_function_pushdown = true;"
    sql "set batch_size = 4096;"

    def tbName = "test_string_function_like_pushdown"
    sql "DROP TABLE IF EXISTS ${tbName}"
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName} (
                id int,
                k varchar(32)
            )
            DISTRIBUTED BY HASH(k) BUCKETS 5 properties("replication_num" = "1");
        """
    sql """
        INSERT INTO ${tbName} VALUES 
            (0, ""),
            (0, " "),
            (1, "a"),
            (1, "b"),
            (2, "bb"),
            (2, "bab"),
            (3, "ba"),
            (4, "ab"),
            (5, "accb");
        """
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \" \" ORDER BY k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"%a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"a%\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"%a%\" ORDER BY k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"_a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"a_\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"_a_\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"a__b\" ORDER BY k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"%a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"a%\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"%a%\" ORDER BY k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"_a\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"a_\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"_a_\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"a__b\" ORDER BY k;"

    qt_sql "SELECT k FROM ${tbName} WHERE k LIKE \"%\" ORDER BY k;"
    qt_sql "SELECT k FROM ${tbName} WHERE k NOT LIKE \"%\" ORDER BY k;"

    qt_sql """
        select * from ${tbName} where k like "%" order by id, k;
    """

    qt_sql """
        select * from ${tbName} where k like "%" and id > 0 order by id, k;
    """

    qt_sql """
        select * from ${tbName} where k like "_" and id = 1 order by id, k;
    """

    qt_sql """
        select * from ${tbName} where k like "_b" and id < 5 order by id, k;
    """

    qt_sql """
        select * from ${tbName} where k like "_b" and id < 4 order by id, k;
    """

    qt_sql """
        select * from ${tbName} where k like "b%b" and id > 1 order by id, k;
    """

    sql "DROP TABLE ${tbName};"
}
