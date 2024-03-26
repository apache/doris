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

suite("test_function_regexp_passall") {
    sql """
        CREATE TABLE test_function_regexp_passall (
            id int,
            value_col string,
            pattern_col string
        ) 
        ENGINE=OLAP
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """INSERT INTO test_function_regexp_passall VALUES
            (0, 'prefix0_infix0_suffix0', 'prefix0'),
            (1, '%prefix1_infix1_suffix1', 'prefix1'),
            (2, 'prefix2_\$infix2\$suffix2', 'infix2'),
            (3, 'prefix3^infix3_suffix3', 'infix3'),
            (4, '\$\$\$prefix4_\$\$\$infix4%%%^^suffix4', 'suffix4'),
            (5, 'prefix5%%\$\$\$__infix5\$^^^%%\$\$suffix5', 'suffix5'),
            (6, 'prefix6__^^%%%infix6%^suffix6%', 'prefix6_^^%%%infix6%^suffix6%'),
            (7, '%%%^^^\$\$\$prefix7_infix7_suffix7%%%^^^\$\$\$', 'prefix7_infix7_suffix7'),
            (8, 'prefix8^^%%%infix8%%\$\$^^___suffix8', ''),
            (9, 'prefix9\$\$%%%^^infix9&&%%\$\$suffix9', NULL);
    """

    qt_sql "SELECT * FROM test_function_regexp_passall WHERE value_col REGEXP '.*' ORDER BY id;"

    qt_sql "SELECT * FROM test_function_regexp_passall WHERE value_col REGEXP '.' ORDER BY id;"

    qt_sql "SELECT * FROM test_function_regexp_passall WHERE value_col REGEXP '.*.*' ORDER BY id;"

    sql """DROP TABLE IF EXISTS test_function_regexp_passall FORCE; """
}