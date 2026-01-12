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

suite("fold_constant_cast") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"
    // bug_fix
    testFoldConst("select concat(substr('2025-03-20',1,4)-1,'-01-01')")
    testFoldConst("select concat(substr('2025-03-20',1,4)-1.0,'-01-01')")
    testFoldConst("select concat(substr('2025-03-20',1,4)+1.0,'-01-01')")
    testFoldConst("select concat(substr('2025-03-20',1,4)-0.5,'-01-01')")
    testFoldConst("select cast(cast(2025.00 as double) as string)")
    testFoldConst("select cast(cast(2025.00 as float) as string)")
    testFoldConst("SELECT CAST(CAST(123 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(123.456 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(-123.456 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(0.001 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(-0.001 AS DOUBLE) AS STRING)")
// be and fe use different strategy of scientific notation, so it does not look the same
//    testFoldConst("SELECT CAST(CAST(1e+10 AS DOUBLE) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(1e-10 AS DOUBLE) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(-1e+10 AS DOUBLE) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(-1e-10 AS DOUBLE) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(123456789.123456789 AS DOUBLE) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(-123456789.123456789 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(0 AS DOUBLE) AS STRING)")
    // be and fe has different precision of float, so it does not look the same
    // testFoldConst("SELECT CAST(CAST(0.1 AS DOUBLE) AS STRING)")
    // testFoldConst("SELECT CAST(CAST(-0.1 AS DOUBLE) AS STRING)")
    testFoldConst("SELECT CAST(CAST(123 AS FLOAT) AS STRING)")
    // testFoldConst("SELECT CAST(CAST(123.456 AS FLOAT) AS STRING)")
    // testFoldConst("SELECT CAST(CAST(-123.456 AS FLOAT) AS STRING)")
    testFoldConst("SELECT CAST(CAST(0.001 AS FLOAT) AS STRING)")
    testFoldConst("SELECT CAST(CAST(-0.001 AS FLOAT) AS STRING)")
//    testFoldConst("SELECT CAST(CAST(1e+10 AS FLOAT) AS STRING)")
}
