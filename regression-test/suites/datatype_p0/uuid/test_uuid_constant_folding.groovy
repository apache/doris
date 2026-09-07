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


// Checklist: A03 A05 A06 C05 E02.
suite("test_uuid_constant_folding", "p0") {

    for (boolean skip : [false,true]) {
        sql "SET debug_skip_fold_constant = ${skip}"
        qt_boundaries """SELECT CAST('00112233445566778899AABBCCDDEEFF' AS UUID),
             CAST('7fffffff-ffff-ffff-ffff-ffffffffffff' AS UUID) < CAST('80000000000000000000000000000000' AS UUID),
             CAST('80000000000000000000000000000000' AS UUID) < CAST(REPEAT('f',32) AS UUID),
             CAST('00112233445566778899AABBCCDDEEFF' AS UUID) = CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID),
             CAST(NULL AS UUID), CAST('invalid' AS UUID),
             CAST('' AS UUID), CAST(' 00112233445566778899aabbccddeeff' AS UUID),
             CAST('00112233445566778899aabbccddeeff ' AS UUID)"""
    }
    sql "SET debug_skip_fold_constant = true"
    sql "SET enable_strict_cast = true"
    for (String value : ['', 'invalid', '00112233-44556677-8899-aabbccddeeff']) {
        test {
            sql "SELECT CAST('${value}' AS UUID)"
            exception "uuid"
        }
        qt_try_cast "SELECT TRY_CAST('${value}' AS UUID)"
    }

}
