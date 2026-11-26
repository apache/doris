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

suite("test_uuid_be_fold_skip", "p0") {
    sql "SET enable_fold_constant_by_be=true"
    String uuidArray = "array(CAST('00112233-4455-6677-8899-aabbccddeeff' AS UUID), CAST(NULL AS UUID))"

    // FE cannot evaluate element_at; UUID results must remain for runtime evaluation.
    explain {
        sql "SELECT element_at(${uuidArray},1) AS uuid_result"
        contains "element_at"
    }
    qt_uuid_results """SELECT element_at(${uuidArray},1), element_at(${uuidArray},2),
        element_at(${uuidArray},3), element_at(array(7),1)"""
    qt_nested "SELECT reverse(${uuidArray})"
    explain {
        sql "SELECT element_at(array(7),1) AS int_result"
        notContains "element_at"
    }
}
