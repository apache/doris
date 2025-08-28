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

suite("test_struct_functions_by_literal") {
    // struct-nested
    qt_sql "select s from (select struct('a', 1, 'doris', 'aaaaa', 1.32) as s) t"

    // struct constructor
    qt_sql "select struct(1, 2, 3)"
    qt_sql "select struct(1, 1000, 10000000000)"
    qt_sql "select struct('a', 1, 'doris', 'aaaaa', 1.32)"
    qt_sql "select struct(1, 'a', null)"
    qt_sql "select struct(null, null, null)"

    qt_sql "select named_struct('f1', 1, 'f2', 2, 'f3', 3)"
    qt_sql "select named_struct('f1', 1, 'f2', 1000, 'f3', 10000000000)"
    qt_sql "select named_struct('f1', 1, 'f2', 'doris', 'f3', 1.32)"
    qt_sql "select named_struct('f1', null, 'f2', null, 'f3', null)"

    qt_sql "select struct_element(named_struct('f1', 1, 'f2', 2, 'f3', 3), 'f1')"
    qt_sql "select struct_element(named_struct('f1', 1, 'f2', 1000, 'f3', 10000000000), 3)"

    //The precision of the decimal type in the test select is inconsistent with the precision of the function named_struct containing the decimal type.
    qt_sql "select cast(123.321 as decimal(6,3)), named_struct('col', 1, 'col1', 12345.24)"
    qt_sql "select cast(123.321 as decimal(6,3)), named_struct('col', 1, 'col1', cast(12345.24 as decimal(7,2)))"
}
