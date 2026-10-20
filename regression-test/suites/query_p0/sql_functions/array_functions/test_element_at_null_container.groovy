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

suite("test_element_at_null_container") {
    // A bare NULL container carries no array/map type. When constant folding is skipped the
    // expression reaches the BE as a null-literal argument and must still evaluate to NULL
    // instead of being rejected as an unsupported BOOL container.
    sql "set debug_skip_fold_constant = true"
    qt_null_container_int_index "select element_at(NULL, 1)"
    qt_null_container_string_key "select element_at(NULL, 'k')"
    qt_null_container_null_index "select element_at(NULL, NULL)"
    qt_null_container_from_rows "select element_at(NULL, number) from numbers('number' = '3') order by number"

    // The folded path must agree with the BE evaluation above.
    sql "set debug_skip_fold_constant = false"
    qt_null_container_folded "select element_at(NULL, 1)"
}
