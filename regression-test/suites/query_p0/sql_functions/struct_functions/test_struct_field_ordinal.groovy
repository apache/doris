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

suite("test_struct_field_ordinal") {
    // One field suffices. Each BIGINT has low 32 bits equal to 1.
    // Both LARGEINTs also have low 64 bits equal to 1. Each selector must fail in analysis.
    ["element_at(struct(7), 4294967297)", "struct(7)[-4294967295]",
     "struct_element(struct(7), 18446744073709551617)", "struct(7)[-18446744073709551615]"].each { expr ->
        test {
            sql "select ${expr}"
            exception "field index out of bound"
        }
    }
    // Legal ordinals exercise both names and the subscript syntax without any table data.
    order_qt_valid "select element_at(struct(7), 1), struct_element(struct(7), 1), struct(7)[1]"
}
