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

// A cast of a VARBINARY value to a string type keeps its bytes, so hex(), to_base64() and length() of a
// VARBINARY constant, which take the value through that cast, must give the same results whether FE folds
// the constant or BE evaluates it. The *_fold and *_no_fold results below must match. X'FF' is not valid
// UTF-8, so FE leaves its cast to BE. The two blocks use different aliases so that they are not served from
// a cached result of the same statement text.

suite("test_varbinary_cast_fold") {
    sql "set debug_skip_fold_constant = false"
    order_qt_hex_fold """
        SELECT hex(cast('abc' AS varbinary(16))), hex(X'E4B8AD'), hex(X'FF'), hex(X'')
    """
    order_qt_base64_fold """
        SELECT to_base64(cast('abc' AS varbinary(16))), to_base64(X'FF')
    """
    order_qt_cast_fold """
        SELECT cast(cast('abc' AS varbinary(16)) AS string),
               length(cast(cast('abc' AS varbinary(16)) AS varchar(16))),
               cast(X'E4B8AD' AS string),
               length(cast(X'FF' AS string))
    """

    sql "set debug_skip_fold_constant = true"
    order_qt_hex_no_fold """
        SELECT hex(cast('abc' AS varbinary(16))) a, hex(X'E4B8AD') b, hex(X'FF') c, hex(X'') d
    """
    order_qt_base64_no_fold """
        SELECT to_base64(cast('abc' AS varbinary(16))) a, to_base64(X'FF') b
    """
    order_qt_cast_no_fold """
        SELECT cast(cast('abc' AS varbinary(16)) AS string) a,
               length(cast(cast('abc' AS varbinary(16)) AS varchar(16))) b,
               cast(X'E4B8AD' AS string) c,
               length(cast(X'FF' AS string)) d
    """
}
