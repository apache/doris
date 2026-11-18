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

suite("test_negative_nan_literal") {
    // Use one generated row and one element per array. The array index uses the row value.
    // Thus BE must read the NaN literal before it can evaluate SIGNBIT.
    // With folding enabled, FE sends each array as a literal. Its FLOAT element also passes
    // through the Nereids-to-legacy conversion before serialization.
    // Do not use IF here. The optimizer can push SIGNBIT into its branches and fold it on FE.
    // The two columns cover negative DOUBLE and FLOAT NaNs. Both results must be true.
    def query = """
        select signbit(element_at(array(cast('-nan' as double)), number + 1)),
               signbit(cast(element_at(array(cast('-nan' as float)), number + 1) as double))
        from numbers("number" = "1")
    """

    sql "set debug_skip_fold_constant = false"
    order_qt_folded query

    // Without FE folding, BE parses the strings itself. The result must be the same.
    sql "set debug_skip_fold_constant = true"
    try {
        order_qt_unfolded query
    } finally {
        sql "set debug_skip_fold_constant = false"
    }
}
