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

#pragma once

#include "exprs/anyval_util.h"
#include "udf/udf.h"

namespace doris {

// Currently Doris does not support array types, so the definition of table function
// is still using the definition of the scalar function.
// The definition here is just to facilitate the query planning stage and the query execution preparation stage
// to make smooth use of the existing function framework
// But the execution logic of the table function is not here. So the function names here are prefixed with "dummy".
// TODO: refactor here after we support real array type.
class DummyTableFunctions {
public:
    static void init();

    static doris_udf::StringVal explode_split(doris_udf::FunctionContext* context,
                                              const doris_udf::StringVal& str,
                                              const doris_udf::StringVal& sep);
    static doris_udf::BigIntVal explode_bitmap(doris_udf::FunctionContext* context,
                                               const doris_udf::StringVal& bitmap);
    static doris_udf::BigIntVal explode_json_array_int(doris_udf::FunctionContext* context,
                                                       const doris_udf::StringVal& str);
    static doris_udf::DoubleVal explode_json_array_double(doris_udf::FunctionContext* context,
                                                          const doris_udf::StringVal& str);
    static doris_udf::StringVal explode_json_array_string(doris_udf::FunctionContext* context,
                                                          const doris_udf::StringVal& str);
    static doris_udf::IntVal explode_numbers(doris_udf::FunctionContext* context,
                                             const doris_udf::IntVal& value);
};
} // namespace doris
