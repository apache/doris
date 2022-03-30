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

#include "exprs/table_function/dummy_table_functions.h"

namespace doris {

void DummyTableFunctions::init() {}

StringVal DummyTableFunctions::explode_split(FunctionContext* context, const StringVal& str,
                                             const StringVal& sep) {
    return StringVal();
}

BigIntVal DummyTableFunctions::explode_bitmap(doris_udf::FunctionContext* context,
                                              const doris_udf::StringVal& bitmap) {
    return BigIntVal();
}

BigIntVal DummyTableFunctions::explode_json_array_int(doris_udf::FunctionContext* context,
                                                      const doris_udf::StringVal& str) {
    return BigIntVal();
}

DoubleVal DummyTableFunctions::explode_json_array_double(doris_udf::FunctionContext* context,
                                                         const doris_udf::StringVal& str) {
    return DoubleVal();
}

StringVal DummyTableFunctions::explode_json_array_string(doris_udf::FunctionContext* context,
                                                         const doris_udf::StringVal& str) {
    return StringVal();
}

IntVal DummyTableFunctions::explode_numbers(doris_udf::FunctionContext* context,
                                            const doris_udf::IntVal& str) {
    return IntVal();
}
} // namespace doris
