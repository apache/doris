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

#include "vec/functions/array/function_array_index.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void register_function_array_index(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayIndex<ArrayContainsAction>>();
    factory.register_function<FunctionArrayIndex<ArrayPositionAction>>();
    factory.register_function<FunctionArrayIndex<ArrayCountEqual>>();

    factory.register_alternative_function<FunctionArrayIndex<ArrayContainsAction, true>>();
    factory.register_alternative_function<FunctionArrayIndex<ArrayPositionAction, true>>();
    factory.register_alternative_function<FunctionArrayIndex<ArrayCountEqual, true>>();
}

} // namespace doris::vectorized
