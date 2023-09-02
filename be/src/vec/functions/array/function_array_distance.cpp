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

#include "vec/functions/array/function_array_distance.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

void register_function_array_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayDistance<L1Distance> >();
    factory.register_function<FunctionArrayDistance<L2Distance> >();
    factory.register_function<FunctionArrayDistance<CosineDistance> >();
    factory.register_function<FunctionArrayDistance<InnerProduct> >();
}

} // namespace doris::vectorized
