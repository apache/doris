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

#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/aggregate/aggregate_function_reader_first_last.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {
#include "common/compile_check_begin.h"

// only replace function in load/reader do different agg operation.
// because Doris can ensure that the data is globally ordered in reader, but cannot in load
// 1. reader, get the first value of input data.
// 2. load, get the last value of input data.
void register_aggregate_function_replace_reader_load(AggregateFunctionSimpleFactory& factory) {
    auto register_function = [&](const std::string& name, const std::string& suffix,
                                 const AggregateFunctionCreator& creator, bool nullable) {
        factory.register_function(name + suffix, creator, nullable);
    };

    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<true>, false);
    register_function("replace", AGG_READER_SUFFIX, create_aggregate_function_first<true>, true);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<false>, false);
    register_function("replace", AGG_LOAD_SUFFIX, create_aggregate_function_last<false>, true);

    register_function("replace_if_not_null", AGG_READER_SUFFIX,
                      create_aggregate_function_first_non_null_value<true>, false);
    register_function("replace_if_not_null", AGG_READER_SUFFIX,
                      create_aggregate_function_first_non_null_value<true>, true);
    register_function("replace_if_not_null", AGG_LOAD_SUFFIX,
                      create_aggregate_function_last_non_null_value<false>, false);
    register_function("replace_if_not_null", AGG_LOAD_SUFFIX,
                      create_aggregate_function_last_non_null_value<false>, true);
}

} // namespace doris
