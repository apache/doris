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

#include "exprs/table_function/table_function_factory.h"

#include "common/object_pool.h"
#include "exprs/table_function/explode_bitmap.h"
#include "exprs/table_function/explode_json_array.h"
#include "exprs/table_function/explode_split.h"
#include "exprs/table_function/table_function.h"
#include "vec/exprs/table_function/vexplode_bitmap.h"
#include "vec/exprs/table_function/vexplode_json_array.h"
#include "vec/exprs/table_function/vexplode_numbers.h"
#include "vec/exprs/table_function/vexplode_split.h"

namespace doris {

template <typename TableFunctionType>
struct TableFunctionCreator {
    TableFunction* operator()() { return new TableFunctionType(); }
};

template <>
struct TableFunctionCreator<ExplodeJsonArrayTableFunction> {
    ExplodeJsonArrayType type;
    TableFunction* operator()() const { return new ExplodeJsonArrayTableFunction(type); }
};

template <>
struct TableFunctionCreator<vectorized::VExplodeJsonArrayTableFunction> {
    ExplodeJsonArrayType type;
    TableFunction* operator()() const {
        return new vectorized::VExplodeJsonArrayTableFunction(type);
    }
};

inline auto ExplodeJsonArrayIntCreator =
        TableFunctionCreator<ExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::INT};
inline auto ExplodeJsonArrayDoubleCreator =
        TableFunctionCreator<ExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::DOUBLE};
inline auto ExplodeJsonArrayStringCreator =
        TableFunctionCreator<ExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::STRING};

inline auto VExplodeJsonArrayIntCreator =
        TableFunctionCreator<vectorized::VExplodeJsonArrayTableFunction> {
                ExplodeJsonArrayType::INT};
inline auto VExplodeJsonArrayDoubleCreator =
        TableFunctionCreator<vectorized::VExplodeJsonArrayTableFunction> {
                ExplodeJsonArrayType::DOUBLE};
inline auto VExplodeJsonArrayStringCreator =
        TableFunctionCreator<vectorized::VExplodeJsonArrayTableFunction> {
                ExplodeJsonArrayType::STRING};

//{fn_name,is_vectorized}->table_function_creator
const std::unordered_map<std::pair<std::string, bool>, std::function<TableFunction*()>>
        TableFunctionFactory::_function_map {
                {{"explode_split", false}, TableFunctionCreator<ExplodeSplitTableFunction> {}},
                {{"explode_bitmap", false}, TableFunctionCreator<ExplodeBitmapTableFunction> {}},
                {{"explode_json_array_int", false}, ExplodeJsonArrayIntCreator},
                {{"explode_json_array_double", false}, ExplodeJsonArrayDoubleCreator},
                {{"explode_json_array_string", false}, ExplodeJsonArrayStringCreator},
                {{"explode_split", true},
                 TableFunctionCreator<vectorized::VExplodeSplitTableFunction>()},
                {{"explode_numbers", true},
                 TableFunctionCreator<vectorized::VExplodeNumbersTableFunction>()},
                {{"explode_json_array_int", true}, VExplodeJsonArrayIntCreator},
                {{"explode_json_array_double", true}, VExplodeJsonArrayDoubleCreator},
                {{"explode_json_array_string", true}, VExplodeJsonArrayStringCreator},
                {{"explode_bitmap", true},
                 TableFunctionCreator<vectorized::VExplodeBitmapTableFunction>()}};

Status TableFunctionFactory::get_fn(const std::string& fn_name, bool is_vectorized,
                                    ObjectPool* pool, TableFunction** fn) {
    auto fn_iterator = _function_map.find({fn_name, is_vectorized});
    if (fn_iterator != _function_map.end()) {
        *fn = pool->add(fn_iterator->second());
        return Status::OK();
    }

    return Status::NotSupported(std::string(is_vectorized ? "vectorized " : "") +
                                "table function " + fn_name + " not support");
}

} // namespace doris
