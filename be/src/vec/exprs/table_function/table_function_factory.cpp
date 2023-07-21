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

#include "vec/exprs/table_function/table_function_factory.h"

#include <utility>

#include "common/object_pool.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/table_function/vexplode.h"
#include "vec/exprs/table_function/vexplode_bitmap.h"
#include "vec/exprs/table_function/vexplode_json_array.h"
#include "vec/exprs/table_function/vexplode_numbers.h"
#include "vec/exprs/table_function/vexplode_split.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename TableFunctionType>
struct TableFunctionCreator {
    std::unique_ptr<TableFunction> operator()() { return TableFunctionType::create_unique(); }
};

template <>
struct TableFunctionCreator<VExplodeJsonArrayTableFunction> {
    ExplodeJsonArrayType type;
    std::unique_ptr<TableFunction> operator()() const {
        return VExplodeJsonArrayTableFunction::create_unique(type);
    }
};

inline auto VExplodeJsonArrayIntCreator =
        TableFunctionCreator<VExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::INT};
inline auto VExplodeJsonArrayDoubleCreator =
        TableFunctionCreator<VExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::DOUBLE};
inline auto VExplodeJsonArrayStringCreator =
        TableFunctionCreator<VExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::STRING};
inline auto VExplodeJsonArrayJsonCreator =
        TableFunctionCreator<VExplodeJsonArrayTableFunction> {ExplodeJsonArrayType::JSON};

const std::unordered_map<std::string, std::function<std::unique_ptr<TableFunction>()>>
        TableFunctionFactory::_function_map {
                {"explode_split", TableFunctionCreator<VExplodeSplitTableFunction>()},
                {"explode_numbers", TableFunctionCreator<VExplodeNumbersTableFunction>()},
                {"explode_json_array_int", VExplodeJsonArrayIntCreator},
                {"explode_json_array_double", VExplodeJsonArrayDoubleCreator},
                {"explode_json_array_string", VExplodeJsonArrayStringCreator},
                {"explode_json_array_json", VExplodeJsonArrayJsonCreator},
                {"explode_bitmap", TableFunctionCreator<VExplodeBitmapTableFunction>()},
                {"explode", TableFunctionCreator<VExplodeTableFunction> {}}};

Status TableFunctionFactory::get_fn(const std::string& fn_name_raw, ObjectPool* pool,
                                    TableFunction** fn) {
    bool is_outer = match_suffix(fn_name_raw, COMBINATOR_SUFFIX_OUTER);
    std::string fn_name_real =
            is_outer ? remove_suffix(fn_name_raw, COMBINATOR_SUFFIX_OUTER) : fn_name_raw;

    auto fn_iterator = _function_map.find(fn_name_real);
    if (fn_iterator != _function_map.end()) {
        *fn = pool->add(fn_iterator->second().release());
        if (is_outer) {
            (*fn)->set_outer();
        }

        return Status::OK();
    }

    return Status::NotSupported("Table function {} is not support", fn_name_raw);
}

} // namespace doris::vectorized
