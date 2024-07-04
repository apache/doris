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

#include <gen_cpp/Types_types.h>

#include <memory>
#include <string_view>
#include <utility>

#include "common/object_pool.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/exprs/table_function/udf_table_function.h"
#include "vec/exprs/table_function/vexplode.h"
#include "vec/exprs/table_function/vexplode_bitmap.h"
#include "vec/exprs/table_function/vexplode_json_array.h"
#include "vec/exprs/table_function/vexplode_json_object.h"
#include "vec/exprs/table_function/vexplode_map.h"
#include "vec/exprs/table_function/vexplode_numbers.h"
#include "vec/exprs/table_function/vexplode_split.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename TableFunctionType>
struct TableFunctionCreator {
    std::unique_ptr<TableFunction> operator()() { return TableFunctionType::create_unique(); }
};

template <typename DataImpl>
struct VExplodeJsonArrayCreator {
    std::unique_ptr<TableFunction> operator()() {
        return VExplodeJsonArrayTableFunction<DataImpl>::create_unique();
    }
};

const std::unordered_map<std::string, std::function<std::unique_ptr<TableFunction>()>>
        TableFunctionFactory::_function_map {
                {"explode_split", TableFunctionCreator<VExplodeSplitTableFunction>()},
                {"explode_numbers", TableFunctionCreator<VExplodeNumbersTableFunction>()},
                {"explode_json_array_int", VExplodeJsonArrayCreator<ParsedDataInt>()},
                {"explode_json_array_double", VExplodeJsonArrayCreator<ParsedDataDouble>()},
                {"explode_json_array_string", VExplodeJsonArrayCreator<ParsedDataString>()},
                {"explode_json_array_json", VExplodeJsonArrayCreator<ParsedDataJSON>()},
                {"explode_bitmap", TableFunctionCreator<VExplodeBitmapTableFunction>()},
                {"explode_map", TableFunctionCreator<VExplodeMapTableFunction> {}},
                {"explode_json_object", TableFunctionCreator<VExplodeJsonObjectTableFunction> {}},
                {"explode", TableFunctionCreator<VExplodeTableFunction> {}}};

Status TableFunctionFactory::get_fn(const TFunction& t_fn, ObjectPool* pool, TableFunction** fn) {
    bool is_outer = match_suffix(t_fn.name.function_name, COMBINATOR_SUFFIX_OUTER);
    if (t_fn.binary_type == TFunctionBinaryType::JAVA_UDF) {
        *fn = pool->add(UDFTableFunction::create_unique(t_fn).release());
        if (is_outer) {
            (*fn)->set_outer();
        }
        return Status::OK();
    } else {
        const std::string& fn_name_raw = t_fn.name.function_name;
        const std::string& fn_name_real =
                is_outer ? remove_suffix(fn_name_raw, COMBINATOR_SUFFIX_OUTER) : fn_name_raw;

        auto fn_iterator = _function_map.find(fn_name_real);
        if (fn_iterator != _function_map.end()) {
            *fn = pool->add(fn_iterator->second().release());
            if (is_outer) {
                (*fn)->set_outer();
            }

            return Status::OK();
        }
    }
    return Status::NotSupported("Table function {} is not support", t_fn.name.function_name);
}

} // namespace doris::vectorized
