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

#include <limits>
#include <type_traits>

#include "common/consts.h"
#include "common/logging.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionMatchBase : public IFunction {
public:
    size_t get_number_of_arguments() const override { return 2; }

    String get_name() const override { return "match"; }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto match_query_str = block.get_by_position(arguments[1]).to_string(0);
        std::string column_name = block.get_by_position(arguments[0]).name;
        auto match_pred_column_name =
                BeConsts::BLOCK_TEMP_COLUMN_PREFIX + column_name + "_match_" + match_query_str;
        if (!block.has(match_pred_column_name)) {
            if (!config::enable_storage_vectorization) {
                return Status::Cancelled(
                        "please check whether turn on the configuration "
                        "'enable_storage_vectorization'");
            }
            LOG(WARNING) << "execute match query meet error, block no column: "
                         << match_pred_column_name;
            return Status::InternalError(
                    "match query meet error, no match predicate evaluate result column in block.");
        }
        auto match_pred_column =
                block.get_by_name(match_pred_column_name).column->convert_to_full_column_if_const();

        block.replace_by_position(result, std::move(match_pred_column));
        return Status::OK();
    }
};

class FunctionMatchAny : public FunctionMatchBase {
public:
    static constexpr auto name = "match_any";
    static FunctionPtr create() { return std::make_shared<FunctionMatchAny>(); }

    String get_name() const override { return name; }
};

class FunctionMatchAll : public FunctionMatchBase {
public:
    static constexpr auto name = "match_all";
    static FunctionPtr create() { return std::make_shared<FunctionMatchAll>(); }

    String get_name() const override { return name; }
};

class FunctionMatchPhrase : public FunctionMatchBase {
public:
    static constexpr auto name = "match_phrase";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

class FunctionMatchElementEQ : public FunctionMatchBase {
public:
    static constexpr auto name = "match_element_eq";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

class FunctionMatchElementLT : public FunctionMatchBase {
public:
    static constexpr auto name = "match_element_lt";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

class FunctionMatchElementGT : public FunctionMatchBase {
public:
    static constexpr auto name = "match_element_gt";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

class FunctionMatchElementLE : public FunctionMatchBase {
public:
    static constexpr auto name = "match_element_le";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

class FunctionMatchElementGE : public FunctionMatchBase {
public:
    static constexpr auto name = "match_element_ge";
    static FunctionPtr create() { return std::make_shared<FunctionMatchPhrase>(); }

    String get_name() const override { return name; }
};

void register_function_match(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMatchAny>();
    factory.register_function<FunctionMatchAll>();
    factory.register_function<FunctionMatchPhrase>();
    factory.register_function<FunctionMatchElementEQ>();
    factory.register_function<FunctionMatchElementLT>();
    factory.register_function<FunctionMatchElementGT>();
    factory.register_function<FunctionMatchElementLE>();
    factory.register_function<FunctionMatchElementGE>();
}

} // namespace doris::vectorized
