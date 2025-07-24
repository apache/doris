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

#include <memory>

#include "common/logging.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/dictionary.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct DictGetState {
    std::shared_ptr<const IDictionary> dict;
    ///TODO:
    // 1. we do not need to check dict every time(shoud only check in open)
    // 2. for some dict, will init some struct each time, we should cache it
};

class FunctionDictGet : public IFunction {
public:
    static constexpr auto name = "dict_get";
    static FunctionPtr create() { return std::make_shared<FunctionDictGet>(); }
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 3; }

    DataTypes get_variadic_argument_types_impl() const override { return {}; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return std::make_shared<DataTypeDecimal128>();
    }

    bool skip_return_type_check() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<DictGetState> state = std::make_shared<DictGetState>();
        context->set_function_state(scope, state);
        DCHECK(context->get_num_args() == 3);
        auto dict_fn = context->dict_function();
        if (!dict_fn) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "not set dict_function");
        }
        auto dict = ExecEnv::GetInstance()->dict_factory()->get(dict_fn->dictionary_id,
                                                                dict_fn->version_id);
        if (!dict) {
            std::string dict_name =
                    context->get_constant_col(0)->column_ptr->get_data_at(0).to_string();
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "can not find dict name : {} , dict_id : {} , version_id : {}  ",
                                   dict_name, dict_fn->dictionary_id, dict_fn->version_id);
        }
        state->dict = dict;
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto* dict_state = reinterpret_cast<DictGetState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!dict_state) {
            return Status::RuntimeError("funciton context for function '{}' must have dict_state;",
                                        get_name());
        }

        const auto dict = dict_state->dict;

        const std::string attribute_name =
                block.get_by_position(arguments[1]).column->get_data_at(0).to_string();
        const DataTypePtr attribute_type = dict->get_attribute_type(attribute_name);

        const ColumnPtr key_column =
                block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        const DataTypePtr key_type = block.get_by_position(arguments[2]).type;

        // key_type is not nullable, but key_column may be nullable
        // wiil check key_column in dict::getColumn
        auto res = dict->get_column(attribute_name, attribute_type, key_column,
                                    remove_nullable(key_type));

        block.replace_by_position(result, std::move(res));

        return Status::OK();
    }
};

void register_function_dict_get(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDictGet>();
}

} // namespace doris::vectorized
