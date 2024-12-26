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
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
namespace doris::vectorized {
struct DictGetState {
    int64_t dictionary_id;
    int64_t version_id;
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
        return std::make_shared<DataTypeDecimal<Decimal128V3>>();
    }

    bool skip_return_type_check() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<DictGetState> state = std::make_shared<DictGetState>();
        context->set_function_state(scope, state);
        auto dict_fn = context->dict_function();
        if (!dict_fn.has_value()) {
            return Status::RuntimeError(
                    "function context for function '{}' must have dict_function;", get_name());
        }
        state->dictionary_id = dict_fn->dictionary_id;
        state->version_id = dict_fn->version_id;
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
        const DataTypePtr attribute_type = block.get_by_position(result).type;
        auto res = attribute_type->create_column();
        for (int i = 0; i < input_rows_count; i++) {
            res->insert_default();
        }

        LOG_WARNING("dict_get execute_impl")
                .tag("dictionary_id", dict_state->dictionary_id)
                .tag("version_id", dict_state->version_id)
                .tag("return type", attribute_type->get_name());

        block.replace_by_position(result, std::move(res));

        return Status::InternalError("just test {},{}", dict_state->dictionary_id,
                                     dict_state->version_id);
    }
};

void register_function_dict_get(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDictGet>();
}

} // namespace doris::vectorized