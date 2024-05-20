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

#include <gtest/gtest.h>

#include <cassert>
#include <memory>

#include "function_test_util.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {
TEST(function_money_format_test, money_format_with_decimalV2) {
    // why not using
    std::multimap<std::string, std::string> input_dec_str_and_expected_str = {
            {std::string("123.12"), std::string("123.12")},
            {std::string("-123.12"), std::string("-123.12")},
            {std::string("-0.12434"), std::string("-0.12")},
            {std::string("-0.12534"), std::string("-0.13")},
            {std::string("-123456789.12434"), std::string("-123,456,789.12")},
            {std::string("-123456789.12534"), std::string("-123,456,789.13")},
            {std::string("0.999999999"), std::string("1.00")},
            {std::string("-0.999999999"), std::string("-1.00")},
            {std::string("999999999999999999.994999999"),
             std::string("999,999,999,999,999,999.99")},
            {std::string("-999999999999999999.994999999"),
             std::string("-999,999,999,999,999,999.99")},
            {std::string("-999999999999999999.995999999"),
             std::string("-1,000,000,000,000,000,000.00")}};

    auto money_format = FunctionMoneyFormat<MoneyFormatDecimalImpl>::create();
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    TypeDescriptor return_type = {PrimitiveType::TYPE_VARCHAR};
    TypeDescriptor arg_type = {PrimitiveType::TYPE_DECIMALV2};
    std::vector<TypeDescriptor> arg_types = {arg_type};

    auto context = FunctionContext::create_context(runtime_state.get(), return_type, arg_types);

    Block block;
    ColumnNumbers arguments = {0};
    size_t result_idx = 1;
    auto col_dec_v2 = ColumnDecimal<Decimal128V2>::create(0, 9);
    auto col_res_expected = ColumnString::create();
    for (const auto& input_and_expected : input_dec_str_and_expected_str) {
        DecimalV2Value dec_v2_value(input_and_expected.first);
        col_dec_v2->insert_value(Decimal128V2(dec_v2_value.value()));
        col_res_expected->insert_data(input_and_expected.second.c_str(),
                                      input_and_expected.second.size());
    }

    block.insert({std::move(col_dec_v2), std::make_shared<DataTypeDecimal<Decimal128V2>>(10, 1),
                  "col_dec_v2"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "col_res"});

    Status exec_status = money_format->execute_impl(context.get(), block, arguments, result_idx,
                                                    block.get_by_position(0).column->size());

    // Check result
    auto col_res = block.get_by_position(result_idx).column;
    for (size_t i = 0; i < col_res->size(); ++i) {
        auto res = col_res->get_data_at(i);
        auto res_expected = col_res_expected->get_data_at(i);
        EXPECT_EQ(res.debug_string(), res_expected.debug_string())
                << "res " << res.debug_string() << ' ' << "res_expected "
                << res_expected.debug_string();
    }
}

}; // namespace doris::vectorized