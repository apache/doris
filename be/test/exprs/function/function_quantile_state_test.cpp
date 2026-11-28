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
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/types.h"
#include "core/value/quantile_state.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"
#include "util/defer_op.h"
#include "util/url_coding.h"

namespace doris {

static Status execute_to_quantile_state(float compression, std::vector<uint8_t>& bytes,
                                        bool constant_values = false,
                                        bool nullable_compression = false,
                                        bool null_compression = false) {
    constexpr size_t rows = 3;
    auto values = ColumnFloat64::create();
    if (constant_values) {
        values->insert_value(1.0);
    } else {
        for (double value : {1.0, 2.0, 3.0}) {
            values->insert_value(value);
        }
    }
    auto compression_column = ColumnFloat32::create();
    compression_column->insert_value(compression);
    auto value_type = std::make_shared<DataTypeFloat64>();
    DataTypePtr compression_type = std::make_shared<DataTypeFloat32>();
    auto result_type = std::make_shared<DataTypeQuantileState>();
    Block block;
    if (constant_values) {
        block.insert({ColumnConst::create(std::move(values), rows), value_type, "value"});
    } else {
        block.insert({std::move(values), value_type, "value"});
    }
    if (nullable_compression) {
        auto null_map = ColumnUInt8::create();
        null_map->insert_value(null_compression);
        auto nullable = ColumnNullable::create(std::move(compression_column), std::move(null_map));
        compression_type = std::make_shared<DataTypeNullable>(compression_type);
        block.insert(
                {ColumnConst::create(std::move(nullable), rows), compression_type, "compression"});
    } else {
        block.insert({ColumnConst::create(std::move(compression_column), rows), compression_type,
                      "compression"});
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            "to_quantile_state", block.get_columns_with_type_and_name(), result_type);
    CHECK(function != nullptr);
    FunctionUtils fn_utils(result_type, {value_type, compression_type}, false);
    auto* context = fn_utils.get_fn_ctx();
    RETURN_IF_ERROR(function->open(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    RETURN_IF_ERROR(function->open(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    block.insert({nullptr, result_type, "result"});
    auto status = function->execute(context, block, {0, 1}, 2, rows);
    static_cast<void>(function->close(context, FunctionContext::FunctionStateScope::THREAD_LOCAL));
    static_cast<void>(
            function->close(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    if (status.ok()) {
        const auto result_column =
                block.get_by_position(2).column->convert_to_full_column_if_const();
        const auto& result = assert_cast<const ColumnQuantileState&>(*result_column);
        EXPECT_EQ(rows, result.size());
        for (size_t i = 0; i < rows; ++i) {
            const auto& state = result.get_element(i);
            EXPECT_EQ(constant_values ? 1 : i + 1, state.get_value_by_percentile(0));
            if (i == 0) {
                bytes.resize(state.get_serialized_size());
                state.serialize(bytes.data());
            }
        }
    }
    return status;
}

TEST(function_quantile_state_test, low_compression_creation_gate) {
    const bool old_setting = config::enable_low_compression_quantile_state;
    Defer restore([&] { config::enable_low_compression_quantile_state = old_setting; });
    std::vector<uint8_t> bytes;
    for (float requested : {32.0F, 32.5F, 127.0F, 128.0F}) {
        for (bool constant_values : {false, true}) {
            config::enable_low_compression_quantile_state = false;
            auto disabled = execute_to_quantile_state(requested, bytes, constant_values);
            EXPECT_FALSE(disabled.ok());
            EXPECT_NE(std::string::npos,
                      disabled.to_string().find("enable_low_compression_quantile_state"));

            config::enable_low_compression_quantile_state = true;
            ASSERT_TRUE(execute_to_quantile_state(requested, bytes, constant_values).ok());
            float actual = 0;
            std::memcpy(&actual, bytes.data(), sizeof(actual));
            EXPECT_EQ(requested, actual);
        }
    }
    for (bool enabled : {false, true}) {
        config::enable_low_compression_quantile_state = enabled;
        for (float requested : {31.0F, 2048.0F, std::numeric_limits<float>::infinity(),
                                std::numeric_limits<float>::quiet_NaN()}) {
            ASSERT_TRUE(execute_to_quantile_state(requested, bytes).ok());
            float actual = 0;
            std::memcpy(&actual, bytes.data(), sizeof(actual));
            EXPECT_EQ(2048, actual);
        }
    }
}

TEST(function_quantile_state_test, nullable_compression_constant) {
    const bool old_setting = config::enable_low_compression_quantile_state;
    Defer restore([&] { config::enable_low_compression_quantile_state = old_setting; });
    std::vector<uint8_t> bytes;
    for (bool constant_values : {false, true}) {
        for (float requested : {32.0F, 127.0F, 128.0F}) {
            config::enable_low_compression_quantile_state = false;
            auto disabled = execute_to_quantile_state(requested, bytes, constant_values, true);
            EXPECT_FALSE(disabled.ok());
            EXPECT_NE(std::string::npos,
                      disabled.to_string().find("enable_low_compression_quantile_state"));

            config::enable_low_compression_quantile_state = true;
            ASSERT_TRUE(execute_to_quantile_state(requested, bytes, constant_values, true).ok());
            float actual = 0;
            std::memcpy(&actual, bytes.data(), sizeof(actual));
            EXPECT_EQ(requested, actual);
        }

        config::enable_low_compression_quantile_state = false;
        for (auto [requested, is_null] :
             {std::pair {32.0F, true}, std::pair {31.0F, false}, std::pair {2048.0F, false}}) {
            ASSERT_TRUE(execute_to_quantile_state(requested, bytes, constant_values, true, is_null)
                                .ok());
            float actual = 0;
            std::memcpy(&actual, bytes.data(), sizeof(actual));
            EXPECT_EQ(2048, actual);
        }
    }
}

TEST(function_quantile_state_test, function_quantile_state_to_base64) {
    std::string func_name = "quantile_state_to_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_QUANTILE_STATE};

    QuantileState empty_quantile_state;

    QuantileState single_quantile_state;
    single_quantile_state.add_value(1.0);

    QuantileState multi_quantile_state;
    multi_quantile_state.add_value(1.0);
    multi_quantile_state.add_value(2.0);
    multi_quantile_state.add_value(3.0);
    multi_quantile_state.add_value(4.0);
    multi_quantile_state.add_value(5.0);

    QuantileState explicit_quantile_state;
    for (int i = 0; i < 100; i++) {
        explicit_quantile_state.add_value(static_cast<double>(i));
    }

    QuantileState tdigest_quantile_state;
    for (int i = 0; i < 3000; i++) {
        tdigest_quantile_state.add_value(static_cast<double>(i));
    }

    uint8_t buf[65536];
    unsigned char encoded_buf[131072];

    std::string empty_base64;
    {
        size_t len = empty_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        empty_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    std::string single_base64;
    {
        size_t len = single_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        single_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    std::string multi_base64;
    {
        size_t len = multi_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        multi_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    std::string explicit_base64;
    {
        size_t len = explicit_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        explicit_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    std::string tdigest_base64;
    {
        size_t len = tdigest_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        tdigest_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    {
        DataSet data_set = {{{&empty_quantile_state}, empty_base64},
                            {{&single_quantile_state}, single_base64},
                            {{&multi_quantile_state}, multi_base64},
                            {{&explicit_quantile_state}, explicit_base64},
                            {{&tdigest_quantile_state}, tdigest_base64}};

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(function_quantile_state_test, function_quantile_state_from_base64) {
    std::string func_name = "quantile_state_from_base64";
    InputTypeSet input_types = {PrimitiveType::TYPE_STRING};

    // Create quantile states for comparison
    QuantileState empty_quantile_state;

    QuantileState single_quantile_state;
    single_quantile_state.add_value(1.0);

    QuantileState multi_quantile_state;
    multi_quantile_state.add_value(1.0);
    multi_quantile_state.add_value(2.0);
    multi_quantile_state.add_value(3.0);
    multi_quantile_state.add_value(4.0);
    multi_quantile_state.add_value(5.0);

    uint8_t buf[65536];
    unsigned char encoded_buf[131072];
    std::string empty_base64;
    std::string single_base64;
    std::string multi_base64;

    {
        size_t len = empty_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        empty_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    {
        size_t len = single_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        single_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    {
        size_t len = multi_quantile_state.serialize(buf);
        size_t encoded_len = base64_encode(buf, len, encoded_buf);
        multi_base64 = std::string(reinterpret_cast<char*>(encoded_buf), encoded_len);
    }

    {
        char decoded_buf[65536];
        int decoded_len = base64_decode(empty_base64.c_str(), empty_base64.length(), decoded_buf);
        EXPECT_GT(decoded_len, 0);

        QuantileState decoded_empty;
        doris::Slice slice(decoded_buf, decoded_len);
        EXPECT_TRUE(decoded_empty.deserialize(slice));

        EXPECT_TRUE(std::isnan(empty_quantile_state.get_value_by_percentile(0.5)));
        EXPECT_TRUE(std::isnan(decoded_empty.get_value_by_percentile(0.5)));
    }

    {
        char decoded_buf[65536];
        int decoded_len = base64_decode(single_base64.c_str(), single_base64.length(), decoded_buf);
        EXPECT_GT(decoded_len, 0);

        QuantileState decoded_single;
        doris::Slice slice(decoded_buf, decoded_len);
        EXPECT_TRUE(decoded_single.deserialize(slice));

        EXPECT_NEAR(single_quantile_state.get_value_by_percentile(0.5),
                    decoded_single.get_value_by_percentile(0.5), 0.01);
    }

    {
        char decoded_buf[65536];
        int decoded_len = base64_decode(multi_base64.c_str(), multi_base64.length(), decoded_buf);
        EXPECT_GT(decoded_len, 0);

        QuantileState decoded_multi;
        doris::Slice slice(decoded_buf, decoded_len);
        EXPECT_TRUE(decoded_multi.deserialize(slice));

        EXPECT_NEAR(multi_quantile_state.get_value_by_percentile(0.5),
                    decoded_multi.get_value_by_percentile(0.5), 0.01);
        EXPECT_NEAR(multi_quantile_state.get_value_by_percentile(0.9),
                    decoded_multi.get_value_by_percentile(0.9), 0.01);
    }
}

TEST(function_quantile_state_test, function_quantile_state_roundtrip) {
    QuantileState original;
    for (int i = 0; i < 50; i++) {
        original.add_value(static_cast<double>(i * 2));
    }

    uint8_t ser_buf[65536];
    size_t ser_len = original.serialize(ser_buf);

    unsigned char encoded_buf[131072];
    size_t encoded_len = base64_encode(ser_buf, ser_len, encoded_buf);
    std::string base64_str(reinterpret_cast<char*>(encoded_buf), encoded_len);

    char decoded_buf[65536];
    int decoded_len = base64_decode(base64_str.c_str(), base64_str.length(), decoded_buf);
    EXPECT_GT(decoded_len, 0);

    QuantileState recovered;
    doris::Slice slice(decoded_buf, decoded_len);
    EXPECT_TRUE(recovered.deserialize(slice));

    EXPECT_NEAR(original.get_value_by_percentile(0.5), recovered.get_value_by_percentile(0.5),
                0.01);
    EXPECT_NEAR(original.get_value_by_percentile(0.9), recovered.get_value_by_percentile(0.9),
                0.01);
}

TEST(function_quantile_state_test, function_quantile_percent_mixed_const_test) {
    std::string func_name = "quantile_percent";
    InputTypeSet input_types = {PrimitiveType::TYPE_QUANTILE_STATE,
                                ConstedNotnull {PrimitiveType::TYPE_FLOAT}};

    QuantileState quantile_state;
    quantile_state.add_value(1.0);
    quantile_state.add_value(2.0);
    quantile_state.add_value(3.0);
    quantile_state.add_value(4.0);
    quantile_state.add_value(5.0);

    DataSet data_set = {{{&quantile_state, 0.5F}, 3.0}};

    static_cast<void>(check_function<DataTypeFloat64, false>(func_name, input_types, data_set));
}

} // namespace doris
