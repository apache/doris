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

#include <string>

#include "function_test_util.h"
#include "runtime/define_primitive_type.h"
#include "util/quantile_state.h"
#include "util/url_coding.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

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

} // namespace doris::vectorized
