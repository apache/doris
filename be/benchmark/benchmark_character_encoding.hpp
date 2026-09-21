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

#pragma once

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "util/defer_op.h"

namespace doris {
namespace {

struct CharacterEncodingData {
    std::string input;
    std::string expected;
    std::string charset;
};

template <int Scenario>
CharacterEncodingData make_character_encoding_data(size_t length) {
    CharacterEncodingData data;
    auto& [input, expected, charset] = data;
    if constexpr (Scenario == 0) {
        charset = "UTF-16BE";
        input.assign(length, 'A');
        for (size_t i = 0; i < length; ++i) {
            expected.append("\0A", 2);
        }
    } else if constexpr (Scenario == 1) {
        charset = "ISO-8859-1";
        input.assign(length, '\xE9');
        for (size_t i = 0; i < length; ++i) {
            expected += "é";
        }
    } else if constexpr (Scenario == 2) {
        charset = "UTF-8";
        input.assign(length - 7, 'A');
        input += "中😀";
        expected = input;
    } else {
        charset = "UTF-16BE";
        for (size_t i = 0; i < length / 2; ++i) {
            input += "N-"; // The UTF-16BE byte pair for 中.
            expected += "中";
        }
    }
    return data;
}

// Benchmark actual block execution, including result allocation and converter setup, not just
// ICU calls. Inputs are materialized columns so constant folding cannot eliminate conversion.
// Args: input size, constant charset (0/1). Large rows use smaller blocks to bound working memory.
template <bool Encode, int Scenario>
void BM_character_encoding(benchmark::State& state) {
    const size_t length = state.range(0);
    const size_t rows = std::min<size_t>(4096, (1 << 20) / length);
    const auto [input, expected, charset] = make_character_encoding_data<Scenario>(length);
    DataTypePtr string_type = std::make_shared<DataTypeString>();
    DataTypePtr binary_type = std::make_shared<DataTypeVarbinary>();
    DataTypePtr input_type = Encode ? string_type : binary_type;
    DataTypePtr result_type = Encode ? binary_type : string_type;
    auto values = input_type->create_column();
    for (size_t i = 0; i < rows; ++i) {
        values->insert_data(input.data(), input.size());
    }
    auto charsets = ColumnString::create();
    const bool constant_charset = state.range(1) != 0;
    for (size_t i = 0; i < (constant_charset ? 1 : rows); ++i) {
        charsets->insert_data(charset.data(), charset.size());
    }
    ColumnPtr charset_column;
    if (constant_charset) {
        charset_column = ColumnConst::create(std::move(charsets), rows);
    } else {
        charset_column = std::move(charsets);
    }
    Block block {{std::move(values), input_type, "input"},
                 {std::move(charset_column), string_type, "charset"}};
    auto function = SimpleFunctionFactory::instance().get_function(
            Encode ? "encode" : "decode", block.get_columns_with_type_and_name(), result_type);
    if (function == nullptr) {
        state.SkipWithError("Character encoding function not registered");
        return;
    }
    auto context = FunctionContext::create_context(nullptr, result_type, {input_type, string_type});
    auto status = function->open(context.get(), FunctionContext::FRAGMENT_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string());
        return;
    }
    Defer close_fragment {[&] {
        auto close_status = function->close(context.get(), FunctionContext::FRAGMENT_LOCAL);
        if (!close_status.ok()) {
            state.SkipWithError(close_status.to_string());
        }
    }};
    status = function->open(context.get(), FunctionContext::THREAD_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string());
        return;
    }
    Defer close_thread {[&] {
        auto close_status = function->close(context.get(), FunctionContext::THREAD_LOCAL);
        if (!close_status.ok()) {
            state.SkipWithError(close_status.to_string());
        }
    }};
    block.insert({nullptr, result_type, "result"});

    // Verify every output outside the measured loop.
    status = function->execute(context.get(), block, {0, 1}, 2, rows);
    if (!status.ok()) {
        state.SkipWithError(status.to_string());
        return;
    }
    for (size_t i = 0; i < rows; ++i) {
        auto actual = block.get_by_position(2).column->get_data_at(i);
        if (std::string_view(actual.data, actual.size) != expected) {
            state.SkipWithError("Character encoding result mismatch");
            return;
        }
    }
    for (auto _ : state) {
        status = function->execute(context.get(), block, {0, 1}, 2, rows);
        if (!status.ok()) {
            state.SkipWithError(status.to_string());
            break;
        }
        benchmark::DoNotOptimize(block.get_by_position(2).column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows);
    state.SetBytesProcessed(state.iterations() * rows * input.size());
}

BENCHMARK_TEMPLATE(BM_character_encoding, true, 0)
        ->Name("encode_utf16be_ascii")
        ->ArgsProduct({{15, 63, 1023, 65535}, {0, 1}});
BENCHMARK_TEMPLATE(BM_character_encoding, false, 1)
        ->Name("decode_latin1_nonascii")
        ->ArgsProduct({{15, 63, 1023, 65535}, {0, 1}});
BENCHMARK_TEMPLATE(BM_character_encoding, true, 2)
        ->Name("encode_utf8_mixed")
        ->ArgsProduct({{15, 63, 1023, 65535}, {0, 1}});
BENCHMARK_TEMPLATE(BM_character_encoding, false, 2)
        ->Name("decode_utf8_mixed")
        ->ArgsProduct({{15, 63, 1023, 65535}, {0, 1}});
BENCHMARK_TEMPLATE(BM_character_encoding, false, 3)
        ->Name("decode_utf16be_cjk")
        ->ArgsProduct({{16, 64, 1024, 65536}, {0, 1}});

} // namespace
} // namespace doris
