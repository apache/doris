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
#include <parquet/encoding.h>
#include <parquet/schema.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#include "core/custom_allocator.h"
#include "format_v2/parquet/reader/native/decoder.h"
#include "parquet_benchmark_scenarios.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace doris::parquet_benchmark {
namespace detail {

constexpr size_t DECODER_ROWS = 1UL << 16;
constexpr size_t DICTIONARY_ENTRIES = 256;
constexpr size_t FIXED_BINARY_WIDTH = 16;

struct EncodedPage {
    std::vector<uint8_t> data;
    std::vector<uint8_t> dictionary;
    size_t dictionary_entries = 0;
    size_t value_width = 0;
    bool binary = false;
};

inline tparquet::Type::type physical_type(ValueType value_type) {
    switch (value_type) {
    case ValueType::INT32:
        return tparquet::Type::INT32;
    case ValueType::INT64:
        return tparquet::Type::INT64;
    case ValueType::FLOAT:
        return tparquet::Type::FLOAT;
    case ValueType::DOUBLE:
        return tparquet::Type::DOUBLE;
    case ValueType::BYTE_ARRAY:
        return tparquet::Type::BYTE_ARRAY;
    case ValueType::FIXED_LEN_BYTE_ARRAY:
        return tparquet::Type::FIXED_LEN_BYTE_ARRAY;
    }
    throw std::logic_error("unknown Parquet benchmark value type");
}

inline tparquet::Encoding::type parquet_encoding(Encoding encoding) {
    switch (encoding) {
    case Encoding::PLAIN:
        return tparquet::Encoding::PLAIN;
    case Encoding::DICTIONARY:
        return tparquet::Encoding::RLE_DICTIONARY;
    case Encoding::BYTE_STREAM_SPLIT:
        return tparquet::Encoding::BYTE_STREAM_SPLIT;
    case Encoding::DELTA_BINARY_PACKED:
        return tparquet::Encoding::DELTA_BINARY_PACKED;
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
        return tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    case Encoding::DELTA_BYTE_ARRAY:
        return tparquet::Encoding::DELTA_BYTE_ARRAY;
    }
    throw std::logic_error("unknown Parquet benchmark encoding");
}

inline std::shared_ptr<::parquet::ColumnDescriptor> descriptor(::parquet::Type::type type,
                                                               int type_length = -1) {
    auto node = type == ::parquet::Type::FIXED_LEN_BYTE_ARRAY
                        ? ::parquet::schema::PrimitiveNode::Make(
                                  "value", ::parquet::Repetition::REQUIRED, type,
                                  ::parquet::ConvertedType::NONE, type_length)
                        : ::parquet::schema::PrimitiveNode::Make(
                                  "value", ::parquet::Repetition::REQUIRED, type);
    return std::make_shared<::parquet::ColumnDescriptor>(node, 0, 0);
}

template <typename T>
std::vector<T> fixed_values(size_t rows) {
    std::vector<T> values(rows);
    for (size_t row = 0; row < rows; ++row) {
        if constexpr (std::is_floating_point_v<T>) {
            values[row] = static_cast<T>((row % 1009) * 0.25 - 100.0);
        } else {
            values[row] = static_cast<T>((row * 17) % 1000003);
        }
    }
    return values;
}

inline std::vector<std::string> binary_values(size_t rows, size_t width = FIXED_BINARY_WIDTH) {
    std::vector<std::string> values;
    values.reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        std::string value(width, 'a');
        const uint64_t id = row % 1009;
        memcpy(value.data(), &id, std::min(width, sizeof(id)));
        values.push_back(std::move(value));
    }
    return values;
}

inline std::vector<uint8_t> encode_plain_binary(const std::vector<std::string>& values) {
    size_t bytes = 0;
    for (const auto& value : values) {
        bytes += sizeof(uint32_t) + value.size();
    }
    std::vector<uint8_t> encoded;
    encoded.reserve(bytes);
    for (const auto& value : values) {
        const auto length = static_cast<uint32_t>(value.size());
        const auto* length_bytes = reinterpret_cast<const uint8_t*>(&length);
        encoded.insert(encoded.end(), length_bytes, length_bytes + sizeof(length));
        encoded.insert(encoded.end(), value.begin(), value.end());
    }
    return encoded;
}

template <typename T>
std::vector<uint8_t> encode_plain_fixed(const std::vector<T>& values) {
    std::vector<uint8_t> encoded(values.size() * sizeof(T));
    memcpy(encoded.data(), values.data(), encoded.size());
    return encoded;
}

inline std::vector<uint8_t> encode_fixed_binary(const std::vector<std::string>& values) {
    std::vector<uint8_t> encoded;
    encoded.reserve(values.size() * FIXED_BINARY_WIDTH);
    for (const auto& value : values) {
        encoded.insert(encoded.end(), value.begin(), value.end());
    }
    return encoded;
}

inline std::vector<uint8_t> encode_byte_stream_split(const std::vector<uint8_t>& plain,
                                                     size_t value_width) {
    const size_t rows = plain.size() / value_width;
    std::vector<uint8_t> encoded(plain.size());
    for (size_t row = 0; row < rows; ++row) {
        for (size_t byte = 0; byte < value_width; ++byte) {
            encoded[byte * rows + row] = plain[row * value_width + byte];
        }
    }
    return encoded;
}

template <typename ParquetType, typename T>
std::vector<uint8_t> encode_delta_fixed(const std::vector<T>& values) {
    auto desc = descriptor(ParquetType::type_num);
    auto encoder = ::parquet::MakeTypedEncoder<ParquetType>(
            ::parquet::Encoding::DELTA_BINARY_PACKED, false, desc.get());
    encoder->Put(values.data(), static_cast<int>(values.size()));
    const auto buffer = encoder->FlushValues();
    return {buffer->data(), buffer->data() + buffer->size()};
}

inline std::vector<uint8_t> encode_delta_binary(const std::vector<std::string>& values,
                                                ::parquet::Encoding::type encoding) {
    std::vector<::parquet::ByteArray> arrays;
    arrays.reserve(values.size());
    for (const auto& value : values) {
        arrays.emplace_back(static_cast<uint32_t>(value.size()),
                            reinterpret_cast<const uint8_t*>(value.data()));
    }
    auto desc = descriptor(::parquet::Type::BYTE_ARRAY);
    auto encoder =
            ::parquet::MakeTypedEncoder<::parquet::ByteArrayType>(encoding, false, desc.get());
    encoder->Put(arrays.data(), static_cast<int>(arrays.size()));
    const auto buffer = encoder->FlushValues();
    return {buffer->data(), buffer->data() + buffer->size()};
}

inline std::vector<uint8_t> encode_dictionary_indices(size_t rows) {
    faststring encoded_ids;
    RleEncoder<uint32_t> encoder(&encoded_ids, 8);
    for (size_t row = 0; row < rows; ++row) {
        encoder.Put(static_cast<uint32_t>(row % DICTIONARY_ENTRIES));
    }
    for (size_t padding = rows; padding % 8 != 0; ++padding) {
        encoder.Put(0);
    }
    encoder.Flush();
    std::vector<uint8_t> result(encoded_ids.size() + 1);
    result[0] = 8;
    memcpy(result.data() + 1, encoded_ids.data(), encoded_ids.size());
    return result;
}

inline EncodedPage dictionary_page(ValueType value_type) {
    EncodedPage page;
    page.data = encode_dictionary_indices(DECODER_ROWS);
    page.dictionary_entries = DICTIONARY_ENTRIES;
    page.binary = value_type == ValueType::BYTE_ARRAY;
    switch (value_type) {
    case ValueType::INT32:
        page.value_width = sizeof(int32_t);
        page.dictionary = encode_plain_fixed(fixed_values<int32_t>(DICTIONARY_ENTRIES));
        break;
    case ValueType::INT64:
        page.value_width = sizeof(int64_t);
        page.dictionary = encode_plain_fixed(fixed_values<int64_t>(DICTIONARY_ENTRIES));
        break;
    case ValueType::FLOAT:
        page.value_width = sizeof(float);
        page.dictionary = encode_plain_fixed(fixed_values<float>(DICTIONARY_ENTRIES));
        break;
    case ValueType::DOUBLE:
        page.value_width = sizeof(double);
        page.dictionary = encode_plain_fixed(fixed_values<double>(DICTIONARY_ENTRIES));
        break;
    case ValueType::BYTE_ARRAY:
        page.value_width = FIXED_BINARY_WIDTH;
        page.dictionary = encode_plain_binary(binary_values(DICTIONARY_ENTRIES));
        break;
    case ValueType::FIXED_LEN_BYTE_ARRAY:
        page.value_width = FIXED_BINARY_WIDTH;
        page.dictionary = encode_fixed_binary(binary_values(DICTIONARY_ENTRIES));
        break;
    }
    return page;
}

inline EncodedPage encoded_page(const DecoderScenario& scenario) {
    if (scenario.encoding == Encoding::DICTIONARY) {
        return dictionary_page(scenario.value_type);
    }

    EncodedPage page;
    page.binary = scenario.value_type == ValueType::BYTE_ARRAY;
    switch (scenario.value_type) {
    case ValueType::INT32: {
        auto values = fixed_values<int32_t>(DECODER_ROWS);
        page.value_width = sizeof(int32_t);
        page.data = scenario.encoding == Encoding::DELTA_BINARY_PACKED
                            ? encode_delta_fixed<::parquet::Int32Type>(values)
                            : encode_plain_fixed(values);
        break;
    }
    case ValueType::INT64: {
        auto values = fixed_values<int64_t>(DECODER_ROWS);
        page.value_width = sizeof(int64_t);
        page.data = scenario.encoding == Encoding::DELTA_BINARY_PACKED
                            ? encode_delta_fixed<::parquet::Int64Type>(values)
                            : encode_plain_fixed(values);
        break;
    }
    case ValueType::FLOAT: {
        auto plain = encode_plain_fixed(fixed_values<float>(DECODER_ROWS));
        page.value_width = sizeof(float);
        page.data = scenario.encoding == Encoding::BYTE_STREAM_SPLIT
                            ? encode_byte_stream_split(plain, page.value_width)
                            : std::move(plain);
        break;
    }
    case ValueType::DOUBLE: {
        auto plain = encode_plain_fixed(fixed_values<double>(DECODER_ROWS));
        page.value_width = sizeof(double);
        page.data = scenario.encoding == Encoding::BYTE_STREAM_SPLIT
                            ? encode_byte_stream_split(plain, page.value_width)
                            : std::move(plain);
        break;
    }
    case ValueType::BYTE_ARRAY: {
        auto values = binary_values(DECODER_ROWS);
        page.value_width = FIXED_BINARY_WIDTH;
        if (scenario.encoding == Encoding::DELTA_LENGTH_BYTE_ARRAY) {
            page.data = encode_delta_binary(values, ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY);
        } else if (scenario.encoding == Encoding::DELTA_BYTE_ARRAY) {
            page.data = encode_delta_binary(values, ::parquet::Encoding::DELTA_BYTE_ARRAY);
        } else {
            page.data = encode_plain_binary(values);
        }
        break;
    }
    case ValueType::FIXED_LEN_BYTE_ARRAY: {
        auto plain = encode_fixed_binary(binary_values(DECODER_ROWS));
        page.value_width = FIXED_BINARY_WIDTH;
        page.data = scenario.encoding == Encoding::BYTE_STREAM_SPLIT
                            ? encode_byte_stream_split(plain, page.value_width)
                            : std::move(plain);
        break;
    }
    }
    return page;
}

class FixedSink final : public ParquetFixedValueConsumer {
public:
    Status consume(const uint8_t* values, size_t num_values, size_t value_width) override {
        benchmark::DoNotOptimize(values);
        benchmark::DoNotOptimize(num_values);
        benchmark::DoNotOptimize(value_width);
        consumed += num_values;
        return Status::OK();
    }

    size_t consumed = 0;
};

class BinarySink final : public ParquetBinaryValueConsumer {
public:
    Status consume(const StringRef* values, size_t num_values) override {
        benchmark::DoNotOptimize(values);
        benchmark::DoNotOptimize(num_values);
        consumed += num_values;
        return Status::OK();
    }

    Status consume_plain_byte_array(
            const char* encoded_data, const uint32_t* payload_offsets,
            const uint32_t* value_offsets, size_t num_values,
            const std::vector<ParquetSelectionRange>& value_spans) override {
        benchmark::DoNotOptimize(encoded_data);
        benchmark::DoNotOptimize(payload_offsets);
        benchmark::DoNotOptimize(value_offsets);
        auto value_spans_data = value_spans.data();
        benchmark::DoNotOptimize(value_spans_data);
        consumed += num_values;
        return Status::OK();
    }

    size_t consumed = 0;
};

class DictionarySink final : public ParquetDictionaryValueConsumer {
public:
    DictionarySink(const uint8_t* dictionary, size_t value_width)
            : _dictionary(dictionary), _value_width(value_width) {}

    Status consume_indices(const uint32_t* indices, size_t num_values) override {
        for (size_t row = 0; row < num_values; ++row) {
            auto* value = _dictionary + indices[row] * _value_width;
            benchmark::DoNotOptimize(value);
        }
        consumed += num_values;
        return Status::OK();
    }

    Status consume_repeated(uint32_t index, size_t num_values) override {
        auto* value = _dictionary + index * _value_width;
        benchmark::DoNotOptimize(value);
        consumed += num_values;
        return Status::OK();
    }

    size_t consumed = 0;

private:
    const uint8_t* _dictionary;
    const size_t _value_width;
};

inline ParquetSelection native_selection(const SelectionPlan& plan) {
    ParquetSelection selection {
            .total_values = plan.total_rows, .selected_values = plan.selected_rows, .ranges = {}};
    selection.ranges.reserve(plan.ranges.size());
    for (const auto& range : plan.ranges) {
        selection.ranges.push_back({.first = range.first, .count = range.count});
    }
    return selection;
}

inline void run_decoder(benchmark::State& state, DecoderScenario scenario, int selectivity,
                        Pattern pattern) {
    auto page = encoded_page(scenario);
    const auto plan = make_selection_plan(DECODER_ROWS, selectivity, pattern);
    const auto selection = native_selection(plan);
    std::unique_ptr<format::parquet::native::Decoder> decoder;
    auto status = format::parquet::native::Decoder::get_decoder(
            physical_type(scenario.value_type), parquet_encoding(scenario.encoding), decoder);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    decoder->set_type_length(static_cast<int32_t>(page.value_width));
    decoder->set_expected_values(DECODER_ROWS);

    std::vector<uint8_t> dictionary_for_sink;
    if (scenario.encoding == Encoding::DICTIONARY) {
        dictionary_for_sink = page.dictionary;
        auto dictionary = make_unique_buffer<uint8_t>(page.dictionary.size());
        memcpy(dictionary.get(), page.dictionary.data(), page.dictionary.size());
        status = decoder->set_dict(dictionary, static_cast<int32_t>(page.dictionary.size()),
                                   page.dictionary_entries);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
    }

    Slice encoded(page.data.data(), page.data.size());
    FixedSink fixed_sink;
    BinarySink binary_sink;
    DictionarySink dictionary_sink(dictionary_for_sink.data(), page.value_width);
    for (auto _ : state) {
        state.PauseTiming();
        status = decoder->set_data(&encoded);
        fixed_sink.consumed = 0;
        binary_sink.consumed = 0;
        dictionary_sink.consumed = 0;
        state.ResumeTiming();
        if (scenario.encoding == Encoding::DICTIONARY) {
            status = decoder->decode_selected_dictionary_values(selection, dictionary_sink);
        } else if (page.binary) {
            status = decoder->decode_selected_binary_values(selection, binary_sink);
        } else {
            status = decoder->decode_selected_fixed_values(selection, fixed_sink);
        }
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            break;
        }
        benchmark::ClobberMemory();
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(plan.selected_rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(page.data.size()));
    state.counters["raw_rows"] = static_cast<double>(DECODER_ROWS);
    state.counters["selected_rows"] = static_cast<double>(plan.selected_rows);
    state.counters["selection_ranges"] = static_cast<double>(plan.ranges.size());
    state.counters["encoded_bytes"] = static_cast<double>(page.data.size());
    state.counters["ns/raw_row"] = benchmark::Counter(
            static_cast<double>(DECODER_ROWS),
            benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
    if (plan.selected_rows > 0) {
        state.counters["ns/selected_row"] = benchmark::Counter(
                static_cast<double>(plan.selected_rows),
                benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
    }
}

inline bool register_decoder_benchmarks() {
    for (const auto& scenario : decoder_scenarios()) {
        for (const int selectivity : {0, 1, 10, 50, 90, 100}) {
            for (const auto pattern : {Pattern::CLUSTERED, Pattern::ALTERNATING}) {
                const std::string name = "ParquetDecoder/" + to_string(scenario.encoding) + "/" +
                                         to_string(scenario.value_type) + "/sel_" +
                                         std::to_string(selectivity) + "/" + to_string(pattern);
                benchmark::RegisterBenchmark(name.c_str(), [=](benchmark::State& state) {
                    run_decoder(state, scenario, selectivity, pattern);
                })->Unit(benchmark::kNanosecond);
            }
        }
    }
    return true;
}

inline const bool DECODER_BENCHMARKS_REGISTERED = register_decoder_benchmarks();

} // namespace detail
} // namespace doris::parquet_benchmark
