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

// Reading one Arrow Stream Load batch of JSON objects into a VARIANT column, per input
// shape:
//   JsonText             utf8, one JSON document per row, parsed on the BE.
//   BinaryStruct         struct<metadata: binary, value: binary>, the batch's shared metadata
//                        repeated in every row.
//   DictionaryNormalized the same struct with dictionary<int32, binary> metadata holding one entry,
//                        decoded to plain binary first, as the stream reader does for a type the
//                        SerDe does not read itself.
//   DictionaryDirect     that dictionary-encoded struct read without decoding.
// The argument is the number of distinct keys in the batch, which sets the shared metadata size:
// 128 keys give about 1.7 KB, a batch with few distinct keys, and 4096 keys about 64 KB, a batch
// whose rows draw on a wide key set. Every row holds 24 fields, about 1.2 KB of JSON text.

#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <benchmark/benchmark.h>
#include <cctz/time_zone.h>

#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "format/arrow/arrow_array_normalizer.h"

namespace doris {
namespace variant_arrow_input_benchmark {

constexpr int64_t ROWS = 4064;
constexpr int FIELDS_PER_ROW = 24;

struct Batch {
    std::shared_ptr<arrow::Array> json_text;
    std::shared_ptr<arrow::Array> binary_struct;
    std::shared_ptr<arrow::Array> dictionary_struct;
    int64_t json_bytes = 0;
};

std::string json_document(int64_t row, int64_t keys) {
    std::string json = "{";
    uint64_t state = static_cast<uint64_t>(row) * 0x9E3779B97F4A7C15ULL + 1;
    for (int field = 0; field < FIELDS_PER_ROW; ++field) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if (field != 0) {
            json += ",";
        }
        // Field f draws from keys congruent to f, so a row never repeats a key.
        json += "\"attribute_" +
                std::to_string(state % (keys / FIELDS_PER_ROW) * FIELDS_PER_ROW + field) + "\":";
        if (field % 3 == 0) {
            json += std::to_string(state % 1000000);
        } else {
            json += "\"value-" + std::to_string(state) + "-" + std::string(18, 'x') + "\"";
        }
    }
    return json + "}";
}

template <typename Builder>
std::shared_ptr<arrow::Array> finish(Builder& builder) {
    auto result = builder.Finish();
    if (!result.ok()) {
        throw std::runtime_error(result.status().ToString());
    }
    return result.ValueOrDie();
}

void check(const arrow::Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.ToString());
    }
}

const Batch& batch(int64_t keys) {
    static std::map<int64_t, Batch> batches;
    auto& result = batches[keys];
    if (result.json_text != nullptr) {
        return result;
    }
    arrow::StringBuilder text;
    JsonStringToVariantEncoder encoder;
    for (int64_t row = 0; row < ROWS; ++row) {
        const std::string json = json_document(row, keys);
        result.json_bytes += json.size();
        check(text.Append(json));
        encoder.add_json({json.data(), json.size()});
    }
    result.json_text = finish(text);

    auto encoded = ColumnVariantV2::create();
    encoded->insert_encoded_batch(encoder.finish_batch());
    arrow::BinaryBuilder metadata;
    arrow::BinaryBuilder values;
    arrow::Int32Builder indices;
    for (int64_t row = 0; row < ROWS; ++row) {
        const auto ref = encoded->get_value_ref(row);
        check(metadata.Append(ref.metadata.data, static_cast<int32_t>(ref.metadata.size)));
        check(values.Append(ref.value.data, static_cast<int32_t>(ref.value.size)));
        check(indices.Append(0));
    }
    auto value_array = finish(values);
    auto metadata_array = finish(metadata);
    const auto struct_of = [&](const std::shared_ptr<arrow::Array>& metadata_child) {
        return arrow::StructArray::Make({metadata_child, value_array},
                                        {arrow::field("metadata", metadata_child->type(), false),
                                         arrow::field("value", arrow::binary(), false)})
                .ValueOrDie();
    };
    result.binary_struct = struct_of(metadata_array);
    auto dictionary =
            arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::binary()),
                                               finish(indices), metadata_array->Slice(0, 1))
                    .ValueOrDie();
    result.dictionary_struct = struct_of(dictionary);
    return result;
}

void read(benchmark::State& state, const std::shared_ptr<arrow::Array>& input, bool normalize) {
    DataTypeVariantV2SerDe serde;
    for (auto _ : state) {
        std::shared_ptr<arrow::Array> array = input;
        if (normalize) {
            const Status status =
                    normalize_arrow_array(input, arrow::default_memory_pool(), &array);
            if (!status.ok()) {
                state.SkipWithError(status.to_string().c_str());
                return;
            }
        }
        auto column = ColumnVariantV2::create();
        const Status status =
                serde.read_column_from_arrow(*column, array.get(), 0, ROWS, cctz::utc_time_zone());
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
        benchmark::DoNotOptimize(column);
    }
    const auto& source = batch(state.range(0));
    state.SetItemsProcessed(state.iterations() * ROWS);
    state.counters["json_bytes_per_row"] = static_cast<double>(source.json_bytes) / ROWS;
    state.counters["metadata_bytes"] = static_cast<double>(
            static_cast<const arrow::BinaryArray&>(
                    *static_cast<const arrow::StructArray&>(*source.binary_struct).field(0))
                    .value_length(0));
}

void BM_VariantArrowInputJsonText(benchmark::State& state) {
    read(state, batch(state.range(0)).json_text, false);
}

void BM_VariantArrowInputBinaryStruct(benchmark::State& state) {
    read(state, batch(state.range(0)).binary_struct, false);
}

void BM_VariantArrowInputDictionaryNormalized(benchmark::State& state) {
    read(state, batch(state.range(0)).dictionary_struct, true);
}

void BM_VariantArrowInputDictionaryDirect(benchmark::State& state) {
    read(state, batch(state.range(0)).dictionary_struct, false);
}

} // namespace variant_arrow_input_benchmark

BENCHMARK(variant_arrow_input_benchmark::BM_VariantArrowInputJsonText)->Arg(128)->Arg(4096);
BENCHMARK(variant_arrow_input_benchmark::BM_VariantArrowInputBinaryStruct)->Arg(128)->Arg(4096);
BENCHMARK(variant_arrow_input_benchmark::BM_VariantArrowInputDictionaryNormalized)
        ->Arg(128)
        ->Arg(4096);
BENCHMARK(variant_arrow_input_benchmark::BM_VariantArrowInputDictionaryDirect)->Arg(128)->Arg(4096);

} // namespace doris
